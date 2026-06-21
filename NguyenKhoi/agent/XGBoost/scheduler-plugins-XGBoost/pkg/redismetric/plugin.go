package redismetric

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

const Name = "RedisMetricPlugin"

// CacheItem là một mục trong bộ nhớ đệm cục bộ
type CacheItem struct {
	TopNodes  []string
	ExpiresAt time.Time
}

// RedisMetricPlugin là struct chính của plugin
type RedisMetricPlugin struct {
	handle   framework.Handle
	dbClient *sql.DB
	cache    sync.Map // Bộ nhớ đệm an toàn cho goroutine
	cacheTTL time.Duration
}

var _ framework.ScorePlugin = &RedisMetricPlugin{}

// New tạo một instance mới của RedisMetricPlugin
func New(_ context.Context, _ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &RedisMetricPlugin{
		handle:   h,
		cacheTTL: 30 * time.Second,
	}, nil
}

// Name trả về tên của plugin, dùng để đăng ký
func (rmp *RedisMetricPlugin) Name() string {
	return Name
}

// ScoreExtensions trả về nil vì plugin này không cần normalize scores
func (rmp *RedisMetricPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// BUG: Global variables rdb and db are declared but never used, and can cause confusion
var rdb *redis.Client
var db *sql.DB

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// BUG: godotenv.Load() is called on every function invocation, which is inefficient and can cause performance issues
// BUG: Context shadowing - the input ctx parameter is overwritten with a new context, losing any parent context values or cancellation signals
// BUG: Redis client is created and closed on every call, causing connection overhead and potential resource exhaustion
func CalculateTopNodeFromDB(ctx context.Context) (string, error) {

	godotenv.Load()

	redisAddr := getEnv("REDIS_ADDR", "redis.default.svc.cluster.local:6379")
	str := getEnv("str", "multi-node-demo-worker")
	// Create a context with timeout to avoid hanging connections
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Configure Redis client
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr, // Redis server address
		Password: "",        // No password set
		DB:       0,         // Default DB
	})
	defer rdb.Close()

	// Test connection with PING
	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		return "", fmt.Errorf("could not connect to Redis: %w", err)
	}
	// BUG: fmt.Println should not be used in production code, use proper logging instead
	fmt.Println("Connected to Redis:", pong)
	// find the best index and format node name
	// bestIndex := Score(decision_matrix)
	index, err := rdb.Get(ctx, "XGBoost_TOP").Int()
	if err != nil {
		if err == redis.Nil {
			index = 0
		} else {
			return "", err
		}
	}
	// Fix: Node names are multi-node-demo-worker, multi-node-demo-worker2, multi-node-demo-worker3, etc.
	// For index 0: no node (return empty)
	// For index 1: multi-node-demo-worker
	// For index 2: multi-node-demo-worker2
	// For index 3: multi-node-demo-worker3
	// For index 4: multi-node-demo-worker4
	// For index 5: multi-node-demo-worker5
	var bestNodeName string
	if index == 0 {
		bestNodeName = ""
	} else if index == 1 {
		bestNodeName = str
	} else {
		bestNodeName = fmt.Sprintf("%s%d", str, index)
	}

	return bestNodeName, nil
}

func calculateScore(nodeName string, topNodes string) int64 {
	if nodeName == topNodes {
		return 100
	}
	return 0
}

// Score là hàm quan trọng nhất, được gọi để chấm điểm cho từng node

// BUG: CalculateTopNodeFromDB is called on every Score invocation without caching, despite the RedisMetricPlugin having a cache field
// BUG: The cache and cacheTTL fields in RedisMetricPlugin struct are never used
func (rmp *RedisMetricPlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := ""
	if n := nodeInfo.Node(); n != nil {
		nodeName = n.Name
	}

	// Lấy danh sách top nodes từ DB thay vì Redis
	topNodes, err := CalculateTopNodeFromDB(ctx)
	if err != nil {
		klog.ErrorS(err, "Failed to get top nodes from DB, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Nếu không có top nodes nào, trả về 0
	// BUG: len(topNodes) check is incorrect - topNodes is a string, not a slice. An empty string "" has length 0, but this check may not be the intended logic
	if len(topNodes) == 0 {
		klog.V(4).InfoS("No top nodes found in DB, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Tính điểm
	score := calculateScore(nodeName, topNodes)
	klog.V(4).InfoS("Calculated score", "node", nodeName, "score", score)
	return score, nil
}
