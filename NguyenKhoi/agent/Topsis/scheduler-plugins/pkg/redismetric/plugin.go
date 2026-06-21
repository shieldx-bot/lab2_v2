package redismetric

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

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

// Name trả về tên của plugin, dùng để đăng ký
func (rmp *RedisMetricPlugin) Name() string {
	return Name
}

// Lấy tên node tốt nhất bằng cách tính trực tiếp từ DB
func (rmp *RedisMetricPlugin) getTopNodesFromDB(ctx context.Context) ([]string, error) {
	// 1. Kiểm tra cache trước
	if cached, ok := rmp.cache.Load("TOP_NODE"); ok {
		item := cached.(CacheItem)
		if time.Now().Before(item.ExpiresAt) {
			klog.V(5).InfoS("Using cached top node", "count", len(item.TopNodes))
			return item.TopNodes, nil
		}
	}

	// 2. Tính toán node tốt nhất
	nodeName, err := CalculateTopNodeFromDB(ctx, rmp.dbClient)
	if err != nil {
		klog.ErrorS(err, "Failed to calculate top node from DB")
		return nil, fmt.Errorf("failed to calculate top node: %w", err)
	}

	// 3. Nếu giá trị rỗng
	if nodeName == "" {
		klog.V(4).InfoS("Calculated top node is empty")
		return []string{}, nil
	}

	// 4. Tạo mảng chứa 1 node top
	topNodes := []string{nodeName}

	// 5. Lưu vào cache
	rmp.cache.Store("TOP_NODE", CacheItem{
		TopNodes:  topNodes,
		ExpiresAt: time.Now().Add(rmp.cacheTTL),
	})

	klog.V(5).InfoS("Loaded top node from DB Calculation", "node", nodeName)

	return topNodes, nil
}

// isNodeInTopNodes kiểm tra node có nằm trong danh sách top nodes không
func (rmp *RedisMetricPlugin) isNodeInTopNodes(nodeName string, topNodes []string) bool {
	for _, topNode := range topNodes {
		if topNode == nodeName {
			return true
		}
	}
	return false
}

// calculateScore tính điểm dựa trên việc node có trong top list hay không
func (rmp *RedisMetricPlugin) calculateScore(nodeName string, topNodes []string) int64 {
	if rmp.isNodeInTopNodes(nodeName, topNodes) {
		klog.V(5).InfoS("Node is in top list, giving full score", "node", nodeName)
		return 100 // Full điểm nếu node trong top list
	}
	klog.V(5).InfoS("Node is not in top list, giving zero score", "node", nodeName)
	return 0 // 0 điểm nếu không trong top list
}

// Score là hàm quan trọng nhất, được gọi để chấm điểm cho từng node

func (rmp *RedisMetricPlugin) Score(ctx context.Context, state fwk.CycleState, p *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	nodeName := ""
	if n := nodeInfo.Node(); n != nil {
		nodeName = n.Name
	}

	// Lấy danh sách top nodes từ DB thay vì Redis
	topNodes, err := rmp.getTopNodesFromDB(ctx)
	if err != nil {
		klog.ErrorS(err, "Failed to get top nodes from DB, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Nếu không có top nodes nào, trả về 0
	if len(topNodes) == 0 {
		klog.V(4).InfoS("No top nodes found in DB, returning score 0", "node", nodeName)
		return 0, nil
	}

	// Tính điểm
	score := rmp.calculateScore(nodeName, topNodes)

	return score, nil
}

func (rmp *RedisMetricPlugin) ScoreExtensions() framework.ScoreExtensions {
	return nil
}

// New là hàm khởi tạo plugin
func New(ctx context.Context, obj runtime.Object, h framework.Handle) (framework.Plugin, error) {
	// Khởi tạo kết nối DB
	dbConn, err := sql.Open("pgx", "postgres://postgres:Vananh12345@54.255.223.141:5432/postgres")
	if err != nil {
		return nil, fmt.Errorf("không thể khởi tạo kết nối database: %w", err)
	}

	// Kéo dài timeout cho kết nối ban đầu
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := dbConn.PingContext(pingCtx); err != nil {
		return nil, fmt.Errorf("không thể kết nối đến PostgreSQL: %w", err)
	}
	klog.InfoS("RedisMetricPlugin đã kết nối thành công đến PostgreSQL")

	return &RedisMetricPlugin{
		handle:   h,
		dbClient: dbConn,
		cacheTTL: 15 * time.Second,
	}, nil
}
