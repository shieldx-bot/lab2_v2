package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/nats-io/nats.go"
)

// ============================================
// CONFIGURATION
// ============================================
type Config struct {
	NATSServers []string
	NATSTimeout time.Duration
	ServerPort  string
}

var config = Config{
	NATSServers: []string{
		"nats://192.168.24.6:6222",
		"nats://192.168.24.2:4222",
		"nats://192.168.24.3:5222"},
	NATSTimeout: 8 * time.Second,
	ServerPort:  ":4000",
}

// ============================================
// CLIENTS
// ============================================
var natsConn *nats.Conn
var ncMu sync.RWMutex

// ============================================
// REQUEST/RESPONSE TYPES
// ============================================
type DBRequest struct {
	QueryType string                 `json:"queryType"`
	Params    map[string]interface{} `json:"params"`
}

type DBResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
	Message string      `json:"message,omitempty"`
}

// ============================================
// INIT
// ============================================
func initNATS() error {
	var err error
	natsConn, err = nats.Connect(
		strings.Join(config.NATSServers, ","),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(1*time.Second),
		nats.Timeout(5*time.Second),
		nats.PingInterval(10*time.Second),
	)
	if err != nil {
		return err
	}
	log.Printf("✅ NATS connected")
	return nil
}

// ============================================
// NATS REQUESTS
// ============================================
func sendDBRequest(ctx context.Context, queryType string, params map[string]interface{}) (*DBResponse, error) {
	req := DBRequest{QueryType: queryType, Params: params}
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ncMu.RLock()
	nc := natsConn
	ncMu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}

	msg, err := nc.RequestWithContext(ctx, "db.query", reqData)
	if err != nil {
		return nil, err
	}

	var resp DBResponse
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func sendBatchDBRequest(ctx context.Context, queries []map[string]interface{}) ([]DBResponse, error) {
	type BatchRequest struct {
		Queries []map[string]interface{} `json:"queries"`
	}
	req := BatchRequest{Queries: queries}
	reqData, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}

	ncMu.RLock()
	nc := natsConn
	ncMu.RUnlock()
	if nc == nil {
		return nil, fmt.Errorf("NATS not connected")
	}

	msg, err := nc.RequestWithContext(ctx, "db.batch.query", reqData)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Success bool         `json:"success"`
		Results []DBResponse `json:"results"`
	}
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// ============================================
// HANDLERS
// ============================================
func handleDBRequest(c *gin.Context, queryType string, params map[string]interface{}) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), config.NATSTimeout)
	defer cancel()

	resp, err := sendDBRequest(ctx, queryType, params)
	if err != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{
			"success": false,
			"error":   err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func handleGetDanhSachLopHocPhan(c *gin.Context) {
	tenMonHocs := c.QueryArray("TenMonHoc")

	if len(tenMonHocs) == 0 {
		if single := c.Query("TenMonHoc"); single != "" {
			tenMonHocs = []string{single}
		}
	}

	if len(tenMonHocs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu TenMonHoc"})
		return
	}

	params := map[string]interface{}{"TenMonHoc": tenMonHocs}
	handleDBRequest(c, "GET_DANH_SACH_LOP_HOC_PHAN", params)
}

func handleGetChiTietLopHocPhan(c *gin.Context) {
	idLopHocPhan := c.Query("idLopHocPhan")
	if idLopHocPhan == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu idLopHocPhan"})
		return
	}

	params := map[string]interface{}{"idLopHocPhan": idLopHocPhan}
	handleDBRequest(c, "GET_CHI_TIET_LOP_HOC_PHAN", params)
}

func handleBatchGetCounters(c *gin.Context) {
	maLopHocPhans := c.QueryArray("maLopHocPhans")
	if len(maLopHocPhans) == 0 {
		if single := c.Query("maLopHocPhans"); single != "" {
			maLopHocPhans = []string{single}
		}
	}

	if len(maLopHocPhans) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu maLopHocPhans"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), config.NATSTimeout)
	defer cancel()

	queries := make([]map[string]interface{}, len(maLopHocPhans))
	for i, maLHP := range maLopHocPhans {
		queries[i] = map[string]interface{}{
			"queryType": "BATCH_GET_COUNTERS",
			"params": map[string]interface{}{
				"maLopHocPhans": []string{maLHP},
			},
		}
	}

	results, err := sendBatchDBRequest(ctx, queries)

	if err != nil {
		c.JSON(http.StatusGatewayTimeout, gin.H{"success": false, "error": err.Error()})
		return
	}

	counterMap := make(map[string]int)
	for _, r := range results {
		if r.Success {
			if m, ok := r.Data.(map[string]interface{}); ok {
				for k, v := range m {
					if vi, ok := v.(float64); ok {
						counterMap[k] = int(vi)
					}
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"success": true, "data": counterMap})
}

// ============================================
// ROUTES
// ============================================
func setupRouter() *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(gin.Logger())

	r.GET("/", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"message": "API healthy"})
	})

	api := r.Group("/DangKyHocPhan")
	{
		// 1. CÁC API TRA CỨU -> DÙNG GET ĐỂ CLOUDFLARE CACHE
		api.GET("/GetChiTietLopHocPhan", handleGetChiTietLopHocPhan)
		api.GET("/GetDanhSachLopHocPhan", handleGetDanhSachLopHocPhan)
		api.GET("/BatchGetCounters", handleBatchGetCounters)

		api.GET("/GetDanhSachMonHocPhanDangKy", func(c *gin.Context) {
			maSinhVien := c.Query("masinhvien")
			dotDangKy := c.Query("dotDangKy")
			hinhThuc := c.Query("hinhThuc")

			if maSinhVien == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu masinhvien"})
				return
			}
			params := map[string]interface{}{
				"masinhvien": maSinhVien,
				"dotDangKy":  dotDangKy,
				"hinhThuc":   hinhThuc,
			}
			handleDBRequest(c, "GET_DANH_SACH_MON_HOC_PHAN_DANG_KY", params)
		})

		// 2. CÁC API ĐĂNG KÝ (GHI DỮ LIỆU) -> BẮT BUỘC DÙNG POST
		api.POST("/DangKyMonHoc", func(c *gin.Context) {
			var req struct {
				MaSinhVien   string `json:"maSinhVien"`
				MaLopHocPhan string `json:"maLopHocPhan"`
				DotDangKy    string `json:"dotDangKy"`
				HinhThuc     string `json:"hinhThuc"`
			}
			if err := c.ShouldBindJSON(&req); err != nil || req.MaSinhVien == "" || req.MaLopHocPhan == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu thông tin"})
				return
			}
			params := map[string]interface{}{
				"maSinhVien":   req.MaSinhVien,
				"maLopHocPhan": req.MaLopHocPhan,
				"dotDangKy":    req.DotDangKy,
				"hinhThuc":     req.HinhThuc,
			}
			handleDBRequest(c, "DANG_KY_MON_HOC", params)
		})

		api.POST("/HuyDangKy", func(c *gin.Context) {
			var req struct {
				MaSinhVien   string `json:"maSinhVien"`
				MaLopHocPhan string `json:"maLopHocPhan"`
			}
			if err := c.ShouldBindJSON(&req); err != nil || req.MaSinhVien == "" || req.MaLopHocPhan == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "Thiếu thông tin"})
				return
			}
			params := map[string]interface{}{
				"maSinhVien":   req.MaSinhVien,
				"maLopHocPhan": req.MaLopHocPhan,
			}
			handleDBRequest(c, "HUY_DANG_KY", params)
		})
	}

	return r
}

// ============================================
// MAIN
// ============================================
func main() {
	if err := initNATS(); err != nil {
		log.Fatalf("❌ NATS init failed: %v", err)
	}

	gin.SetMode(gin.ReleaseMode)
	router := setupRouter()

	srv := &http.Server{
		Addr:              config.ServerPort,
		Handler:           router,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}

	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit
		log.Println("Shutting down server...")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			log.Fatalf("Server forced shutdown: %v", err)
		}

		ncMu.Lock()
		if natsConn != nil {
			natsConn.Close()
		}
		ncMu.Unlock()

		log.Println("Server exited")
	}()

	log.Printf("🚀 Go API Service listening on %s", config.ServerPort)

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server error: %v", err)
	}
}
