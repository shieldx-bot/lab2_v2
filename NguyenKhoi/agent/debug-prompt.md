# 🎯 Prompt: Phân Tích Điểm Nghẽn Hệ Thống Kubernetes

## Context

Bạn là một AI SRE đang vận hành hệ thống **Đăng ký học phần** trên Kubernetes. Hệ thống đang có hiệu năng kém và cần bạn điều tra.

### Kiến trúc hệ thống

```
K6 Load Test (100 req/s, max 600 VUs)
    ↓ HTTP POST (JSON)
API Service (Node.js, KEDA auto-scale 2-30 replicas)
    ↓ NATS (db.query subject)
DB Service (Node.js, 3 replicas, PostgreSQL connection pool)
    ↓ SQL
PostgreSQL (metricdb-rw)
↑ Redis Cache (TTL: 10 phút)
```

### Kết quả load test hiện tại

```
http_req_duration avg=4.22s  p(90)=10.7s  p(95)=11.38s  max=12.11s
dropped_iterations = 506 (~17% requests bị rớt)
http_reqs = 65.29/s  (target: 100/s)
Cache: Nghi ngờ cache HIT rate = 0% (do lỗi generateCacheKey)
```

### Nhiệm vụ của bạn

Hãy điều tra và trả lời các câu hỏi sau:

## 1. Redis Cache Analysis

Chạy các lệnh sau:

```bash
# Kiểm tra Redis cache stats
kubectl exec -it <redis-pod> -- redis-cli INFO stats
kubectl exec -it <redis-pod> -- redis-cli INFO keyspace
kubectl exec -it <redis-pod> -- redis-cli DBSIZE
kubectl exec -it <redis-pod> -- redis-cli MONITOR | head -20
```

**Cần trả lời:**
- Cache keyspace có bao nhiêu keys?
- `keyspace_hits` vs `keyspace_misses` ratio là bao nhiêu?
- Cache TTL có đang hoạt động không?
- Có bao nhiêu API pods đang chạy thực tế? (`kubectl get pods -l app=api-services`)

## 2. NATS Queue Analysis

```bash
# Kiểm tra NATS stats
kubectl exec -it <nats-pod> -- nats-server --signal quit  # không chạy lệnh này

# Dùng nats CLI tool
kubectl exec -it <nats-pod> -- nats-sub -a -s nats://localhost:4222
# hoặc port-forward và dùng nats CLI
nats server check connection -s nats://localhost:4222
```

**Cần trả lời:**
- Số lượng messages đang pending trong subject `db.query`?
- Consumer `db-workers` có bao nhiêu workers active?
- Response subject có bị accumulate không?

## 3. DB Service Analysis

```bash
# Logs của DB service pods
kubectl logs -l app=db-service --tail=100

# Kiểm tra resource usage
kubectl top pod -l app=db-service

# Kiểm tra connection pool
kubectl exec -it <db-pod> -- node -e "
const { Pool } = require('pg');
const pool = new Pool({ ... });
pool.query('SELECT count(*) FROM pg_stat_activity').then(r => console.log(r.rows));
"
```

**Cần trả lời:**
- Có bao nhiêu DB connections đang active trong PostgreSQL?
- Thời gian xử lý trung bình của mỗi query là bao lâu?
- Có query nào đang bị lock hay chậm không?
- Logs có xuất hiện lỗi timeout, connection refused không?

## 4. API Service Analysis

```bash
# Logs của API service
kubectl logs -l app=api-services --tail=100

# Resource usage
kubectl top pod -l app=api-services

# Kiểm tra event loop lag (nếu có thể exec vào pod)
kubectl exec -it <api-pod> -- node -e "
const http = require('http');
const start = Date.now();
setImmediate(() => {
  console.log('Event loop delay (ms):', Date.now() - start);
});
"
```

**Cần trả lời:**
- Log có `Cache HIT` hay toàn `Cache MISS`?
- Log có `DB service timeout` hay `504` errors?
- Event loop có bị block không?
- CPU/memory usage của API pods có cao không?

## 5. PostgreSQL Slow Query Analysis

```bash
# Kết nối đến PostgreSQL và kiểm tra slow queries
kubectl exec -it <postgres-pod> -- psql -c "
SELECT 
  query, 
  calls, 
  mean_time, 
  total_time,
  rows
FROM pg_stat_statements 
ORDER BY total_time DESC 
LIMIT 10;
"

# Kiểm tra connections đang active
kubectl exec -it <postgres-pod> -- psql -c "
SELECT count(*) as total_connections,
  (SELECT count(*) FROM pg_stat_activity WHERE state = 'active') as active,
  (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle') as idle,
  (SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction') as idle_in_transaction
FROM pg_stat_activity;
"

# Kiểm tra locks
kubectl exec -it <postgres-pod> -- psql -c "
SELECT relation::regclass, mode, granted FROM pg_locks;
"
```

## 6. Network & NATS Latency

```bash
# Kiểm tra latency giữa các services
kubectl run -it --rm nettest --image=alpine -- sh -c "
apk add curl && \
for svc in api-services db-service nats-js redis; do
  echo '---' && \
  echo \"Testing \$svc...\" && \
  time curl -s -o /dev/null http://\$svc.default.svc.cluster.local:3000/ 2>&1 || true
done
"
```

## Output yêu cầu

Sau khi chạy các lệnh trên, hãy tổng hợp kết quả theo format:

```markdown
## 🎯 Kết Luận

### Điểm nghẽn #1: [TÊN]
- **Bằng chứng:** [kết quả từ lệnh]
- **Nguyên nhân gốc:** [phân tích]
- **Impact:** [tác động lên hệ thống]

### Điểm nghẽn #2: [TÊN]
...

## 🚀 Khuyến Nghị

### Priority 1: [Hành động]
- **File cần sửa:** ...
- **Mô tả:** ...
- **Dự kiến tác động:** ...

### Priority 2: ...
```

## Lưu ý quan trọng

1. Nếu Redis `keyspace_misses` cao gấp nhiều lần `keyspace_hits` → Cache không hoạt động, đây là root cause chính
2. Nếu API logs thấy `Cache MISS` cho mọi request → lỗi `generateCacheKey` sort từng ký tự
3. Nếu DB logs thấy query chậm > 1s → thiếu index PostgreSQL hoặc connection pool
4. Nếu NATS có pending messages > 100 → DB workers không kịp xử lý
5. Nếu `dropped_iterations` vẫn cao dù resources đủ → lỗi application logic

Kết luận: Cause gốc rất có thể là **cache không HIT** do bug, gây quá tải DB và timeout hàng loạt.