# REPORT — Lab Day 9

| | |
|---|---|
| Họ và tên | Duong Van Thanh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602368` |
| GitHub | https://github.com/JJayzdev/K4-L3L4-Track4-DataPipeline4ComputerVision |
| Trip dùng cho bài tập sập và chạy lại | `trip-00003` |

## Kết quả cuối

- `dataset_version` của bạn: `55e5ca990413`.
- `run_summary.json` ghi `verify_all_passed: true`; toàn bộ kiểm tra đạt `27/27`.
- Đã tạo đủ `run_summary.json`, `idempotency.json`, `break_gate.json`, `leakage.json`, `threshold.json`.

## Bước 1 · Bronze

### Câu 1

Telemetry A dùng `ts_utc`, `speed_mps`, `heading_deg`, `lat`, `lon`; B dùng `timestamp_ms`, `spd_kmh`, `hdg`, `latitude`, `longitude`. A dùng m/s và ISO UTC, còn B dùng km/h và epoch milliseconds; các cột điều khiển cũng khác. Hai nguồn được giữ ở hai bảng Bronze riêng để bảo toàn schema gốc, lineage và lỗi dữ liệu. Các ô để dạng chuỗi giúp không ép kiểu hoặc làm mất thông tin trước bước chuẩn hóa.

## Bước 2 · Silver telemetry

### Câu 2

Staging đổi tên cột về schema chung, chuyển km/h sang m/s, epoch milliseconds sang UTC và trừ clock offset 1.5 giây của drive B. Dòng sạch là A: 2968 và B: 3181. Mỗi drive có 20 duplicate, 30 thiếu tốc độ và 3 tốc độ ngoài phạm vi; tổng quarantine là 106 dòng. Giữ rejects giúp truy nguyên, kiểm toán và sửa nguồn thay vì xóa dữ liệu âm thầm.

### Câu 3

`max_gap_ms` là `warn` vì mất tín hiệu cần theo dõi nhưng không luôn làm dữ liệu vô dụng. `in_range` là `error` vì tốc độ sai vật lý làm hỏng phân tích và training. `break_gate.json` ghi rằng khi nới lên 1000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` bắt 6 dòng, kết quả `PASS=5 WARN=1 ERROR=1`; khôi phục luật cho `PASS=6 WARN=1 ERROR=0`. Quality gate vì vậy thực sự chặn dữ liệu xấu.

## Bước 3 · Silver frames

### Câu 4

`idempotency.json` ghi bảng có 1246 dòng trước và sau hai lần chạy lại, fingerprint đều `6c7480dfde7e`. Trip `trip-00003` có 60 frame; append sai hai lần sẽ thành 120. Delta overwrite theo predicate `trip_id` thay thế đúng partition cần chạy lại, không nhân đôi trip khác và không cần xóa toàn bộ bảng. Đây là cách phục hồi cục bộ và an toàn hơn xóa rồi ghi lại thủ công.

### Câu 5

Frame đen gần như không có nội dung nên có thể loại; frame tối vẫn có thể chứa đường hoặc vật cản nên được giữ và gắn cờ. Với dHash khác tối đa 0, 2, 6 bit, số frame trùng A/B lần lượt là `61/8`, `149/53`, `363/244`. Pipeline chọn 2 để bắt frame gần giống mà không quá mạnh. Xe chạy chậm hoặc camera đứng hình làm frame liên tiếp giống nhau, nên ngưỡng cao dễ loại nhầm cảnh hợp lệ.

## Bước 4 · Gold

### Câu 6

`leakage.json` cho thấy chia theo frame làm cả 26 trip straddle nhiều split và cả 26 trip có frame ở train lẫn test. Chia theo trip của pipeline có 0 trip bị cắt. Các frame liên tiếp trong một cảnh gần giống nhau, nên chia theo frame làm điểm test cao giả tạo vì mô hình gần như đã thấy cảnh đó ở train. Chia theo trip phản ánh tổng quát hóa sang cảnh chưa thấy.

### Câu 7

`dataset_version` là hash nội dung nên không phụ thuộc thời điểm hay số lần chạy. Cùng dữ liệu và MSSV cho cùng version; lần này là `55e5ca990413`. MSSV được trộn vào thứ tự trip trước khi gán split, vì vậy bạn khác có version khác dù dùng cùng dữ liệu. Version giúp kiểm tra tính tái lập và phát hiện dataset không đúng người.

### Câu 8

Manifest ghi dataset `55e5ca990413`, 1019 frame, train 707, val 120, test 192 và lineage của các bảng upstream. Để tái tạo sau ba tháng cần giữ Bronze, các version upstream, cấu hình dbt về tỷ lệ split/ngưỡng chất lượng và `LAB_STUDENT_ID=2A202602368`. URI ảnh cùng manifest cho phép kiểm tra từng file đầu ra và phát hiện file thiếu.

## Bước 5 · Điều phối và phục vụ

### Câu 9

Trip retry là `trip-00003`. Chạy lại partition hai lần giữ toàn bảng ở 1246 dòng và fingerprint không đổi. Trong Airflow, task extraction lỗi chuyển đỏ và downstream phụ thuộc có thể `upstream_failed`; clear đúng task trip rồi chạy lại. Nhờ ghi theo partition và idempotent, dữ liệu không nhân đôi và dataset version không đổi nếu đầu vào không đổi. Evidence là `idempotency.json`.

### Câu 10

Ở quy mô 500 xe/ngày, tách video thành frame, object storage/Delta và join-transform bằng một DuckDB đơn lẻ sẽ nghẽn trước. Tôi sẽ dùng queue và worker phân tán theo trip/xe, object storage partitioned có catalog, rồi dùng engine xử lý phân tán hoặc incremental ELT. Airflow tiếp tục điều phối batch và quality gate, nhưng không nên tự xử lý toàn bộ dữ liệu nặng trong một worker.

## Câu hỏi riêng trên Metabase

SQL đề xuất để tìm trip có tỷ lệ frame bị loại cao nhất:

```sql
select trip_id, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / frames, 2) as rejected_pct
from marts.mart_trip_summary
order by rejected_pct desc
limit 10;
```

Truy vấn giúp ưu tiên điều tra trip có video kém hoặc mất đồng bộ nhiều nhất; dashboard có thể so sánh tổng frame với frame giữ lại theo trip.

## Nhìn lại

Điều đáng chú ý là 106 dòng telemetry lỗi không bị xóa mà được phân loại theo drive và nguyên nhân. Leakage cho thấy chia theo frame làm cả 26 trip xuất hiện ở train và test. Tôi còn cần bổ sung ảnh Airflow, MinIO và Metabase trước khi nộp. Một giới hạn là các cờ chất lượng dùng heuristic; dữ liệu vẫn có thể sai ngữ nghĩa dù không vi phạm ngưỡng.



