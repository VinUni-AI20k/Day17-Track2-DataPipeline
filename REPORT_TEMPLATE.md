# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Điền Mạnh Hùng  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-17

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
run 1/3 … 9.6s
run 2/3 … 10.0s
run 3/3 … 10.1s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
─────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
─────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    f8d3f591f0    f8d3f591f0    f8d3f591f0   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
─────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
```

</details>

Tổng kết: **4/4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau khi bấm Clear Task trong Airflow, `gold_training_set` tăng số hàng sau mỗi lần chạy lại. Không có báo lỗi. |
| **Nguyên nhân** | Incremental model không khai báo `unique_key`, nên dbt sinh ra câu `INSERT INTO ...` (append) thay vì `MERGE INTO ... ON CONFLICT`. Chạy lại cùng partition ngày → dữ liệu cũ không bị thay thế mà được ghi thêm vào cuối bảng. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào config. `dags/ai_training_pipeline.py`: đổi `catchup=True` → `catchup=False`, thêm `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38,750 hàng (thừa 26,270) · Sau: 12,480 hàng · Checksum 3 lượt: 8dd7c98653 ✓ |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` thiếu khoảng 5% (455 hàng) so với đối chiếu thủ công. Chỉ thiếu ở những ngày đã chạy xong từ lâu, ngày mới thì đủ. |
| **P99 độ trễ đo được** | **2.73 ngày** |
| **Lookback đã chọn** | **3 ngày** — vì P99 = 2.73 ngày, làm tròn lên thành 3 ngày |
| **Nguyên nhân** | Điều kiện lọc `event_date > max(event_date)` chỉ xử lý ngày lớn hơn ngày lớn nhất đã có. Nhưng nguồn dữ liệu có 5.1% bản ghi tới kho muộn hơn 1 ngày, P99 = 2.73 ngày. Một event xảy ra ngày 08-12 nhưng được ingest ngày 08-15 sẽ bị bỏ qua ở lượt 08-15 vì điều kiện `>` ngăn nó đi qua. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: (1) đổi `>` thành `>=` và lùi thêm 3 ngày: `event_date >= max(event_date) - interval 3 day`; (2) thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` để lần tính sau thay thế lần tính trước. |
| **Bằng chứng** | Trước: 8,645 hàng (thiếu 455) · Sau: 9,100 hàng · Checksum 3 lượt: f8d3f591f0 ✓ |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 (2.73 ngày) chọn vì nó bao phủ 99% dữ liệu mà không bị ảnh hưởng bởi outlier hiếm gặp (max = 2.94 ngày). Dùng `max` làm căn cứ sẽ lùi 3 ngày luôn, nhưng mỗi ngày lùi thêm tốn chi phí quét lại ở mọi lượt chạy sau — và 0.21 ngày dư thừa (2.94 - 2.73) chỉ phục vụ một outlier hiếm. P99 là điểm cân bằng giữa độ phủ và chi phí.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Team backend đổi kiểu cột `priority` từ số sang chuỗi hôm 08-10. Pipeline không dừng nhưng `silver_tickets.priority` chứa NULL, 0, 5, -1 (6,606 hàng sai). |
| **Nguyên nhân** | Biểu thức `try_cast(priority_raw as integer)` chỉ xử lý giá trị đã là số. Khi nguồn đổi từ `'1','2','3','4'` sang `'urgent','high','medium','low'`, cast thất bại và trả về NULL cho tất cả — bao gồm cả giá trị lỗi thật lẫn nhãn chuỗi hợp lệ mới. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **Nhóm 1** ('1','2','3','4'): đúng contract cũ → giữ nguyên. **Nhóm 2** ('urgent','high','medium','low'): schema evolution, ý nghĩa không đổi → map: urgent→1, high→2, medium→3, low→4. **Nhóm 3** ('P1','unknown','0','5','-1','',NULL): dữ liệu lỗi → trả về NULL để đi vào quarantine. |
| **Cách khắc phục** | (1) `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng CASE xử lý đủ 3 nhóm. (2) `dbt/models/silver/silver_tickets.sql`: lọc NULL **trước** xếp hạng để không loại cả ticket. (3) `dbt/models/silver/quarantine_tickets.sql`: đổi `where false` → `where normalize_priority(...) is null`. (4) `dbt/models/silver/schema.yml`: bật `enforced: true`, thêm test `accepted_values: [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` = 11/11 pass ✓ · `silver_tickets.priority ∈ 1..4, không NULL` = sạch ✓ |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver** vì Bronze giữ nguyên dữ liệu thô để phục vụ điều tra sự cố về sau. Nếu Bronze từ chối row lỗi, không còn bản ghi gốc để traceback. **Không dừng pipeline** vì 312 bản ghi lỗi (~2%) không có quyền chặn hơn 130,000 event và 31,200 chunk hoàn toàn bình thường. Quarantine là hàng đợi cho người trực xử lý thủ công, không phải lý do để stop toàn bộ hệ thống.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

### Bài A — Query Dashboard Chậm (+5 điểm)

| | |
|---|---|
| **Triệu chứng** | Dashboard mất 38 giây load, 5,000 file Parquet nhỏ (small-file problem). Query filter `strftime(event_time, '%Y-%m-%d') = '...'` không sargable. |
| **Nguyên nhân** | (1) 5,000 file nhỏ → DuckDB phải đọc 5M rows (mỗi file làm tròn lên ~1K rows). (2) Filter dùng hàm `strftime()` → engine không thể dùng partition pruning hay min/max statistics. |
| **Cách khắc phục** | (1) `tools/compact.py`: partition theo `event_date`, ORDER BY `event_date, customer_name`, ROW_GROUP_SIZE=10000. (2) `queries/dashboard.sql`: đổi path sang `gold_events_v2/**`, bật `hive_partitioning=true`, viết lại filter sargable: `event_time >= '2026-08-09' AND event_time < '2026-08-10'`. |
| **Bằng chứng** | rows scanned: 5,000,000 → 137,368 (giảm 36.4×) · files: 5,000 → 14 · result hash: không đổi ✓ |

---

### Bài B — Consumer Crash (+5 điểm)

| | |
|---|---|
| **Triệu chứng** | `make crash-test` cho thấy: crash ở lô 7 → mất 500 hàng. Chạy lại sau crash không đủ 20,000 hàng. |
| **Nguyên nhân** | Thứ tự `consumer.commit()` trước `write_batch()` → at-most-once semantics. Offset được commit nhưng batch chưa ghi xong → crash = mất dữ liệu. |
| **Cách khắc phục** | (1) Đảo thứ tự: `write_batch()` → `commit()`. (2) Thêm `primary key` vào `event_id` trong DDL. (3) Đổi `INSERT` thuần thành `INSERT ... ON CONFLICT (event_id) DO UPDATE` để đảm bảo idempotent. |
| **Bằng chứng** | `make crash-test`: ✓ không mất bản ghi · ✓ không trùng · ✓ C == A (20,000 = 20,000) · `make verify`: 4/4 tiêu chí đạt |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra tất cả incremental model có `unique_key` và `incremental_strategy` đúng grain (entity vs event). Thử chạy lại một partition và soát số hàng trước/sau. |
| 2 | Đo phân bố độ trễ của dữ liệu từ nguồn đến warehouse (P50/P95/P99). Tính lookback window dựa trên P99. |
| 3 | Đối chiếu schema giữa source và target. Kiểm tra xem có giá trị nằm ngoài contract không. Tách bản ghi lỗi riêng thay vì để pipeline dừng. |
