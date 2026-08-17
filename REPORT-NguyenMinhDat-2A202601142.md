# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Minh Đạt  **Lớp:** Track 2 - K4 - E403  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 40.9s
  run 2/3 … 45.7s
  run 3/3 … 45.4s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✗ 5,000,000 → 5,000,000 (1.0×, cần ≥ 10×)
    số file parquet                           ✗ 5,000 → 5,000
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

*(Dòng "dashboard rows scanned" / "số file parquet" thuộc Bài mở rộng A trong EXTRA.md — không bắt buộc, chưa làm.)*

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Chạy `make pipeline` lần 1: `gold_training_set` = 13.790 hàng. Chạy lần 2 (không reset): 26.270 hàng. Chênh lệch = 12.480 — đúng bằng số ticket kỳ vọng (`expected/gold_training_set.count`). Mỗi lần chạy lại, toàn bộ snapshot ticket hiện tại bị ghi thêm một lần nữa thay vì ghi đè. |
| **Nguyên nhân** | Model `gold_training_set` là incremental nhưng không khai báo `unique_key`, nên dbt sinh câu `INSERT` thuần thay vì `MERGE`. `make pipeline` chạy 14 lượt dbt tuần tự (1 lượt/ngày); `silver_tickets` được rebuild mỗi ngày phản ánh trạng thái mới nhất. Một ticket tạo ở D1 rồi bị sửa (op='u') ở D2 sẽ có `_ingested_at` nhảy từ D1 sang D2 trong `silver_tickets` — nên lọt qua điều kiện `WHERE _ingested_at` của CẢ hai lượt ngày D1 và D2, bị `INSERT` hai lần vì không có key để dbt biết cần thay thế bản cũ thay vì ghi thêm. |
| **Cách khắc phục** | (1) `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. (2) `dags/ai_training_pipeline.py`: đặt `catchup=False` (không tự chạy bù các ngày đã lỡ) và `max_active_runs=1` (không cho nhiều run ghi đồng thời vào cùng bảng) — đây là biện pháp giảm tần suất kích hoạt lỗi ở tầng vận hành, không thay thế cho việc sửa model. |
| **Bằng chứng** | trước: 13.790 hàng (lượt 1) → 26.270 hàng (lượt 2, +12.480) · sau khi sửa: ổn định **12.480** hàng cả 3 lượt, checksum giống hệt nhau (`8622572a97` × 3) · `gold_training_set: 1 hàng/1 ticket` ✓ · nguồn: `silver_tickets` sạch (12.480=12.480) · `bronze_tickets_cdc` op='u'=1.310 = đúng phần dư 13.790−12.480 |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` = 8.645 hàng, thiếu 455 so với kỳ vọng 9.100 (14 ngày × 650 customer). Chỉ thiếu ở các ngày đã chạy xong từ lâu, ngày mới thì đủ — khớp phiếu #1043. |
| **P99 độ trễ đo được** | **≈ 2,73 ngày** *(bắt buộc)* — đo bằng `quantile_cont(date_diff('second', event_time, _ingested_at)/86400.0, 0.99)` trên `bronze_events`. Phân bố có 2 cụm tách biệt: đa số đến trong 0-6h, ~5,05% đến muộn 43-71h (1,8-2,94 ngày) — khớp tỷ lệ thiếu ~5% trong triệu chứng. Max ≈ 2,94 ngày. |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99 (2,73 ngày). Không dùng `max` (2,94 ngày) vì đó chỉ là giá trị lớn nhất quan sát được trong đúng 130k dòng hiện có — không đại diện cho phân bố tổng thể, và mỗi ngày lùi thêm là chi phí quét thêm dữ liệu ở **mọi lượt chạy sau này** (vĩnh viễn), không phải chi phí một lần. |
| **Nguyên nhân** | Điều kiện `WHERE event_date > (select max(event_date) from {{ this }})` chỉ chấp nhận sự kiện có `event_date` lớn hơn ngày lớn nhất đã có trong bảng đích. Khi một sự kiện xảy ra ngày 08-12 nhưng tới kho muộn (ví dụ ngày 08-15), tại lượt chạy 08-15 thì `max(event_date)` trong bảng đích đã là 08-15 — điều kiện `08-12 > 08-15` sai nên bản ghi bị loại. Vì `max(event_date)` chỉ tăng dần theo thời gian, bản ghi đó **không bao giờ** được xử lý lại ở bất kỳ lượt chạy nào sau đó — giải thích vì sao chỉ các ngày cũ (đã "đóng sổ") mới bị thiếu, còn ngày mới (chưa có gì trong target để so sánh) vẫn đủ. |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: (1) đổi điều kiện lọc thành `where event_date > (select max(event_date) from {{ this }}) - interval '3 days'` để mở lại cửa sổ 3 ngày gần nhất mỗi lượt chạy; (2) thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` vào `config()` — bắt buộc phải có, vì mở rộng window khiến cùng một cặp (ngày, khách hàng) được tính lại ở nhiều lượt chạy; không có merge sẽ tái tạo đúng lỗi của Nhiệm vụ 1. |
| **Bằng chứng** | trước: 8.645 hàng (thiếu 455) · sau: ổn định **9.100** hàng cả 3 lượt, checksum giống hệt (`3db448685c` × 3) |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` chỉ là **một giá trị quan sát được** trong đúng tập dữ liệu hiện có — một outlier hiếm gặp (lỗi mạng, retry của producer...) có thể kéo `max` lên rất cao mà không đại diện cho phân bố thật. `P99` là một thước đo thống kê ổn định hơn: bao trọn 99% trường hợp mà không bị một điểm dị thường chi phối. Về chi phí: mỗi ngày lùi thêm trong lookback window không phải trả một lần — nó bị quét lại ở **mọi lượt chạy incremental sau này, mãi mãi**. Chọn theo `max` nghĩa là chấp nhận trả chi phí quét lớn nhất vĩnh viễn chỉ để vá đúng một lần dị thường đã quan sát được; chọn theo P99 chấp nhận có thể bỏ sót phần đuôi hiếm (khoảng 1%) để đổi lấy chi phí quét thấp hơn ở mọi lượt chạy — phần đuôi hiếm đó, nếu cần, nên xử lý bằng cơ chế đối soát định kỳ riêng chứ không phải bằng cách nới window theo giá trị cực đoan nhất từng thấy.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority`: 6.488/12.480 ticket là NULL, ngoài ra còn giá trị `-1`, `0`, `5` vi phạm miền 1..4. `quarantine_tickets` = 0 hàng dù kỳ vọng 312. Team backend đổi `priority` từ số sang chuỗi (`urgent/high/medium/low`) từ 08-10, pipeline không dừng nhưng dữ liệu sai lặng lẽ lọt qua. |
| **Nguyên nhân** | Macro `normalize_priority` dùng `try_cast(priority_raw as integer)`, sai theo hai hướng ngược nhau: (1) biến mọi nhãn chữ hợp lệ `urgent/high/medium/low` thành NULL, dù ý nghĩa dữ liệu không đổi — team backend chỉ đổi cách biểu diễn từ 08-10, không đổi ý nghĩa; (2) lại chấp nhận các số ngoài miền hợp lệ như `0`, `5`, `-1` vì chúng ép kiểu integer thành công, trong khi contract quy định priority chỉ ∈ 1..4. Đồng thời `contract.enforced` đang tắt nên dbt không kiểm tra kiểu dữ liệu cột `priority`, và không có model nào tách bản ghi lỗi ra khỏi luồng chính — mọi giá trị sai (kể cả NULL) đều lặng lẽ đi thẳng vào Silver rồi lan xuống Gold, không có log hay cảnh báo. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | Số hợp lệ `1,2,3,4` (6.846 row) → giữ nguyên · Nhãn chuỗi `urgent/high/medium/low` (7.142 row, đổi format nhưng đúng ý nghĩa — schema evolution) → map về 1..4 · Dữ liệu lỗi thật `0,5,-1,'',unknown,P1,P2,NULL` (312 row, khớp đúng expected) → quarantine |
| **Cách khắc phục** | (1) `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng `CASE` — giữ nguyên số hợp lệ trong 1..4, map 4 nhãn chữ về số, còn lại trả `NULL`. (2) `dbt/models/silver/silver_tickets.sql`: tách CTE lọc `priority_clean is not null` **trước** khi `row_number()` xếp hạng — để chỉ loại bản ghi CDC hỏng, không loại mất cả ticket khi bản ghi mới nhất của nó bị hỏng. (3) `dbt/models/silver/quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null` — dùng đúng macro nên không thể lệch với Silver. (4) `dbt/models/silver/schema.yml`: bật `contract.enforced: true`, thêm test `not_null` + `accepted_values: [1,2,3,4]` cho cột `priority`. |
| **Bằng chứng** | trước: 6.606 hàng sai domain, `quarantine_tickets` 0/312 · sau: `quarantine_tickets` = **312/312** ổn định, `silver_tickets.priority` sạch (∈1..4, không NULL) ✓, `gold_training_set` vẫn giữ 12.480 · `dbt test`: **11/11 pass** (bản gốc 9 → +2 test mới) |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Nên chặn ở **tầng Silver**, không phải Bronze. Bronze phải giữ nguyên dữ liệu thô — nếu Bronze từ chối luôn bản ghi có `priority_raw` lạ, ta mất bằng chứng gốc để điều tra sau này (nguồn thực sự gửi gì, lúc nào, có phải lỗi hệ thống nguồn hay lỗi xử lý ở phía mình). Contract và business rule (miền giá trị 1..4, cách map nhãn chữ) là logic nghiệp vụ, thuộc về tầng Silver — nơi dữ liệu được chuẩn hoá.
>
> Không nên để `dbt test` fail và dừng cả DAG vì quy mô: 312 bản ghi lỗi trên tổng 14.300 bản ghi CDC (≈2,2%), trong khi hơn 130.000 event và 31.200 chunk khác hoàn toàn hợp lệ vẫn đang chờ phục vụ RAG index, classifier và routing agent. Dừng toàn bộ pipeline vì một phần nhỏ dữ liệu lỗi sẽ chặn luôn phần lớn dữ liệu tốt đến tay người dùng — thiệt hại lớn hơn nhiều so với lợi ích của việc "phát hiện lỗi ngay". `quarantine_tickets` cho phép pipeline tiếp tục chạy, đồng thời giữ lại đúng các bản ghi lỗi thành một hàng đợi để người trực xử lý riêng.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm — tập trung hoàn thành 3 nhiệm vụ chính trong thời gian lab. |
| **Nguyên nhân** | — |
| **Cách khắc phục** | — |
| **Bằng chứng** | — |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Với mọi model `materialized = 'incremental'`, đọc ngay `config()` xem `unique_key` và `incremental_strategy` đã khai báo chưa — thiếu một trong hai là dấu hiệu model có thể đang chỉ `append`. Đồng thời kiểm tra `catchup`/`max_active_runs` của scheduler, vì đó là nơi một thao tác retry/backfill tưởng vô hại biến lỗi tiềm ẩn thành sự cố thực tế. |
| 2 | Với bất kỳ điều kiện lọc theo thời gian nào trong `is_incremental()`, đo phân bố độ trễ thực tế giữa lúc dữ liệu sinh ra và lúc nó tới kho (P99) trước khi tin vào bất kỳ mốc so sánh nào — một điều kiện chỉ dựa vào giá trị lớn nhất hiện có trong bảng đích luôn có nguy cơ âm thầm bỏ sót dữ liệu đến muộn, và triệu chứng sẽ không phải là lỗi mà là "thiếu vài % không rõ lý do". |
| 3 | Kiểm tra `contract`/schema đang `enforced` hay không, và pipeline có nơi tách riêng bản ghi không đạt chuẩn (quarantine) hay chỉ âm thầm `try_cast`/bỏ qua. Lỗi kiểu dữ liệu lặng lẽ là loại lỗi nguy hiểm nhất trong vận hành — không có log, không dừng job, chỉ lộ ra gián tiếp qua chất lượng model giảm dần theo thời gian. |
