#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — NHIỆM VỤ 4.  CHƯA CÓ LOGIC.

Hiện trạng: `data/gold_events/` gồm 5.000 file, mỗi file vài chục KB, không
partition, thứ tự hàng ngẫu nhiên.

Yêu cầu: đọc toàn bộ dataset cũ, ghi ra dataset mới có layout hợp lý hơn, sau đó cập
nhật `queries/dashboard.sql` để trỏ vào dataset mới.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

KHUNG THỰC HIỆN

    COPY (
        SELECT *
        FROM   read_parquet('data/gold_events/*.parquet')
        ORDER  BY <cột A>, <cột B>
    ) TO 'data/gold_events_v2' (
        FORMAT          parquet,
        PARTITION_BY    (<cột partition>),
        OVERWRITE_OR_IGNORE,
        ROW_GROUP_SIZE  <?>
    )

Ba quyết định, mỗi quyết định cần một lý do viết được ra giấy:

  <cột partition>   Engine chỉ bỏ qua được file mà nó biết là vô ích TRƯỚC khi
                    mở file. Thông tin đó đến từ đường dẫn. Vậy cột nào của
                    truy vấn dashboard nên xuất hiện trong tên thư mục? Cột đó
                    có bao nhiêu giá trị phân biệt — tức bao nhiêu thư mục?
                    Partition theo cột có 650 giá trị thì hệ quả là gì?

  <cột A>, <cột B>  Thứ tự hàng trong file quyết định thống kê min/max của mỗi
                    row group có ích hay vô dụng. Sắp thế nào để các hàng cùng
                    một khách hàng nằm liền nhau?

  ROW_GROUP_SIZE    Mặc định 122.880 hàng. Một ngày có khoảng bao nhiêu hàng?
                    Nếu cả ngày gói gọn trong MỘT row group thì min/max của
                    row group đó phủ những gì, và còn tác dụng lọc không?

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    # Đếm số row trước khi compact
    n_rows = con.execute(
        f"SELECT count(*) FROM read_parquet('{SRC}/*.parquet')"
    ).fetchone()[0]
    print(f"  số hàng: {n_rows:,}")

    # Compact: partition theo DATE(event_time), order theo customer_name
    # ROW_GROUP_SIZE = 10000: mỗi ngày (~9,500 events) gói gọn trong ~1 row group
    # Partition theo ngày vì query filter theo event_time và cần ~14 thư mục
    con.execute(f"""
        COPY (
            SELECT *
            FROM read_parquet('{SRC}/*.parquet')
            ORDER BY event_date, customer_name
        ) TO '{DST}' (
            FORMAT parquet,
            PARTITION_BY (event_date),
            OVERWRITE_OR_IGNORE,
            ROW_GROUP_SIZE 10000
        )
    """)

    # Kiểm tra không mất hàng
    n_dst = con.execute(
        f"SELECT count(*) FROM read_parquet('{DST}/**/*.parquet')"
    ).fetchone()[0]
    n_dst_files = len(list(DST.glob("**/*.parquet")))
    print(f"  đích  : {DST}  ({n_dst_files:,} file, {n_dst:,} hàng)")
    assert n_rows == n_dst, f"Mất dữ liệu! nguồn={n_rows}, đích={n_dst}"

    return 0


if __name__ == "__main__":
    sys.exit(main())
