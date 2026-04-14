#!/usr/bin/env python3
"""
Thanh khoan dashboard (Streamlit)

Run:
    cd /home/chungnt/crawlvip
    source venv/bin/activate
    streamlit run craw/auto/thanhkhoan_dashboard.py
"""

import os
from typing import Dict, List

import pymysql
import streamlit as st


def get_db_conf() -> Dict:
    return {
        "host": os.getenv("TK_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TK_DB_PORT", "3306")),
        "user": os.getenv("TK_DB_USER", "root"),
        "password": os.getenv("TK_DB_PASS", ""),
        "database": os.getenv("TK_DB_NAME", "craw_db"),
        "charset": "utf8mb4",
        "cursorclass": pymysql.cursors.DictCursor,
        "autocommit": True,
    }


def fetch_all(sql: str, params: tuple = ()) -> List[Dict]:
    conn = pymysql.connect(**get_db_conf())
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


@st.cache_data(ttl=120)
def load_filter_options():
    period_types = fetch_all(
        "SELECT DISTINCT period_type FROM thanhkhoan_index ORDER BY period_type"
    )
    levels = fetch_all(
        "SELECT DISTINCT liquidity_level FROM thanhkhoan_index "
        "WHERE liquidity_level IS NOT NULL AND liquidity_level <> '' ORDER BY liquidity_level"
    )
    provinces = fetch_all(
        """
        SELECT DISTINCT t.province_id AS id,
               COALESCE(m.new_city_name, CONCAT('Province ', t.province_id)) AS name
        FROM thanhkhoan_index t
        LEFT JOIN transaction_city_merge m
          ON m.new_city_id = t.province_id
         AND m.action_type = 0
        WHERE t.province_id IS NOT NULL
        ORDER BY name
        """
    )
    wards = fetch_all(
        """
        SELECT DISTINCT t.ward_id AS id,
               COALESCE(m.new_city_name, CONCAT('Ward ', t.ward_id)) AS name
        FROM thanhkhoan_index t
        LEFT JOIN transaction_city_merge m
          ON m.new_city_id = t.ward_id
         AND m.action_type = 0
        WHERE t.ward_id IS NOT NULL
        ORDER BY name
        """
    )
    return period_types, levels, provinces, wards


def load_period_values(period_type: str):
    return fetch_all(
        """
        SELECT DISTINCT period_value
        FROM thanhkhoan_index
        WHERE period_type=%s
        ORDER BY period_value DESC
        """,
        (period_type,),
    )


def load_data(
    period_type: str,
    period_value: str,
    scope: str,
    province_id: int,
    ward_id: int,
    level: str,
    median_group: int,
    limit: int,
):
    where = ["t.period_type = %s"]
    params: List = [period_type]

    if period_value:
        where.append("t.period_value = %s")
        params.append(period_value)
    if scope:
        where.append("t.scope = %s")
        params.append(scope)
    if province_id > 0:
        where.append("t.province_id = %s")
        params.append(province_id)
    if ward_id > 0:
        where.append("t.ward_id = %s")
        params.append(ward_id)
    if level:
        where.append("t.liquidity_level = %s")
        params.append(level)
    if median_group > 0:
        where.append("t.median_group = %s")
        params.append(median_group)

    sql = f"""
    SELECT
        t.period_type,
        t.period_value,
        t.scope,
        t.province_id,
        COALESCE(mp.new_city_name, CONCAT('Province ', t.province_id)) AS province_name,
        t.ward_id,
        COALESCE(mw.new_city_name, CONCAT('Ward ', t.ward_id)) AS ward_name,
        t.median_group,
        t.raw_rows,
        t.trimmed_rows,
        t.median_price_m2,
        t.stddev_price_m2,
        t.cv,
        t.vitality_score,
        t.liquidity_level,
        t.median_source,
        t.computed_at
    FROM thanhkhoan_index t
    LEFT JOIN transaction_city_merge mp
      ON mp.new_city_id = t.province_id
     AND mp.action_type = 0
    LEFT JOIN transaction_city_merge mw
      ON mw.new_city_id = t.ward_id
     AND mw.action_type = 0
    WHERE {" AND ".join(where)}
    ORDER BY t.period_value DESC, t.scope, t.province_id, t.ward_id, t.median_group
    LIMIT %s
    """
    params.append(limit)
    return fetch_all(sql, tuple(params))


def load_summary(period_type: str, period_value: str, scope: str):
    where = ["period_type=%s"]
    params: List = [period_type]
    if period_value:
        where.append("period_value=%s")
        params.append(period_value)
    if scope:
        where.append("scope=%s")
        params.append(scope)

    sql = f"""
    SELECT liquidity_level, COUNT(*) AS total
    FROM thanhkhoan_index
    WHERE {" AND ".join(where)}
    GROUP BY liquidity_level
    ORDER BY total DESC
    """
    return fetch_all(sql, tuple(params))


def main():
    st.set_page_config(page_title="Thanh Khoan Dashboard", layout="wide")
    st.title("Dashboard Thanh Khoan BDS")
    st.caption("Nguon du lieu: thanhkhoan_index")

    try:
        period_types, levels, provinces, wards = load_filter_options()
    except Exception as e:
        st.error(f"Khong load duoc du lieu. Loi DB: {e}")
        st.stop()

    if not period_types:
        st.warning("Bang thanhkhoan_index dang rong.")
        st.stop()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        period_type = st.selectbox(
            "Ky tinh",
            options=[x["period_type"] for x in period_types],
            index=0,
        )
    period_values = load_period_values(period_type)
    with col2:
        period_value = st.selectbox(
            "Gia tri ky",
            options=[""] + [x["period_value"] for x in period_values],
            format_func=lambda x: "Tat ca" if x == "" else x,
            index=1 if len(period_values) > 0 else 0,
        )
    with col3:
        scope = st.selectbox("Scope", options=["", "ward", "region"], format_func=lambda x: "Tat ca" if x == "" else x)
    with col4:
        median_group = st.selectbox("Median group", options=[0, 1, 2, 3, 4], format_func=lambda x: "Tat ca" if x == 0 else str(x))

    col5, col6, col7, col8 = st.columns(4)
    with col5:
        province_id = st.selectbox(
            "Tinh",
            options=[0] + [x["id"] for x in provinces],
            format_func=lambda x: "Tat ca" if x == 0 else next((p["name"] for p in provinces if p["id"] == x), str(x)),
        )
    with col6:
        ward_id = st.selectbox(
            "Xa/Phuong",
            options=[0] + [x["id"] for x in wards],
            format_func=lambda x: "Tat ca" if x == 0 else next((w["name"] for w in wards if w["id"] == x), str(x)),
        )
    with col7:
        level = st.selectbox(
            "Muc thanh khoan",
            options=[""] + [x["liquidity_level"] for x in levels],
            format_func=lambda x: "Tat ca" if x == "" else x,
        )
    with col8:
        limit = st.number_input("So dong toi da", min_value=100, max_value=200000, value=5000, step=100)

    rows = load_data(
        period_type=period_type,
        period_value=period_value,
        scope=scope,
        province_id=province_id,
        ward_id=ward_id,
        level=level,
        median_group=median_group,
        limit=int(limit),
    )

    summary = load_summary(period_type=period_type, period_value=period_value, scope=scope)
    c1, c2 = st.columns([1, 3])
    with c1:
        st.metric("Tong ket qua", f"{len(rows):,}")
    with c2:
        if summary:
            st.write("Phan bo theo muc thanh khoan:")
            st.dataframe(summary, use_container_width=True, hide_index=True)

    st.dataframe(rows, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
