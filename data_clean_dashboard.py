import streamlit as st

from craw.database import Database


def _fetch_regions(conn):
    sql = """
        SELECT city_id, city_title_news
        FROM transaction_city_new
        WHERE city_parent_id = 0 AND is_processed = 1 AND is_merged = 0
        ORDER BY city_title_news
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    options = []
    for row in rows:
        if isinstance(row, tuple):
            options.append((row[0], row[1]))
        else:
            options.append((row.get("city_id"), row.get("city_title_news")))
    return options


def _fetch_new_wards(conn, region_id):
    params = []
    where = "WHERE is_processed = 1"
    if region_id is None:
        where += " AND new_city_parent_id <> 0"
    else:
        where += " AND new_city_parent_id = %s"
        params.append(region_id)
    sql = f"""
        SELECT
            city_id,
            COALESCE(new_city_title, city_title_news, city_title) AS new_name
        FROM transaction_city_new
        {where}
        ORDER BY new_name
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    options = []
    for row in rows:
        if isinstance(row, tuple):
            options.append((row[0], row[1]))
        else:
            options.append((row.get("city_id"), row.get("new_name")))
    return options


def _fetch_rows(conn, region_id, ward_id, type_filter, category_filter, month_filter, limit_rows):
    params = []
    extra_filters = []
    if type_filter is not None:
        extra_filters.append("d.type = %s")
        params.append(type_filter)
    if category_filter is not None:
        extra_filters.append("d.category = %s")
        params.append(category_filter)
    if month_filter is not None:
        extra_filters.append("DATE_FORMAT(FROM_UNIXTIME(d.list_time/1000), '%Y-%m') = %s")
        params.append(month_filter)
    extra_sql = ""
    if extra_filters:
        extra_sql = " AND " + " AND ".join(extra_filters)

    if ward_id is not None:
        sql = """
            SELECT
                d.ad_id, d.list_id, d.list_time, d.orig_list_time,
                DATE_FORMAT(FROM_UNIXTIME(d.list_time/1000), '%Y-%m') AS list_month,
                d.region_v2, d.area_v2, d.ward,
                d.street_name, d.street_number, d.unique_street_id,
                d.category, d.size, d.price, d.type, d.time_crawl, d.price_m2_vnd
            FROM transaction_city_mergev2 m
            JOIN nhadat_nhatot n
              ON n.match_type = 'ward' AND n.cf_ward_id = m.old_city_id
            JOIN data_clean d
              ON d.ward = n.nt_ward_id
            WHERE m.new_city_id = %s
        """
        params = [ward_id] + params
        if extra_sql:
            sql += extra_sql
        sql += """
            ORDER BY d.list_time DESC
            LIMIT %s
        """
        params.append(limit_rows)
    elif region_id is not None:
        sql = """
            SELECT
                d.ad_id, d.list_id, d.list_time, d.orig_list_time,
                DATE_FORMAT(FROM_UNIXTIME(d.list_time/1000), '%Y-%m') AS list_month,
                d.region_v2, d.area_v2, d.ward,
                d.street_name, d.street_number, d.unique_street_id,
                d.category, d.size, d.price, d.type, d.time_crawl, d.price_m2_vnd
            FROM transaction_city_new cn
            JOIN transaction_city_mergev2 m
              ON m.new_city_id = cn.city_id
            JOIN nhadat_nhatot n
              ON n.match_type = 'ward' AND n.cf_ward_id = m.old_city_id
            JOIN data_clean d
              ON d.ward = n.nt_ward_id
            WHERE cn.new_city_parent_id = %s
        """
        params = [region_id] + params
        if extra_sql:
            sql += extra_sql
        sql += """
            ORDER BY d.list_time DESC
            LIMIT %s
        """
        params.append(limit_rows)
    else:
        sql = """
            SELECT
                d.ad_id, d.list_id, d.list_time, d.orig_list_time,
                DATE_FORMAT(FROM_UNIXTIME(d.list_time/1000), '%Y-%m') AS list_month,
                d.region_v2, d.area_v2, d.ward,
                d.street_name, d.street_number, d.unique_street_id,
                d.category, d.size, d.price, d.type, d.time_crawl, d.price_m2_vnd
            FROM data_clean d
            WHERE 1=1
        """
        if extra_sql:
            sql += extra_sql
        sql += """
            ORDER BY d.list_time DESC
            LIMIT %s
        """
        params.append(limit_rows)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
    results = []
    for row in rows:
        if isinstance(row, tuple):
            results.append(dict(zip(columns, row)))
        else:
            results.append(row)
    return results


def _count_rows(conn, region_id, ward_id, type_filter, category_filter, month_filter):
    if ward_id is not None:
        sql = """
            SELECT COUNT(*)
            FROM transaction_city_mergev2 m
            JOIN nhadat_nhatot n
              ON n.match_type = 'ward' AND n.cf_ward_id = m.old_city_id
            JOIN data_clean d
              ON d.ward = n.nt_ward_id
            WHERE m.new_city_id = %s
        """
        params = [ward_id]
    elif region_id is not None:
        sql = """
            SELECT COUNT(*)
            FROM transaction_city_new cn
            JOIN transaction_city_mergev2 m
              ON m.new_city_id = cn.city_id
            JOIN nhadat_nhatot n
              ON n.match_type = 'ward' AND n.cf_ward_id = m.old_city_id
            JOIN data_clean d
              ON d.ward = n.nt_ward_id
            WHERE cn.new_city_parent_id = %s
        """
        params = [region_id]
    else:
        sql = "SELECT COUNT(*) FROM data_clean"
        params = []

    if type_filter is not None:
        sql += " AND d.type = %s"
        params.append(type_filter)
    if category_filter is not None:
        sql += " AND d.category = %s"
        params.append(category_filter)
    if month_filter is not None:
        sql += " AND DATE_FORMAT(FROM_UNIXTIME(d.list_time/1000), '%Y-%m') = %s"
        params.append(month_filter)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        total = cur.fetchone()
    if isinstance(total, tuple):
        return total[0]
    return total.get("COUNT(*)", 0)


def _fetch_types(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT type FROM data_clean WHERE type IS NOT NULL ORDER BY type")
        rows = cur.fetchall()
    types = []
    for row in rows:
        if isinstance(row, tuple):
            types.append(row[0])
        else:
            types.append(row.get("type"))
    return types


def _fetch_categories(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT DISTINCT category FROM data_clean WHERE category IS NOT NULL ORDER BY category")
        rows = cur.fetchall()
    cats = []
    for row in rows:
        if isinstance(row, tuple):
            cats.append(row[0])
        else:
            cats.append(row.get("category"))
    return cats


def _fetch_months(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT DATE_FORMAT(FROM_UNIXTIME(list_time/1000), '%Y-%m') AS ym "
            "FROM data_clean WHERE list_time IS NOT NULL ORDER BY ym DESC"
        )
        rows = cur.fetchall()
    months = []
    for row in rows:
        if isinstance(row, tuple):
            months.append(row[0])
        else:
            months.append(row.get("ym"))
    return months


def _build_price_source(region_id, ward_id, type_filter, category_filter, month_filter):
    params = []
    extra_filters = []
    if type_filter is not None:
        extra_filters.append("d.type = %s")
        params.append(type_filter)
    if category_filter is not None:
        extra_filters.append("d.category = %s")
        params.append(category_filter)
    if month_filter is not None:
        extra_filters.append("DATE_FORMAT(FROM_UNIXTIME(d.list_time/1000), '%Y-%m') = %s")
        params.append(month_filter)
    extra_filters.append("d.price_m2_vnd IS NOT NULL")
    extra_sql = " AND " + " AND ".join(extra_filters)

    if ward_id is not None:
        sql = """
            SELECT d.price_m2_vnd
            FROM transaction_city_mergev2 m
            JOIN nhadat_nhatot n
              ON n.match_type = 'ward' AND n.cf_ward_id = m.old_city_id
            JOIN data_clean d
              ON d.ward = n.nt_ward_id
            WHERE m.new_city_id = %s
        """
        params = [ward_id] + params
    elif region_id is not None:
        sql = """
            SELECT d.price_m2_vnd
            FROM transaction_city_new cn
            JOIN transaction_city_mergev2 m
              ON m.new_city_id = cn.city_id
            JOIN nhadat_nhatot n
              ON n.match_type = 'ward' AND n.cf_ward_id = m.old_city_id
            JOIN data_clean d
              ON d.ward = n.nt_ward_id
            WHERE cn.new_city_parent_id = %s
        """
        params = [region_id] + params
    else:
        sql = "SELECT d.price_m2_vnd FROM data_clean d WHERE 1=1"
    sql += extra_sql
    return sql, params


def _calc_price_stats(conn, region_id, ward_id, type_filter, category_filter, month_filter):
    base_sql, params = _build_price_source(region_id, ward_id, type_filter, category_filter, month_filter)
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM ({base_sql}) t", params)
        total = cur.fetchone()
        total = total[0] if isinstance(total, tuple) else list(total.values())[0]
        if total == 0:
            return {"count": 0, "avg": None, "median_trim": None}
        cur.execute(f"SELECT AVG(price_m2_vnd) FROM ({base_sql}) t", params)
        avg_val = cur.fetchone()
        avg_val = avg_val[0] if isinstance(avg_val, tuple) else list(avg_val.values())[0]

        cut = int(total * 0.1)
        trimmed = total - (cut * 2)
        if trimmed <= 0:
            return {"count": total, "avg": avg_val, "median_trim": None}

        if trimmed % 2 == 1:
            mid_offset = cut + (trimmed // 2)
        else:
            mid_offset = cut + (trimmed // 2 - 1)
        cur.execute(
            f"SELECT price_m2_vnd FROM ({base_sql}) t ORDER BY price_m2_vnd LIMIT 1 OFFSET {mid_offset}",
            params,
        )
        v1 = cur.fetchone()
        v1 = v1[0] if isinstance(v1, tuple) else list(v1.values())[0]
        if trimmed % 2 == 1:
            median_val = v1
        else:
            cur.execute(
                f"SELECT price_m2_vnd FROM ({base_sql}) t ORDER BY price_m2_vnd LIMIT 1 OFFSET {mid_offset + 1}",
                params,
            )
            v2 = cur.fetchone()
            v2 = v2[0] if isinstance(v2, tuple) else list(v2.values())[0]
            median_val = (v1 + v2) / 2 if v2 is not None else v1

    return {"count": total, "avg": avg_val, "median_trim": median_val}


def main():
    st.set_page_config(page_title="Data Clean Viewer", layout="wide")
    st.title("Data Clean Viewer")
    st.caption("Hien thi du lieu tu bang data_clean.")

    db = Database(host="localhost", user="root", password="", database="craw_db", port=3306)
    conn = db.get_connection()
    try:
        region_options = _fetch_regions(conn)
        region_name_map = {rid: name for rid, name in region_options}
        region_ids = [None] + [rid for rid, _ in region_options]

        col1, col2, col3, col4, col5, col6 = st.columns([2, 2, 1, 1, 1, 1])
        with col1:
            region_id = st.selectbox(
                "Tinh/Thanh pho moi",
                region_ids,
                format_func=lambda rid: "Tat ca" if rid is None else region_name_map.get(rid, str(rid)),
            )
        with col2:
            ward_options = _fetch_new_wards(conn, region_id)
            ward_name_map = {wid: name for wid, name in ward_options}
            ward_ids = [None] + [wid for wid, _ in ward_options]
            ward_id = st.selectbox(
                "Phuong/xa moi",
                ward_ids,
                format_func=lambda wid: "Tat ca" if wid is None else ward_name_map.get(wid, str(wid)),
            )
            st.caption(f"So xa/phuong moi: {len(ward_options)}")
            if region_id is not None:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM transaction_city_new WHERE new_city_parent_id = %s AND is_processed = 1",
                        (region_id,),
                    )
                    cnt = cur.fetchone()
                if isinstance(cnt, tuple):
                    cnt_val = cnt[0]
                else:
                    cnt_val = list(cnt.values())[0]
                st.caption(f"Debug region_id={region_id} | wards_in_db={cnt_val}")
        with col3:
            limit_rows = st.number_input("So dong hien thi", min_value=10, max_value=10000, value=200, step=10)
        with col4:
            type_options = [None] + _fetch_types(conn)
            type_filter = st.selectbox(
                "Type",
                type_options,
                format_func=lambda val: "Tat ca" if val is None else str(val),
            )
        with col5:
            cat_options = [None] + _fetch_categories(conn)
            category_filter = st.selectbox(
                "Category",
                cat_options,
                format_func=lambda val: "Tat ca" if val is None else str(val),
            )
        with col6:
            month_options = [None] + _fetch_months(conn)
            month_filter = st.selectbox(
                "Thang",
                month_options,
                format_func=lambda val: "Tat ca" if val is None else str(val),
            )

        total_rows = _count_rows(conn, region_id, ward_id, type_filter, category_filter, month_filter)
        st.metric("Tong so dong", total_rows)

        rows = _fetch_rows(conn, region_id, ward_id, type_filter, category_filter, month_filter, int(limit_rows))
        st.dataframe(rows, use_container_width=True, hide_index=True)

        stats = _calc_price_stats(conn, region_id, ward_id, type_filter, category_filter, month_filter)
        st.markdown("### Thong so gia")
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.metric("So mau (price_m2_vnd)", stats["count"])
        with col_b:
            st.metric("Gia trung binh", None if stats["avg"] is None else f"{stats['avg']:.2f}")
        with col_c:
            st.metric("Gia trung vi (cat 10%)", None if stats["median_trim"] is None else f"{stats['median_trim']:.2f}")
        st.caption(
            "Cong thuc: sap xep price_m2_vnd tang dan, loai 10% thap nhat + 10% cao nhat, "
            "N = so mau con lai. Neu N le -> lay phan tu giua; "
            "neu N chan -> (A[N/2 - 1] + A[N/2]) / 2."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
