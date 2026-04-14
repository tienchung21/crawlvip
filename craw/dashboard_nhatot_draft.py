
import streamlit as st
import pandas as pd
from typing import Dict, Any, List
from craw.database import Database

def _fetch_location_map(db: Database, query: str, params=(), id_col=None) -> Dict[int, str]:
    """Helper for fetching simple ID->Name maps (already present in dashboard.py, duplicated here or imported?)"""
    # Ideally should be a shared utility. For now, duplication or pass db/helper is fine.
    # But since this function is small, I will implement it here or assume dashboard passes it?
    # Dashboard uses it locally. Let's include it here for Safety.
    try:
        conn = db.get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        mapping = {}
        for r in rows:
            val_id = r.get(id_col)
            val_name = r.get("name")
            if val_id:
                mapping[val_id] = val_name
        return mapping
    except Exception as e:
        print(f"Error fetching location map: {e}")
        return {}

def render_nhatot_tab():
    st.header("Task 1 - API URL Builder")
    st.markdown("Chon tinh/quan/xa tu bang location_detail va tu dong ghep query.")

    if "db_location" not in st.session_state:
        st.session_state.db_location = Database(
            host="localhost",
            user="root",
            password="",
            database="craw_db",
        )

    base_url = "https://gateway.chotot.com/v1/public/ad-listing?cg=1000"

    try:
        region_map = _fetch_location_map(
            st.session_state.db_location,
            """
            SELECT region_id, name
            FROM location_detail
            WHERE level = 1 AND is_active = 1
            ORDER BY name
            """,
            (),
            "region_id",
        )
    except Exception as exc:
        st.error(f"Khong load duoc location_detail: {exc}")
        region_map = {}

    select_all_regions = st.checkbox("Chon tat ca tinh", value=False, key="task1_select_all_regions")
    excluded_regions = []
    if select_all_regions:
        excluded_regions = st.multiselect(
            "Loai tru tinh",
            options=list(region_map.keys()),
            format_func=lambda x: region_map.get(x, str(x)),
            key="task1_exclude_regions",
        )

    region_options = [None] + list(region_map.keys())
    if st.session_state.get("task1_region_id") not in region_options:
        st.session_state["task1_region_id"] = None
    region_id = st.selectbox(
        "Tinh/TP",
        region_options,
        format_func=lambda x: region_map.get(x, "(Chon tinh)"),
        key="task1_region_id",
    )

    area_map: Dict[int, str] = {}
    if region_id:
        area_map = _fetch_location_map(
            st.session_state.db_location,
            """
            SELECT area_id, name
            FROM location_detail
            WHERE level = 2 AND region_id = %s AND area_id IS NOT NULL AND is_active = 1
            ORDER BY name
            """,
            (region_id,),
            "area_id",
        )
    
    # ... This extraction is tricky because of the huge amount of logic/state.
    # The user didn't ask for a refactor, but for an ADDITION.
    # Moving code might break state keys if not careful.
    # 
    # LET'S PAUSE the extraction strategy.
    # It might be safer to simply INJECT the Mogi crawler at the TOP of Tab 1
    # with an expander "Open Mogi Crawler" (default expanded=False)
    # OR
    # Use the "st.radio" approach to toggle visibility.
    # st.radio("Select Tool", ["NhaTot API", "Mogi Crawler"])
    # If "Mogi", show mogi UI.
    # If "NhaTot", show existing code.
    #
    # BUT "show existing code" still requires indentation if I put it in an `if`.
    #
    # Wait, can I use `st.empty()` or containers?
    # No, python flow is sequential.
    
    st.write("Refactor cancelled. Returning to main file editing strategy.")
