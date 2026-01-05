"""
Streamlit Web Interface cho Batdongsan.com.vn Crawler
"""

import streamlit as st
import asyncio
import json
import os
import sys
from datetime import datetime
from extract_batdongsan import extract_batdongsan

# Fix asyncio for Windows
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

st.set_page_config(
    page_title="BĐS Crawler",
    page_icon="🏠",
    layout="wide"
)

st.title("🏠 Batdongsan.com.vn Crawler")

# Sidebar config
with st.sidebar:
    st.header("⚙️ Cấu hình")
    
    # Decrypt settings
    decrypt_enabled = st.checkbox("Giải mã số điện thoại", value=True, 
                                   help="Cần cookies hợp lệ trong decrypt_config.py")
    
    use_ai = st.checkbox("Dùng AI extract", value=False,
                         help="Tốn thời gian hơn nhưng chính xác hơn")
    
    st.divider()
    
    st.subheader("📝 Hướng dẫn")
    st.markdown("""
    1. Paste URL từ batdongsan.com.vn
    2. Click **Crawl**
    3. Đợi kết quả (8-15s)
    4. Xem JSON hoặc download
    
    **Giải mã SĐT:**
    - Cần paste cookies vào `decrypt_config.py`
    - Bật `DECRYPT_ENABLED = True`
    """)

# Main area
url_input = st.text_input(
    "🔗 URL tin đăng",
    placeholder="https://batdongsan.com.vn/ban-shophouse-...",
    help="Paste link tin đăng từ batdongsan.com.vn"
)

col1, col2, col3 = st.columns([1, 1, 4])

with col1:
    crawl_btn = st.button("🚀 Crawl", type="primary", use_container_width=True)

with col2:
    if st.button("🗑️ Clear", use_container_width=True):
        st.rerun()

# Results area
if crawl_btn and url_input:
    if not url_input.startswith("https://batdongsan.com.vn"):
        st.error("❌ URL không hợp lệ! Phải là link từ batdongsan.com.vn")
    else:
        with st.spinner("⏳ Đang crawl... (8-15 giây)"):
            try:
                # Run async extract
                result = asyncio.run(extract_batdongsan(url_input, use_ai=use_ai))
                
                if result.get('success'):
                    data = result.get('data', {})
                    
                    st.success("✅ Crawl thành công!")
                    
                    # Display key info
                    st.markdown(f"**📍 Địa chỉ:** {data.get('dia_chi', 'N/A')}")
                    
                    # Tọa độ
                    toa_do = data.get('toa_do', {})
                    if toa_do.get('lat') and toa_do.get('lng'):
                        col_map1, col_map2 = st.columns([3, 1])
                        with col_map1:
                            st.write(f"🗺️ Tọa độ: `{toa_do['lat']}, {toa_do['lng']}`")
                        with col_map2:
                            map_url = f"https://www.google.com/maps?q={toa_do['lat']},{toa_do['lng']}"
                            st.markdown(f"[📍 Xem bản đồ]({map_url})")
                    
                    st.divider()
                    
                    col_a, col_b, col_c = st.columns(3)
                    
                    with col_a:
                        dac_diem = data.get('dac_diem', {})
                        st.metric("💰 Giá", dac_diem.get('khoang_gia', 'N/A'))
                    
                    with col_b:
                        st.metric("📐 Diện tích", dac_diem.get('dien_tich', 'N/A'))
                    
                    with col_c:
                        st.metric("🏘️ Loại hình", dac_diem.get('loai_hinh', 'N/A'))
                    
                    # Tabs for different views
                    tab1, tab2, tab3 = st.tabs(["📊 Thông tin", "👤 Môi giới", "📄 JSON"])
                    
                    with tab1:
                        st.subheader(data.get('title', 'N/A'))
                        
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("**🏢 Dự án:**")
                            du_an = data.get('du_an', {})
                            st.write(du_an.get('ten', 'N/A'))
                            if du_an.get('link'):
                                st.markdown(f"[🔗 Xem dự án]({du_an['link']})")
                            
                            st.markdown("**📅 Ngày đăng:**")
                            st.write(data.get('ngay_dang', 'N/A'))
                            
                            st.markdown("**⏰ Hết hạn:**")
                            st.write(data.get('ngay_het_han', 'N/A'))
                            
                            st.markdown("**📌 Loại tin:**")
                            st.write(data.get('loai_tin', 'N/A'))
                        
                        with col2:
                            st.markdown("**🏠 Đặc điểm:**")
                            for key, val in dac_diem.items():
                                if val:
                                    st.write(f"• **{key.replace('_', ' ').title()}**: {val}")
                        
                        st.markdown("**📝 Mô tả:**")
                        st.write(data.get('mo_ta', 'N/A'))
                    
                    with tab2:
                        moi_gioi = data.get('moi_gioi', {})
                        
                        col_mg1, col_mg2 = st.columns([1, 2])
                        
                        with col_mg1:
                            if moi_gioi.get('link_hinh'):
                                st.image(moi_gioi['link_hinh'], width=200)
                        
                        with col_mg2:
                            st.markdown(f"### {moi_gioi.get('ten', 'N/A')}")
                            
                            # Phone numbers
                            if moi_gioi.get('so_dien_thoai_giai_ma'):
                                st.success(f"📞 **SĐT đã giải mã:** `{moi_gioi['so_dien_thoai_giai_ma']}`")
                            elif moi_gioi.get('so_dien_thoai'):
                                st.info(f"📞 **SĐT:** `{moi_gioi['so_dien_thoai']}`")
                            
                            if moi_gioi.get('so_dien_thoai_ma_hoa'):
                                with st.expander("🔐 Số mã hóa"):
                                    st.code(moi_gioi['so_dien_thoai_ma_hoa'])
                    
                    with tab3:
                        st.json(data, expanded=False)
                        
                        # Download button
                        json_str = json.dumps(data, ensure_ascii=False, indent=2)
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = f"batdongsan_{timestamp}.json"
                        
                        st.download_button(
                            label="💾 Download JSON",
                            data=json_str,
                            file_name=filename,
                            mime="application/json"
                        )
                
                else:
                    st.error(f"❌ Lỗi: {result.get('error', 'Unknown error')}")
            
            except Exception as e:
                st.error(f"❌ Exception: {str(e)}")
                st.exception(e)

# Footer
st.divider()
st.caption("🏠 Batdongsan.com.vn Crawler | Made with Streamlit")
