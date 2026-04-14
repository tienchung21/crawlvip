import sys
import os
import json
import requests
from decimal import Decimal

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'craw'))
from database import Database

AUTH_TOKEN = "$2y$10$0f/Frpwde3r0.th2lxB3Nuq7dGgZUhPMe4aoAC9Toz0how..g1rJ6"
AUTH_SECRET = "8aHAzSUUJw"
API_URL = "https://api.cafeland.vn/api/website-service/api/app-sync/reals/post-data/"
STATIC_BASE_URL = "https://static2.cafeland.vn"

def json_serial(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def test_upload(listing_id):
    db = Database()
    conn = db.get_connection()
    cursor = conn.cursor()
    
    print(f"Fetching listing {listing_id}...")
    cursor.execute("SELECT * FROM data_full WHERE id = %s", (listing_id,))
    listing = cursor.fetchone()
    
    if not listing:
        print("Listing not found")
        return

    detail_id = listing.get('id_img') or listing.get('id')
    
    # Get images
    cursor.execute("""
        SELECT DISTINCT ftp_path FROM scraped_detail_images 
        WHERE detail_id = %s AND status = 'UPLOADED' AND ftp_path IS NOT NULL
        ORDER BY idx
    """, (detail_id,))
    img_rows = cursor.fetchall()
    
    images = []
    for r in img_rows:
        path = r['ftp_path'] if isinstance(r, dict) else r[0]
        if path and path.startswith('http'):
            images.append(path)
        elif path:
            images.append(f"{STATIC_BASE_URL}{path}")
    
    print(f"Found {len(images)} images.")
    
    # Detail must be > 50 chars
    detail = listing.get('description') or ''
    if len(detail) < 50:
        detail = f"{detail} - {listing.get('title')} - Liên hệ để biết thêm chi tiết."
    if len(detail) < 50:
        detail += " " + ("." * (50 - len(detail)))

    # Category and Type from DB
    cat_id = listing.get('cat_id') or 1
    type_id = listing.get('type_id') or 1

    # Legal status to stratum_id mapping
    LEGAL_MAP = {
        'sổ hồng': 1, 'sổ đỏ': 1, 'đã có sổ': 1,
        'hợp đồng mua bán': 2, 'hđmb': 2,
        'đang chờ sổ': 3,
        'giấy tờ hợp lệ': 4,
    }
    legal = listing.get('legal_status') or ''
    stratum_id = LEGAL_MAP.get(legal.lower().strip(), None)

    # Build payload from REAL data
    payload_data = {
        "cat_id": cat_id,
        "type_id": type_id,
        "city_id": listing.get('province_id') or 1,
        "district_id": listing.get('district_id') or 0,
        "wards_id": listing.get('ward_id') or 0,
        "txtOtherWards": None,
        "wards_name": None,
        "number_house": "",
        "street_house": 0,
        "street_name": None,
        "txtOtherStreet": None,
        "project_id": 0,
        "txtProjectOther": None,
        "project_name": None,
        "sl_location": [str(listing.get('lat') or 10.78), str(listing.get('long') or 106.70)],
        "title": listing.get('title'),
        "detail": detail,
        "area_used": listing.get('area'),
        "area_home_1": float(listing.get('width')) if listing.get('width') else None,
        "area_home_2": float(listing.get('length')) if listing.get('length') else None,
        "price": int(listing.get('price')) if listing.get('price') else None,
        "currency": "vnd",
        "unit_area": "m2",
        "stratum_id": stratum_id,
        "road_home": None,
        "way_home": None,
        "storey": listing.get('floors') or 0,
        "sitting_room": listing.get('living_rooms') or 0,
        "bathroom": listing.get('bathrooms') or 0,
        "rooms": listing.get('bedrooms') or 0,
        "other_room": 0,
        "list_feature_1": None, "list_feature_2": None, "list_feature_3": None,
        "list_feature_4": None, "list_feature_5": None, "list_feature_6": None,
        "list_feature_7": None, "list_feature_8": None, "list_feature_9": None,
        "list_feature_other": None,
        "sl_avatar": images[0] if images else "",
        "sl_avatar_360": None,
        "sendupload": images if images else [],
        "sendupload_phap_ly": None,
        "show_pic_thumb_phap_ly": 0,
        "video_youtube": "",
        "contact_name": listing.get('broker_name') or "Minh Hoang",
        "contact_mobile": listing.get('phone') or "0903123456",
        "location_extra1": None, "location_extra2": None,
        "data_sgd": None, "phan_tram_chia_se": None, "thoi_gian_chia_se": None
    }

    print(f"\nPayload: city_id={payload_data['city_id']}, ward_id={payload_data['wards_id']}")
    print(f"Title: {payload_data['title'][:50]}...")
    print(f"Detail length: {len(payload_data['detail'])} chars")
    
    print(f"\nPayload sent to API:\n{json.dumps(payload_data, default=json_serial, indent=2, ensure_ascii=False)}")
    
    print("\nSending to API...")
    headers = {'token': AUTH_TOKEN, 'secret': AUTH_SECRET}
    post_body = {'data_form': json.dumps(payload_data, default=json_serial)}
    
    response = requests.post(API_URL, data=post_body, headers=headers, timeout=30)
    
    print(f"\n=== API RESPONSE ===")
    print(f"Status: {response.status_code}")
    print(f"Body: {response.text}")
    
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_upload(int(sys.argv[1]))
    else:
        print("Usage: python3 test_api_response.py <listing_id>")
