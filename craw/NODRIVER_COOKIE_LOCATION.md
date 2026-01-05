# Vị trí lưu Cookie của Nodriver

## Tổng quan

Nodriver (undetected-chromedriver) sử dụng Chrome browser thực tế, nên cookie được lưu trong **User Data Directory** của Chrome, giống như khi bạn sử dụng Chrome bình thường.

## Vị trí mặc định

### Windows
```
C:\Users\<USERNAME>\AppData\Local\Google\Chrome\User Data
```

Ví dụ:
```
C:\Users\chungnt\AppData\Local\Google\Chrome\User Data
```

### Linux
```
~/.config/google-chrome
```

### macOS
```
~/Library/Application Support/Google/Chrome
```

## Cấu trúc thư mục Cookie

Cookie được lưu trong profile của Chrome. **LƯU Ý**: Cookie là **FILE**, không phải folder!

```
User Data/
├── Default/                    # Profile mặc định
│   ├── Cookies                 # FILE chứa cookie (SQLite database) - KHÔNG PHẢI FOLDER
│   ├── Cookies-journal          # FILE journal của SQLite
│   ├── Local Storage/          # Folder chứa Local Storage data
│   ├── Session Storage/        # Folder chứa Session Storage data
│   └── ...
├── Profile 1/                  # Profile thứ 2 (nếu có)
│   ├── Cookies                 # FILE (không phải folder)
│   └── ...
└── ...
```

## File Cookies

- **Tên file**: `Cookies` (không có extension, là file SQLite)
- **Định dạng**: SQLite database
- **Vị trí**: `User Data/Default/Cookies` (hoặc `User Data/Profile X/Cookies` nếu dùng profile khác)
- **Lưu ý**: Đây là **FILE**, không phải folder. 

### Nếu không thấy file `Cookies`:

1. **File có thể chưa được tạo**: Chrome chỉ tạo file `Cookies` khi có cookie được lưu. Nếu bạn chưa từng truy cập website nào hoặc đã xóa tất cả cookie, file này có thể không tồn tại.

2. **File bị ẩn**: File có thể bị ẩn (hidden). Trong Windows, bật "Show hidden files" trong File Explorer.

3. **File bị khóa**: Khi Chrome/nodriver đang chạy, file `Cookies` có thể bị khóa và không hiển thị hoặc không thể truy cập.

4. **Cookie được lưu trong profile khác**: Nếu bạn dùng profile khác (không phải Default), cookie sẽ ở `User Data/Profile X/Cookies`.

5. **Extension Cookies**: Chrome cũng lưu cookie của extension trong file `Extension Cookies` (riêng biệt với `Cookies`).

### Các file cookie liên quan:

- `Cookies` - Cookie của các website thông thường
- `Extension Cookies` - Cookie của Chrome extensions
- `Cookies-journal` - Journal file của SQLite (tự động tạo khi database được sử dụng)

## Cách chỉ định User Data Directory tùy chỉnh

Nếu muốn nodriver sử dụng một thư mục user data riêng (để tách biệt với Chrome thông thường), bạn có thể thêm argument `--user-data-dir`:

```python
import nodriver as uc

# Tạo thư mục user data riêng
import os
user_data_dir = os.path.join(os.getcwd(), "nodriver_profile")

# Khởi động nodriver với user data directory tùy chỉnh
browser = await uc.start(
    headless=False,
    browser_args=[
        f"--user-data-dir={user_data_dir}",
        "--blink-settings=imagesEnabled=false",
        "--disable-images",
        "--mute-audio",
    ]
)
```

Với cách này, cookie sẽ được lưu trong:
```
<nodriver_profile>/
└── Default/
    └── Cookies
```

## Đọc Cookie từ file

Cookie được lưu trong SQLite database. Bạn có thể đọc bằng:

```python
import sqlite3
import os

# Đường dẫn đến file Cookies
cookies_path = os.path.join(
    os.path.expanduser("~"),
    "AppData", "Local", "Google", "Chrome", "User Data", "Default", "Cookies"
)

# Kết nối database
conn = sqlite3.connect(cookies_path)
cursor = conn.cursor()

# Đọc cookie
cursor.execute("SELECT name, value, host_key, path, expires_utc FROM cookies")
cookies = cursor.fetchall()

for cookie in cookies:
    print(f"Name: {cookie[0]}, Value: {cookie[1]}, Domain: {cookie[2]}")

conn.close()
```

## Lưu ý

1. **File Cookies bị khóa**: Khi Chrome/nodriver đang chạy, file `Cookies` có thể bị khóa và không thể đọc trực tiếp.

2. **Profile mặc định**: Nếu không chỉ định `--user-data-dir`, nodriver sẽ sử dụng profile mặc định của Chrome, có thể gây xung đột nếu Chrome đang chạy.

3. **Cookie persistence**: Cookie sẽ tự động được lưu và tải lại khi khởi động lại nodriver với cùng user data directory.

4. **Bảo mật**: File `Cookies` chứa thông tin nhạy cảm (session tokens, authentication cookies), cần được bảo vệ cẩn thận.

## Kiểm tra vị trí User Data Directory hiện tại

Để kiểm tra nodriver đang sử dụng user data directory nào, bạn có thể:

1. Mở Chrome với nodriver
2. Vào `chrome://version/`
3. Xem dòng "Profile Path" - đó chính là user data directory đang được sử dụng

