"""
Script để tạo icon cho extension
Cần cài: pip install Pillow
"""

try:
    from PIL import Image, ImageDraw, ImageFont
except ImportError:
    print("Cần cài Pillow: pip install Pillow")
    exit(1)

import os

def create_icon(size):
    """Tạo icon với kích thước cho trước"""
    # Tạo image với background xanh lá
    img = Image.new('RGB', (size, size), color='#4CAF50')
    draw = ImageDraw.Draw(img)
    
    # Vẽ icon kính lúp (search icon)
    center = size // 2
    radius = size // 3
    
    # Vẽ vòng tròn
    circle_bbox = [
        center - radius,
        center - radius,
        center + radius,
        center + radius
    ]
    draw.ellipse(circle_bbox, outline='white', width=max(1, size // 8))
    
    # Vẽ tay cầm
    handle_start_x = center + int(radius * 0.7)
    handle_start_y = center + int(radius * 0.7)
    handle_length = size // 4
    handle_end_x = handle_start_x + handle_length
    handle_end_y = handle_start_y + handle_length
    
    draw.line(
        [(handle_start_x, handle_start_y), (handle_end_x, handle_end_y)],
        fill='white',
        width=max(1, size // 8)
    )
    
    return img

def main():
    # Tạo thư mục icons nếu chưa có
    icons_dir = 'icons'
    if not os.path.exists(icons_dir):
        os.makedirs(icons_dir)
    
    # Tạo các icon với kích thước khác nhau
    sizes = [16, 48, 128]
    
    for size in sizes:
        icon = create_icon(size)
        icon_path = os.path.join(icons_dir, f'icon{size}.png')
        icon.save(icon_path)
        print(f'✅ Đã tạo: {icon_path}')
    
    print('\n🎉 Hoàn thành! Các icon đã được tạo trong thư mục icons/')

if __name__ == '__main__':
    main()

