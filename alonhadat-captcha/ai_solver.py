#!/usr/bin/env python3
"""
AI Captcha Solver cho alonhadat.com.vn
Sử dụng MobileNetV2 đã train để tự động nhận diện 13 loài động vật.
"""

import os
import sys
import json
import requests
import numpy as np
from io import BytesIO
from PIL import Image
import torch
import torch.nn as nn
from torchvision import transforms, models

# ============== CONFIG ==============
MODEL_PATH = '/home/chungnt/alonhadat-captcha/captcha_model.pth'
LABELS_PATH = '/home/chungnt/alonhadat-captcha/labels.json'
CAPTCHA_URL = 'https://alonhadat.com.vn/ImageCaptcha.ashx?v=3'
IMAGE_SIZE = 224

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://alonhadat.com.vn/'
}

# Transform cho inference (giống val_transforms lúc train)
inference_transform = transforms.Compose([
    transforms.Resize((IMAGE_SIZE, IMAGE_SIZE)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])


class CaptchaSolver:
    def __init__(self, model_path=MODEL_PATH, labels_path=LABELS_PATH):
        self.device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
        
        # Load labels
        with open(labels_path) as f:
            labels_map = json.load(f)
        self.class_names = [labels_map[str(i)] for i in range(len(labels_map))]
        
        # Load model
        self.model = self._load_model(model_path)
        self.model.eval()
        print(f'✅ Model loaded | {len(self.class_names)} classes | Device: {self.device}')
    
    def _load_model(self, model_path):
        """Load trained model"""
        model = models.mobilenet_v2(weights=None)
        model.classifier = nn.Sequential(
            nn.Dropout(0.3),
            nn.Linear(model.last_channel, 256),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(256, len(self.class_names)),
        )
        
        checkpoint = torch.load(model_path, map_location=self.device, weights_only=True)
        model.load_state_dict(checkpoint['model_state_dict'])
        model = model.to(self.device)
        return model
    
    def cut_cells(self, img):
        """Cắt captcha thành 4 cells"""
        w, h = img.size
        cell_w = w // 4
        cells = []
        for i in range(4):
            x1 = i * cell_w
            x2 = (i + 1) * cell_w
            border = 3
            cell = img.crop((x1 + border, border, x2 - border, h - border))
            cells.append(cell)
        return cells
    
    def detect_border_color(self, img, cell_idx):
        """Phát hiện màu viền (xanh dương hay vàng) của cell"""
        w, h = img.size
        cell_w = w // 4
        x1 = cell_idx * cell_w
        x2 = (cell_idx + 1) * cell_w
        
        # Lấy pixels viền trên + dưới + trái + phải
        pixels = np.array(img)
        border_pixels = []
        
        # Viền trái
        for y in range(h):
            for x in range(x1, min(x1 + 3, x2)):
                border_pixels.append(pixels[y][x])
        
        # Viền phải
        for y in range(h):
            for x in range(max(x2 - 3, x1), x2):
                border_pixels.append(pixels[y][x])
        
        # Viền trên
        for x in range(x1, x2):
            for y in range(min(3, h)):
                border_pixels.append(pixels[y][x])
        
        # Viền dưới
        for x in range(x1, x2):
            for y in range(max(h - 3, 0), h):
                border_pixels.append(pixels[y][x])
        
        border_pixels = np.array(border_pixels)
        avg_r = np.mean(border_pixels[:, 0])
        avg_g = np.mean(border_pixels[:, 1])
        avg_b = np.mean(border_pixels[:, 2])
        
        # Xanh dương: R < 100, B > 150
        if avg_b > 150 and avg_r < 100:
            return 'blue'
        else:
            return 'yellow'
    
    def predict_cell(self, cell_img):
        """Dự đoán loài động vật từ cell image"""
        img = cell_img.convert('RGB')
        tensor = inference_transform(img).unsqueeze(0).to(self.device)
        
        with torch.no_grad():
            outputs = self.model(tensor)
            probabilities = torch.softmax(outputs, dim=1)
            confidence, predicted = torch.max(probabilities, 1)
        
        pred_class = self.class_names[predicted.item()]
        conf = confidence.item()
        return pred_class, conf
    
    def solve(self, img_bytes=None, img_path=None):
        """
        Giải captcha hoàn chỉnh.
        Returns: (answer_string, details_dict)
        """
        if img_bytes:
            img = Image.open(BytesIO(img_bytes)).convert('RGB')
        elif img_path:
            img = Image.open(img_path).convert('RGB')
        else:
            raise ValueError("Cần img_bytes hoặc img_path")
        
        cells = self.cut_cells(img)
        
        results = []
        for i, cell in enumerate(cells):
            animal, confidence = self.predict_cell(cell)
            color = self.detect_border_color(img, i)
            results.append({
                'cell': i + 1,
                'animal': animal,
                'confidence': confidence,
                'border': color,
            })
        
        # Đáp án = 3 con KHÔNG có viền xanh (viền vàng)
        answer_parts = [r['animal'] for r in results if r['border'] != 'blue']
        answer = ''.join(answer_parts)
        
        return answer, results
    
    def solve_from_url(self, session=None):
        """Download và giải captcha từ URL"""
        if session is None:
            session = requests.Session()
        
        resp = session.get(CAPTCHA_URL, headers=HEADERS)
        if resp.status_code != 200:
            raise Exception(f"Download failed: {resp.status_code}")
        
        answer, results = self.solve(img_bytes=resp.content)
        return answer, results, resp.content, session


def demo_mode():
    """Demo: giải 10 captcha liên tiếp"""
    solver = CaptchaSolver()
    session = requests.Session()
    
    correct = 0
    total = 10
    
    print(f'\n🎮 Demo: Giải {total} captcha...\n')
    
    for i in range(total):
        try:
            answer, results, img_bytes, session = solver.solve_from_url(session)
            
            print(f'--- Captcha #{i+1} ---')
            for r in results:
                color_emoji = '🔵' if r['border'] == 'blue' else '🟡'
                print(f"  Ô {r['cell']}: {color_emoji} {r['animal']:8s} ({r['confidence']:.1%})")
            print(f'  🎯 Đáp án: {answer}')
            
            # Lưu ảnh
            save_path = f'/home/chungnt/alonhadat-captcha/demo/demo_{i+1:03d}.png'
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, 'wb') as f:
                f.write(img_bytes)
            print()
            
        except Exception as e:
            print(f'  ❌ Lỗi: {e}\n')
    
    print(f'✅ Demo hoàn tất!')


def test_single(img_path):
    """Test với 1 ảnh có sẵn"""
    solver = CaptchaSolver()
    answer, results = solver.solve(img_path=img_path)
    
    print(f'\n📷 Test ảnh: {img_path}')
    for r in results:
        color_emoji = '🔵' if r['border'] == 'blue' else '🟡'
        print(f"  Ô {r['cell']}: {color_emoji} {r['animal']:8s} ({r['confidence']:.1%})")
    print(f'🎯 Đáp án: {answer}')


if __name__ == '__main__':
    if len(sys.argv) > 1:
        if sys.argv[1] == 'demo':
            demo_mode()
        elif os.path.exists(sys.argv[1]):
            test_single(sys.argv[1])
        else:
            print(f'Không tìm thấy: {sys.argv[1]}')
    else:
        print('Usage:')
        print('  python3 ai_solver.py demo         # Giải 10 captcha online')
        print('  python3 ai_solver.py <image.png>   # Test 1 ảnh')
