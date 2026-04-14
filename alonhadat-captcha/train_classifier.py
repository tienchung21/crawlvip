#!/usr/bin/env python3
"""
Train MobileNetV2 classifier cho 13 loài động vật captcha alonhadat.
Sử dụng transfer learning + data augmentation.
"""

import os
import sys
import json
import copy
import time
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, random_split
from torchvision import transforms, models, datasets

# ============== CONFIG ==============
DATA_DIR = '/home/chungnt/alonhadat-captcha/training_data'
MODEL_PATH = '/home/chungnt/alonhadat-captcha/captcha_model.pth'
LABELS_PATH = '/home/chungnt/alonhadat-captcha/labels.json'

NUM_CLASSES = 13
BATCH_SIZE = 16
NUM_EPOCHS = 30
LEARNING_RATE = 0.001
TRAIN_RATIO = 0.8  # 80% train, 20% val
IMAGE_SIZE = 224    # MobileNetV2 input size
NUM_WORKERS = 0     # 0 for debug, 2-4 for production

# 13 con vật
ANIMAL_NAMES = ['bo', 'chim', 'cho', 'chuot', 'ga', 'heo', 'ho', 'meo', 'ngua', 'tho', 'trau', 'vit', 'voi']

# ============== DATA AUGMENTATION ==============
train_transforms = transforms.Compose([
    transforms.Resize((IMAGE_SIZE, IMAGE_SIZE)),
    transforms.RandomHorizontalFlip(),
    transforms.RandomRotation(15),
    transforms.ColorJitter(brightness=0.3, contrast=0.3, saturation=0.3, hue=0.1),
    transforms.RandomAffine(degrees=0, translate=(0.1, 0.1), scale=(0.85, 1.15)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])

val_transforms = transforms.Compose([
    transforms.Resize((IMAGE_SIZE, IMAGE_SIZE)),
    transforms.ToTensor(),
    transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
])


def create_model(num_classes):
    """Tạo MobileNetV2 với custom classifier layer"""
    model = models.mobilenet_v2(weights=models.MobileNet_V2_Weights.IMAGENET1K_V1)
    
    # Freeze feature extractor layers
    for param in model.features.parameters():
        param.requires_grad = False
    
    # Unfreeze last 3 layers for fine-tuning
    for param in model.features[-3:].parameters():
        param.requires_grad = True
    
    # Replace classifier
    model.classifier = nn.Sequential(
        nn.Dropout(0.3),
        nn.Linear(model.last_channel, 256),
        nn.ReLU(),
        nn.Dropout(0.2),
        nn.Linear(256, num_classes),
    )
    
    return model


def train_model(model, dataloaders, criterion, optimizer, scheduler, num_epochs, device):
    """Training loop với early stopping"""
    best_model_wts = copy.deepcopy(model.state_dict())
    best_acc = 0.0
    patience = 7
    no_improve = 0
    
    for epoch in range(num_epochs):
        print(f'\n📅 Epoch {epoch+1}/{num_epochs}')
        print('-' * 40)
        
        for phase in ['train', 'val']:
            if phase == 'train':
                model.train()
            else:
                model.eval()
            
            running_loss = 0.0
            running_corrects = 0
            total = 0
            
            for inputs, labels in dataloaders[phase]:
                inputs = inputs.to(device)
                labels = labels.to(device)
                
                optimizer.zero_grad()
                
                with torch.set_grad_enabled(phase == 'train'):
                    outputs = model(inputs)
                    _, preds = torch.max(outputs, 1)
                    loss = criterion(outputs, labels)
                    
                    if phase == 'train':
                        loss.backward()
                        optimizer.step()
                
                running_loss += loss.item() * inputs.size(0)
                running_corrects += torch.sum(preds == labels.data).item()
                total += inputs.size(0)
            
            epoch_loss = running_loss / total
            epoch_acc = running_corrects / total
            
            if phase == 'train':
                print(f'   Train - Loss: {epoch_loss:.4f}, Acc: {epoch_acc:.4f} ({running_corrects}/{total})')
            else:
                print(f'   Val   - Loss: {epoch_loss:.4f}, Acc: {epoch_acc:.4f} ({running_corrects}/{total})')
                scheduler.step(epoch_loss)
                
                if epoch_acc > best_acc:
                    best_acc = epoch_acc
                    best_model_wts = copy.deepcopy(model.state_dict())
                    no_improve = 0
                    print(f'   🏆 Best model cập nhật! Acc: {best_acc:.4f}')
                else:
                    no_improve += 1
                    if no_improve >= patience:
                        print(f'\n⏹️  Early stopping sau {patience} epochs không cải thiện')
                        model.load_state_dict(best_model_wts)
                        return model, best_acc
    
    model.load_state_dict(best_model_wts)
    return model, best_acc


def evaluate_per_class(model, dataloader, class_names, device):
    """Đánh giá accuracy từng class"""
    model.eval()
    class_correct = {name: 0 for name in class_names}
    class_total = {name: 0 for name in class_names}
    
    with torch.no_grad():
        for inputs, labels in dataloader:
            inputs = inputs.to(device)
            labels = labels.to(device)
            outputs = model(inputs)
            _, preds = torch.max(outputs, 1)
            
            for i in range(len(labels)):
                label = class_names[labels[i]]
                class_total[label] += 1
                if preds[i] == labels[i]:
                    class_correct[label] += 1
    
    print('\n📊 Accuracy từng loài:')
    for name in class_names:
        if class_total[name] > 0:
            acc = class_correct[name] / class_total[name]
            print(f'   {name:8s}: {acc:.2%} ({class_correct[name]}/{class_total[name]})')
        else:
            print(f'   {name:8s}: N/A (0 mẫu)')


def main():
    print('🐾 ALONHADAT CAPTCHA CLASSIFIER - MobileNetV2')
    print('=' * 50)
    
    # Check data
    if not os.path.exists(DATA_DIR):
        print(f'❌ Không tìm thấy {DATA_DIR}')
        sys.exit(1)
    
    # Device
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f'📱 Device: {device}')
    
    # Load full dataset (dùng val_transforms trước để đọc)
    full_dataset = datasets.ImageFolder(DATA_DIR, transform=val_transforms)
    class_names = full_dataset.classes
    print(f'🏷️  Classes: {class_names}')
    print(f'📦 Tổng mẫu: {len(full_dataset)}')
    
    # Verify classes match expected
    if class_names != sorted(ANIMAL_NAMES):
        print(f'⚠️  Classes không khớp! Expected: {sorted(ANIMAL_NAMES)}, Got: {class_names}')
    
    # Split train/val
    train_size = int(TRAIN_RATIO * len(full_dataset))
    val_size = len(full_dataset) - train_size
    
    # Random split
    generator = torch.Generator().manual_seed(42)
    train_indices, val_indices = random_split(range(len(full_dataset)), [train_size, val_size], generator=generator)
    
    # Create datasets with appropriate transforms
    class TransformedSubset(torch.utils.data.Dataset):
        def __init__(self, dataset, indices, transform):
            self.dataset = dataset
            self.indices = list(indices)
            self.transform = transform
        
        def __len__(self):
            return len(self.indices)
        
        def __getitem__(self, idx):
            img_path, label = self.dataset.samples[self.indices[idx]]
            from PIL import Image
            img = Image.open(img_path).convert('RGB')
            if self.transform:
                img = self.transform(img)
            return img, label
    
    train_dataset = TransformedSubset(full_dataset, train_indices, train_transforms)
    val_dataset = TransformedSubset(full_dataset, val_indices, val_transforms)
    
    print(f'🔀 Train: {len(train_dataset)} | Val: {len(val_dataset)}')
    
    # DataLoaders
    dataloaders = {
        'train': DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True, num_workers=NUM_WORKERS),
        'val': DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False, num_workers=NUM_WORKERS),
    }
    
    # Model
    print('\n🔧 Tạo model MobileNetV2...')
    model = create_model(NUM_CLASSES)
    model = model.to(device)
    
    # Count trainable params
    total_params = sum(p.numel() for p in model.parameters())
    train_params = sum(p.numel() for p in model.parameters() if p.requires_grad)
    print(f'   Tổng params: {total_params:,} | Trainable: {train_params:,}')
    
    # Loss, Optimizer, Scheduler
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(filter(lambda p: p.requires_grad, model.parameters()), 
                          lr=LEARNING_RATE, weight_decay=1e-4)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=3)
    
    # Train
    print('\n🚀 Bắt đầu training...')
    start_time = time.time()
    model, best_acc = train_model(model, dataloaders, criterion, optimizer, scheduler, NUM_EPOCHS, device)
    elapsed = time.time() - start_time
    
    print(f'\n✅ Training hoàn tất!')
    print(f'   ⏱️  Thời gian: {elapsed:.1f}s')
    print(f'   🏆 Best Val Accuracy: {best_acc:.4f}')
    
    # Evaluate per class
    evaluate_per_class(model, dataloaders['val'], class_names, device)
    
    # Save model
    torch.save({
        'model_state_dict': model.state_dict(),
        'class_names': class_names,
        'num_classes': NUM_CLASSES,
        'best_acc': best_acc,
    }, MODEL_PATH)
    print(f'\n💾 Model đã lưu: {MODEL_PATH}')
    
    # Save labels mapping
    labels_map = {i: name for i, name in enumerate(class_names)}
    with open(LABELS_PATH, 'w') as f:
        json.dump(labels_map, f, indent=2)
    print(f'📋 Labels đã lưu: {LABELS_PATH}')


if __name__ == '__main__':
    main()
