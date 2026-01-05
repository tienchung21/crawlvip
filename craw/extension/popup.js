// Popup script
let isSelecting = false;
let selectedFields = [];

// Load state từ storage
async function loadState() {
  const result = await chrome.storage.local.get(['isSelecting', 'selectedFields']);
  isSelecting = result.isSelecting || false;
  selectedFields = result.selectedFields || [];
  updateUI();
}

// Save state vào storage
async function saveState() {
  await chrome.storage.local.set({
    isSelecting: isSelecting,
    selectedFields: selectedFields
  });
}

// Update UI
function updateUI() {
  const toggleBtn = document.getElementById('toggleBtn');
  const toggleText = document.getElementById('toggleText');
  const statusIndicator = document.getElementById('statusIndicator');
  const statusText = document.getElementById('statusText');
  const fieldsList = document.getElementById('fieldsList');
  const scrapeBtn = document.getElementById('scrapeBtn');
  const exportBtn = document.getElementById('exportBtn');

  // Update toggle button
  if (isSelecting) {
    toggleBtn.classList.add('active');
    toggleText.textContent = 'Tắt chế độ chọn';
    statusIndicator.classList.add('active');
    statusText.textContent = 'Đang bật - Click vào phần tử để chọn';
  } else {
    toggleBtn.classList.remove('active');
    toggleText.textContent = 'Bật chế độ chọn';
    statusIndicator.classList.remove('active');
    statusText.textContent = 'Tắt';
  }

  // Update fields list
  if (selectedFields.length === 0) {
    fieldsList.innerHTML = '<p class="empty-message">Chưa có trường nào được chọn</p>';
  } else {
    fieldsList.innerHTML = selectedFields.map((field, index) => `
      <div class="field-item">
        <div class="field-header">
          <span class="field-name">${field.name || `Trường ${index + 1}`}</span>
          <button class="btn-remove" data-index="${index}">×</button>
        </div>
        <div class="field-info">
          <small>Selector: <code>${field.selector}</code></small>
        </div>
      </div>
    `).join('');

    // Add remove button listeners
    fieldsList.querySelectorAll('.btn-remove').forEach(btn => {
      btn.addEventListener('click', (e) => {
        const index = parseInt(e.target.dataset.index);
        removeField(index);
      });
    });
  }

  // Enable/disable buttons
  const hasFields = selectedFields.length > 0;
  scrapeBtn.disabled = !hasFields;
  exportBtn.disabled = !hasFields;
}

// Toggle selecting mode
async function toggleSelecting() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    // Check if we can inject script (for pages like chrome://, about:, etc)
    if (!tab.url || tab.url.startsWith('chrome://') || tab.url.startsWith('edge://') || tab.url.startsWith('about:')) {
      alert('Extension không hoạt động trên trang này. Vui lòng vào một website bình thường.');
      return;
    }
    
    isSelecting = !isSelecting;
    await saveState();

    // Try to inject content script if needed
    try {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        files: ['content.js']
      });
    } catch (e) {
      // Script might already be injected, that's okay
      console.log('Script injection:', e.message);
    }

    // Wait a bit for script to be ready
    await new Promise(resolve => setTimeout(resolve, 100));

    // Send message to content script
    try {
      const response = await chrome.tabs.sendMessage(tab.id, {
        action: 'toggleSelecting',
        isSelecting: isSelecting
      });
      console.log('Toggle response:', response);
    } catch (error) {
      console.error('Error sending message:', error);
      alert('Lỗi: Không thể kết nối với trang web. Vui lòng reload trang và thử lại.');
      // Revert state
      isSelecting = !isSelecting;
      await saveState();
    }

    updateUI();
  } catch (error) {
    console.error('Toggle error:', error);
    alert('Lỗi: ' + error.message);
  }
}

// Remove field
async function removeField(index) {
  selectedFields.splice(index, 1);
  await saveState();
  updateUI();
  
  // Notify content script
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    chrome.tabs.sendMessage(tab.id, {
      action: 'updateFields',
      fields: selectedFields
    }).catch(err => console.log('Error updating fields:', err));
  } catch (error) {
    console.error('Error:', error);
  }
}

// Clear all fields
async function clearAll() {
  if (confirm('Bạn có chắc muốn xóa tất cả các trường đã chọn?')) {
    selectedFields = [];
    await saveState();
    updateUI();
    
    // Notify content script
    try {
      const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
      chrome.tabs.sendMessage(tab.id, {
        action: 'updateFields',
        fields: []
      }).catch(err => console.log('Error clearing fields:', err));
    } catch (error) {
      console.error('Error:', error);
    }
  }
}

// Scrape data
async function scrapeData() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  const resultDiv = document.getElementById('result');
  
  resultDiv.classList.remove('hidden');
  resultDiv.innerHTML = '<p class="loading">Đang cào dữ liệu...</p>';

  try {
    const response = await chrome.tabs.sendMessage(tab.id, {
      action: 'scrape',
      fields: selectedFields
    });

    if (response.success) {
      const data = response.data;
      resultDiv.innerHTML = `
        <h4>Kết quả:</h4>
        <pre class="result-json">${JSON.stringify(data, null, 2)}</pre>
      `;
    } else {
      resultDiv.innerHTML = `<p class="error">Lỗi: ${response.error}</p>`;
    }
  } catch (error) {
    resultDiv.innerHTML = `<p class="error">Lỗi: ${error.message}</p>`;
  }
}

// Export JSON
async function exportJSON() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    const response = await chrome.tabs.sendMessage(tab.id, {
      action: 'scrape',
      fields: selectedFields
    });
    
    if (response && response.success) {
      const data = {
        url: tab.url,
        scrapedAt: new Date().toISOString(),
        fields: selectedFields,
        data: response.data
      };
      
      const json = JSON.stringify(data, null, 2);
      const blob = new Blob([json], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `scraped_data_${Date.now()}.json`;
      a.click();
      URL.revokeObjectURL(url);
    } else {
      alert('Lỗi: Không thể lấy dữ liệu từ trang web');
    }
  } catch (error) {
    alert('Lỗi khi export: ' + error.message);
  }
}

// Listen for field selection from content script
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'fieldSelected') {
    selectedFields.push(message.field);
    // Save state asynchronously without blocking
    saveState().catch(err => console.error('Error saving state:', err));
    updateUI();
  }
  return true; // Keep channel open for async response
});

// Event listeners
document.addEventListener('DOMContentLoaded', () => {
  loadState();
  
  document.getElementById('toggleBtn').addEventListener('click', toggleSelecting);
  document.getElementById('clearBtn').addEventListener('click', clearAll);
  document.getElementById('scrapeBtn').addEventListener('click', scrapeData);
  document.getElementById('exportBtn').addEventListener('click', exportJSON);
});

