// Side panel script
let isSelecting = false;
let selectedFields = [];
let selectorTypePreference = 'xpath'; // 'css' or 'xpath'
let currentMode = 'detail'; // 'detail' or 'listing'
let listingData = {
  itemLinkSelector: '',
  nextPageSelector: '',
  url: ''
};

// Domain-specific defaults for listing mode (can extend for other sites)
// Note: These are just initial suggestions. The extension will generate better selectors
// based on the actual clicked elements using the generic icon detection logic.
const DOMAIN_DEFAULTS = [
  {
    match: (url) => url.includes('batdongsan.com.vn'),
    item: 'a.js__product-link-for-product-id[href]',
    // Generic selector - extension will auto-detect icon and parent <a> when user clicks
    next: '' // Leave empty to let extension generate from user click
  },
  {
    match: (url) => url.includes('nhadat.cafeland.vn'),
    item: 'a.realTitle[href]',
    next: '.pagination-all nav ul li a[href*="/page-"]'
  }
];

// Apply domain defaults so users don't have to hand-edit JSON templates
function applyListingDefaults(tabUrl) {
  if (!tabUrl) return;
  for (const def of DOMAIN_DEFAULTS) {
    if (def.match(tabUrl)) {
      // Chỉ apply default nếu selector chưa được user chọn
      // Nếu selector bắt đầu bằng dấu chấm (class selector), nghĩa là user đã chọn, không override
      if (!listingData.itemLinkSelector || 
          (!listingData.itemLinkSelector.includes('href') && !listingData.itemLinkSelector.startsWith('.'))) {
        listingData.itemLinkSelector = def.item;
      }
      if (!listingData.nextPageSelector || listingData.nextPageSelector.trim() === '') {
        listingData.nextPageSelector = def.next;
      }
      break;
    }
  }
}

// Load state từ storage và sync với content script
async function loadState() {
  const result = await chrome.storage.local.get(['isSelecting', 'selectedFields', 'selectorTypePreference', 'currentMode', 'listingData']);
  selectedFields = result.selectedFields || [];
  selectorTypePreference = result.selectorTypePreference || 'xpath';
  currentMode = result.currentMode || 'detail';
  listingData = result.listingData || {
    itemLinkSelector: '',
    nextPageSelector: '',
    url: ''
  };
  
  // Reset isSelecting to false when loading - user needs to manually enable
  // This prevents showing "Tắt chế độ" when content script is not ready
  isSelecting = false;
  
  // Update selector type UI
  const cssRadio = document.getElementById('selectorTypeCss');
  const xpathRadio = document.getElementById('selectorTypeXpath');
  if (cssRadio && xpathRadio) {
    if (selectorTypePreference === 'css') {
      cssRadio.checked = true;
    } else {
      xpathRadio.checked = true;
    }
  }
  
  // Try to sync with content script to get actual state
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    // Only try to sync if tab is a valid web page
    if (tab && tab.url && !tab.url.startsWith('chrome://') && 
        !tab.url.startsWith('edge://') && !tab.url.startsWith('about:')) {
      
      // Apply domain defaults for listing mode
      applyListingDefaults(tab.url);
      
      // Try to inject content script if needed
      try {
        await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          files: ['content.js']
        });
        // Wait a bit for script to be ready
        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (e) {
        // Script might already be injected, that's okay
        console.log('Script injection:', e.message);
      }
      
      // Try to get actual state from content script
      try {
        const response = await chrome.tabs.sendMessage(tab.id, {
          action: 'getState'
        });
        
        if (response && typeof response.isSelecting === 'boolean') {
          isSelecting = response.isSelecting;
        }
      } catch (e) {
        // Content script not ready or not responding - keep isSelecting as false
        console.log('Could not get state from content script:', e.message);
        isSelecting = false;
      }
    }
  } catch (error) {
    console.log('Error syncing state:', error);
    isSelecting = false;
  }
  
  // Save the synced state
  await saveState();
  updateUI();
  
  // Only switch mode and update listing UI if elements exist
  try {
    updateListingUI();
    switchMode(currentMode);
  } catch (e) {
    console.log('Error in switchMode/updateListingUI:', e);
  }
  
  // Send selector type preference and mode to content script
  await sendSelectorTypeToContent();
  await sendModeToContent();
}

// Listen for messages from content script
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'listingSelectionComplete') {
    try {
      const { selectionType, selector, matchedCount } = message;
      
      if (selectionType === 'itemLink') {
        const itemLinkInput = document.getElementById('itemLinkSelector');
        const itemLinkBtn = document.getElementById('selectItemLinkBtn');
        const resetBtn = document.getElementById('resetItemLinkBtn');
        const statusDiv = document.getElementById('itemLinkStatus');
        const statusText = document.getElementById('itemLinkStatusText');
        const previewDiv = document.getElementById('itemLinkPreview');
        const previewUrlsDiv = document.getElementById('itemLinkPreviewUrls');
        
        // Check if this is first click or second click (completion)
        if (message.step === 'first') {
          // First click - show status and wait for second
          if (statusDiv && statusText) {
            statusDiv.style.display = 'block';
            statusDiv.style.background = '#e3f2fd';
            statusText.style.color = '#1976d2';
            statusText.textContent = `✅ Đã chọn Item 1. Bây giờ click vào Item 2 (link sản phẩm tương tự khác)...`;
          }
          if (itemLinkBtn) {
            itemLinkBtn.textContent = '⏳ Đang chọn Item 2...';
            itemLinkBtn.disabled = true;
          }
          if (resetBtn) {
            resetBtn.style.display = 'inline-block';
          }
        } else if (message.step === 'complete') {
          // Second click - show final result
          listingData.itemLinkSelector = selector;
          
          if (itemLinkInput) itemLinkInput.value = selector;
          if (itemLinkBtn) {
            itemLinkBtn.textContent = '🎯 Click Item 1';
            itemLinkBtn.disabled = false;
          }
          if (resetBtn) {
            resetBtn.style.display = 'none';
          }
          
          // Show preview URLs
          const previewUrl = message.previewUrl || '';
          const previewUrls = message.previewUrls || [];
          
          // Display preview in sidepanel
          if (previewDiv && previewUrlsDiv) {
            if (previewUrls.length > 0 || previewUrl) {
              let previewHTML = '';
              
              if (previewUrl) {
                previewHTML += `<div style="margin-bottom: 8px;"><strong>📎 URL đã chọn:</strong><br><a href="${previewUrl}" target="_blank" style="color: #1976d2; word-break: break-all;">${previewUrl}</a></div>`;
              }
              
              if (previewUrls.length > 0) {
                previewHTML += `<div style="margin-top: 8px;"><strong>📋 Preview URLs (${Math.min(previewUrls.length, 5)}/${matchedCount}):</strong></div>`;
                previewHTML += '<div style="margin-top: 5px;">';
                previewUrls.forEach((url, idx) => {
                  previewHTML += `<div style="margin-bottom: 4px; padding: 4px; background: white; border-radius: 3px;">
                    <span style="color: #666;">${idx + 1}.</span> 
                    <a href="${url}" target="_blank" style="color: #1976d2; word-break: break-all; font-size: 11px;">${url}</a>
                  </div>`;
                });
                if (matchedCount > previewUrls.length) {
                  previewHTML += `<div style="color: #666; font-style: italic; margin-top: 5px;">... và ${matchedCount - previewUrls.length} URLs khác</div>`;
                }
                previewHTML += '</div>';
              }
              
              previewUrlsDiv.innerHTML = previewHTML;
              previewDiv.style.display = 'block';
            } else {
              previewDiv.style.display = 'none';
            }
          }
          
          // Update status
          if (statusDiv && statusText) {
            statusDiv.style.background = '#e8f5e9';
            statusText.style.color = '#2e7d32';
            statusText.textContent = `✅ Hoàn thành! Tìm thấy ${matchedCount} phần tử với selector: ${selector}`;
          }
          
          // Show alert with summary
          let alertMessage = `✅ Đã tìm thấy pattern chung!\n\n`;
          alertMessage += `Selector: ${selector}\n\n`;
          alertMessage += `Tìm thấy ${matchedCount} phần tử tương tự trên trang.`;
          
          if (previewUrl) {
            alertMessage += `\n\n📎 URL mẫu:\n${previewUrl}`;
          }
          
          alert(alertMessage);
        } else if (message.step === 'error') {
          // Error - no common pattern found
          if (statusDiv && statusText) {
            statusDiv.style.background = '#ffebee';
            statusText.style.color = '#c62828';
            statusText.textContent = `❌ ${message.error || 'Không tìm thấy pattern chung. Vui lòng chọn 2 link sản phẩm tương tự.'}`;
          }
          if (itemLinkBtn) {
            itemLinkBtn.textContent = '🎯 Click Item 1';
            itemLinkBtn.disabled = false;
          }
          if (resetBtn) {
            resetBtn.style.display = 'none';
          }
          
          alert(`⚠️ ${message.error || 'Không tìm thấy pattern chung giữa 2 phần tử đã chọn. Vui lòng chọn 2 link sản phẩm tương tự.'}`);
        }
        
        updateListingUI();
        saveState();
      } else if (selectionType === 'nextPage' || selectionType === 'nextPageV1' || selectionType === 'nextLi' || selectionType === 'nextLastPagination') {
        listingData.nextPageSelector = selector;
        const nextPageInput = document.getElementById('nextPageSelector');
        const nextPageBtn = document.getElementById('selectNextPageBtn');
        const nextPageV1Btn = document.getElementById('selectNextPageV1Btn');
        const nextLiBtn = document.getElementById('selectNextLiBtn');
        const nextLastPaginationBtn = document.getElementById('selectNextLastPaginationBtn');
        if (nextPageInput) nextPageInput.value = selector;
        if (nextPageBtn) {
          nextPageBtn.textContent = '🎯 Chọn';
          nextPageBtn.disabled = false;
        }
        if (nextPageV1Btn) {
          nextPageV1Btn.textContent = 'Next trang v1';
          nextPageV1Btn.disabled = false;
        }
        if (nextLiBtn) {
          nextLiBtn.textContent = 'next li';
          nextLiBtn.disabled = false;
        }
        if (nextLastPaginationBtn) {
          nextLastPaginationBtn.textContent = 'next last pagination';
          nextLastPaginationBtn.disabled = false;
        }
        const labelMap = {
          nextPage: 'Next Page',
          nextPageV1: 'Next Page v1',
          nextLi: 'Next li',
          nextLastPagination: 'Next last pagination'
        };
        const label = labelMap[selectionType] || 'Next Page';
        
        alert(`Selected ${label} selector!\n\nSelector: ${selector}`);
      }
      
      updateListingUI();
      saveState();
      sendResponse({ success: true });
    } catch (e) {
      console.error('Error handling listingSelectionComplete:', e);
      sendResponse({ success: false, error: e.message });
    }
  }
  
  return true;
});

// Send selector type preference to content script
async function sendSelectorTypeToContent() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab && tab.url && !tab.url.startsWith('chrome://') && 
        !tab.url.startsWith('edge://') && !tab.url.startsWith('about:')) {
      try {
        await chrome.tabs.sendMessage(tab.id, {
          action: 'setSelectorType',
          selectorType: selectorTypePreference
        });
      } catch (e) {
        // Content script might not be ready, that's okay
      }
    }
  } catch (error) {
    // Ignore errors
  }
}

// Save state vào storage
async function saveState() {
  await chrome.storage.local.set({
    isSelecting: isSelecting,
    selectedFields: selectedFields,
    currentMode: currentMode,
    listingData: listingData
  });
}

// Switch between Detail and Listing mode
function switchMode(mode) {
  currentMode = mode;
  
  // Update tab buttons
  const tabButtons = document.querySelectorAll('.tab-btn');
  if (tabButtons.length > 0) {
    tabButtons.forEach(btn => {
      if (btn.dataset.mode === mode) {
        btn.classList.add('active');
        btn.style.borderBottomColor = '#4CAF50';
        btn.style.color = '#4CAF50';
      } else {
        btn.classList.remove('active');
        btn.style.borderBottomColor = 'transparent';
        btn.style.color = '#666';
      }
    });
  }
  
  // Show/hide content
  const detailContent = document.getElementById('detailModeContent');
  const listingContent = document.getElementById('listingModeContent');
  
  if (detailContent && listingContent) {
    if (mode === 'detail') {
      detailContent.style.display = 'block';
      listingContent.style.display = 'none';
    } else {
      detailContent.style.display = 'none';
      listingContent.style.display = 'block';
    }
  }
  
  // Turn off selecting mode when switching
  if (isSelecting) {
    isSelecting = false;
    saveState();
    updateUI();
    sendModeToContent();
  }
  
  updateListingUI();
  saveState();
}

// Reset item link selection (two-click workflow)
async function resetItemLinkSelection() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab && tab.url && !tab.url.startsWith('chrome://') && 
        !tab.url.startsWith('edge://') && !tab.url.startsWith('about:')) {
      try {
        await chrome.tabs.sendMessage(tab.id, {
          action: 'resetItemLinkSelection'
        });
      } catch (e) {
        // Content script might not be ready
      }
    }
    
    // Reset UI
    const btn = document.getElementById('selectItemLinkBtn');
    const resetBtn = document.getElementById('resetItemLinkBtn');
    const statusDiv = document.getElementById('itemLinkStatus');
    const statusText = document.getElementById('itemLinkStatusText');
    const previewDiv = document.getElementById('itemLinkPreview');
    
    if (btn) {
      btn.textContent = '🎯 Click Item 1';
      btn.disabled = false;
    }
    if (resetBtn) {
      resetBtn.style.display = 'none';
    }
    if (statusDiv) {
      statusDiv.style.display = 'none';
    }
    if (previewDiv) {
      previewDiv.style.display = 'none';
    }
    
    listingData.itemLinkSelector = '';
    const itemLinkInput = document.getElementById('itemLinkSelector');
    if (itemLinkInput) itemLinkInput.value = '';
    
    updateListingUI();
    saveState();
  } catch (error) {
    console.error('Error resetting item link selection:', error);
  }
}

// Start listing selection
async function startListingSelection(type) {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (!tab || tab.url.startsWith('chrome://') || tab.url.startsWith('edge://') || tab.url.startsWith('about:')) {
      alert('⚠️ Không thể chọn trên trang này');
      return;
    }
    
    // Inject content script if needed
    try {
      await chrome.scripting.executeScript({
        target: { tabId: tab.id },
        files: ['content.js']
      });
      await new Promise(resolve => setTimeout(resolve, 300));
    } catch (e) {
      // Script might already be injected
    }
    
    // Send message to start listing selection
    await chrome.tabs.sendMessage(tab.id, {
      action: 'startListingSelection',
      selectionType: type,
      mode: 'listing'
    });
    
    // Update button state
    if (type === 'itemLink') {
      const btn = document.getElementById('selectItemLinkBtn');
      if (btn) {
        btn.textContent = '⏳ Đang chọn Item 1...';
        btn.disabled = true;
      }
    } else {
      const btn = document.getElementById('selectNextPageBtn');
      if (btn) {
        btn.textContent = '⏳ Đang chọn...';
        btn.disabled = true;
      }
    }
    
  } catch (error) {
    console.error('Error starting listing selection:', error);
    alert('❌ Lỗi: ' + error.message);
  }
}

// Reset listing mode
function resetListingMode() {
  listingData = {
    itemLinkSelector: '',
    nextPageSelector: '',
    url: ''
  };
  
  const itemLinkInput = document.getElementById('itemLinkSelector');
  const nextPageInput = document.getElementById('nextPageSelector');
  if (itemLinkInput) itemLinkInput.value = '';
  if (nextPageInput) nextPageInput.value = '';
  
  updateListingUI();
  saveState();
}

// Save listing template
async function saveListingTemplate() {
  if (!listingData || !listingData.itemLinkSelector) {
    alert('⚠️ Vui lòng chọn Item Link selector');
    return;
  }
  
  try {
    // Get current tab URL
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    const tab = tabs && tabs.length > 0 ? tabs[0] : null;
    listingData.url = (tab && tab.url) ? tab.url : '';
    
    // KHÔNG apply domain defaults khi lưu template - giữ nguyên selector đã chọn
    // applyListingDefaults(listingData.url);
    
    const template = {
      type: 'listing',
      itemSelector: listingData.itemLinkSelector, // Giữ nguyên selector đã chọn (chỉ có class, không có a và [href])
      nextPageSelector: listingData.nextPageSelector || null,
      url: listingData.url,
      createdAt: new Date().toISOString()
    };
    
    const jsonStr = JSON.stringify(template, null, 2);
    const blob = new Blob([jsonStr], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `listing_template_${Date.now()}.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    
    alert('✅ Đã lưu Listing Template!');
  } catch (error) {
    console.error('Error saving listing template:', error);
    alert('❌ Lỗi khi lưu template: ' + error.message);
  }
}

// Update listing UI
function updateListingUI() {
  const itemLinkInput = document.getElementById('itemLinkSelector');
  const nextPageInput = document.getElementById('nextPageSelector');
  const saveBtn = document.getElementById('saveListingTemplateBtn');
  
  if (itemLinkInput) {
    itemLinkInput.value = listingData.itemLinkSelector || '';
  }
  if (nextPageInput) {
    nextPageInput.value = listingData.nextPageSelector || '';
  }
  if (saveBtn) {
    saveBtn.disabled = !listingData.itemLinkSelector;
  }
  
  // Reset button states
  const selectItemLinkBtn = document.getElementById('selectItemLinkBtn');
  const selectNextPageBtn = document.getElementById('selectNextPageBtn');
  const selectNextPageV1Btn = document.getElementById('selectNextPageV1Btn');
  const selectNextLiBtn = document.getElementById('selectNextLiBtn');
  const selectNextLastPaginationBtn = document.getElementById('selectNextLastPaginationBtn');
  if (selectItemLinkBtn) {
    selectItemLinkBtn.textContent = '🎯 Chọn';
    selectItemLinkBtn.disabled = false;
  }
  if (selectNextPageBtn) {
    selectNextPageBtn.textContent = '🎯 Chọn';
    selectNextPageBtn.disabled = false;
  }
  if (selectNextPageV1Btn) {
    selectNextPageV1Btn.textContent = 'Next trang v1';
    selectNextPageV1Btn.disabled = false;
  }
}

// Send mode to content script
async function sendModeToContent() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (tab && tab.url && !tab.url.startsWith('chrome://') && 
        !tab.url.startsWith('edge://') && !tab.url.startsWith('about:')) {
      try {
        await chrome.tabs.sendMessage(tab.id, {
          action: 'setMode',
          mode: currentMode
        });
      } catch (e) {
        // Content script might not be ready
      }
    }
  } catch (error) {
    // Ignore errors
  }
}

// Update UI
function updateUI() {
  const toggleBtn = document.getElementById('toggleBtn');
  const toggleText = document.getElementById('toggleText');
  const statusIndicator = document.getElementById('statusIndicator');
  const statusText = document.getElementById('statusText');
  const fieldsList = document.getElementById('fieldsList');
  const scrapeBtn = document.getElementById('scrapeBtn');
  const scrapeCrawl4AIBtn = document.getElementById('scrapeCrawl4AIBtn');
  const exportBtn = document.getElementById('exportBtn');
  const saveTemplateBtn = document.getElementById('saveTemplateBtn');

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

  // Enable/disable buttons
  const hasFields = selectedFields.length > 0;
  scrapeBtn.disabled = !hasFields;
  scrapeCrawl4AIBtn.disabled = !hasFields;
  exportBtn.disabled = !hasFields;
  saveTemplateBtn.disabled = !hasFields;
  
  // Update fields list
  if (selectedFields.length === 0) {
    fieldsList.innerHTML = '<p class="empty-message">Chưa có trường nào được chọn</p>';
  } else {
    fieldsList.innerHTML = selectedFields.map((field, index) => {
      const valueType = field.valueType || 'text';
      const customSelector = field.customSelector || field.selector;
      return `
      <div class="field-item" data-index="${index}">
        <div class="field-header">
          <span class="field-name" data-index="${index}">${field.name || `Trường ${index + 1}`}</span>
          <div class="field-actions">
            <button class="btn-edit" data-index="${index}" title="Chỉnh sửa">✎</button>
            <button class="btn-remove" data-index="${index}" title="Xóa">×</button>
          </div>
        </div>
        <div class="field-info">
          <small>Selector: <code>${customSelector}</code></small>
          <small style="display: block; margin-top: 4px;">Value: <code>${valueType}</code></small>
        </div>
        <div class="field-edit-panel" id="edit-panel-${index}" style="display: none;">
          <div class="edit-form">
            <div class="form-group">
              <label>Title (Key name):</label>
              <input type="text" class="edit-title" value="${field.name || ''}" data-index="${index}">
            </div>
            <div class="form-group">
              <label>Value Type:</label>
              <select class="edit-value-type" data-index="${index}">
                <option value="text" ${valueType === 'text' ? 'selected' : ''}>Text</option>
                <option value="html" ${valueType === 'html' ? 'selected' : ''}>HTML</option>
                <option value="src" ${valueType === 'src' ? 'selected' : ''}>Src (Image)</option>
                <option value="href" ${valueType === 'href' ? 'selected' : ''}>Href (Link)</option>
                <option value="alt" ${valueType === 'alt' ? 'selected' : ''}>Alt (Image)</option>
                <option value="title" ${valueType === 'title' ? 'selected' : ''}>Title</option>
                <option value="data-id" ${valueType === 'data-id' ? 'selected' : ''}>Data ID</option>
                <option value="data-phone" ${valueType === 'data-phone' ? 'selected' : ''}>Data Phone</option>
                <option value="innerText" ${valueType === 'innerText' ? 'selected' : ''}>Inner Text</option>
                <option value="all" ${valueType === 'all' || valueType === 'container' ? 'selected' : ''}>All (Container - lấy toàn bộ giá trị)</option>
              </select>
            </div>
            <div class="form-group">
              <label>Selector (có thể chỉnh để lấy từ parent):</label>
              <div style="display: flex; gap: 5px; margin-bottom: 5px;">
                <input type="text" class="edit-selector" value="${escapeAttr(customSelector)}" data-index="${index}" placeholder="Nhập selector" style="flex: 1;">
                <button class="btn-convert-selector" data-index="${index}" title="Chuyển đổi giữa XPath và CSS" style="padding: 5px 10px; background: #2196F3; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 12px;">🔄</button>
              </div>
              <small class="help-text">Ví dụ: thay ".col-xs-6 > strong" bằng ".col-xs-6" để lấy từ parent. Nhấn 🔄 để chuyển đổi giữa XPath và CSS            <div class="form-group">
              <label>Exclude words (split by | or ,):</label>
              <input type="text" class="edit-exclude-words" value="${escapeAttr(field.excludeWords || '')}" data-index="${index}" placeholder="VD: Chieu rong|Chieu dai">
              <small class="help-text">Example: enter label text to remove from value.</small>
            </div>
</small>
            </div>
            <div class="form-group">
              <label>Giá trị nhận được:</label>
              <div class="preview-value" id="preview-${index}" data-index="${index}">
                <span class="preview-loading">Đang tải...</span>
              </div>
            </div>
            <div class="form-actions">
              <button class="btn-save" data-index="${index}">Lưu</button>
              <button class="btn-cancel" data-index="${index}">Hủy</button>
            </div>
          </div>
        </div>
      </div>
    `;
    }).join('');

    // Add event listeners
    fieldsList.querySelectorAll('.btn-remove').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(e.target.dataset.index);
        removeField(index);
      });
    });

    fieldsList.querySelectorAll('.btn-edit, .field-name').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(e.target.dataset.index);
        toggleEditPanel(index);
      });
    });

    fieldsList.querySelectorAll('.btn-save').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(e.target.dataset.index);
        saveFieldEdit(index);
      });
    });

    fieldsList.querySelectorAll('.btn-cancel').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(e.target.dataset.index);
        toggleEditPanel(index);
      });
    });

    // Add change listeners for valueType and selector to preview value
    fieldsList.querySelectorAll('.edit-value-type').forEach(select => {
      select.addEventListener('change', (e) => {
        const index = parseInt(e.target.dataset.index);
        previewFieldValue(index);
      });
    });

    fieldsList.querySelectorAll('.edit-selector').forEach(input => {
      let timeout;
      input.addEventListener('input', (e) => {
        clearTimeout(timeout);
        const index = parseInt(e.target.dataset.index);
        // Debounce để không gọi quá nhiều lần khi đang gõ
        timeout = setTimeout(() => {
          previewFieldValue(index);
        }, 500);
      });
    });

    fieldsList.querySelectorAll('.btn-convert-selector').forEach(btn => {
      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const index = parseInt(e.target.dataset.index);
        convertSelectorType(index);
      });
    });

    // Load preview for all open panels
    fieldsList.querySelectorAll('.field-edit-panel[style*="block"]').forEach(panel => {
      const index = parseInt(panel.id.replace('edit-panel-', ''));
      previewFieldValue(index);
    });
  }
}

// Toggle selecting mode
async function toggleSelecting() {
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    // Check if we can inject script (for pages like chrome://, about:, localhost, streamlit, etc)
    const isRestrictedPage = !tab.url || 
        tab.url.startsWith('chrome://') || 
        tab.url.startsWith('edge://') || 
        tab.url.startsWith('about:') ||
        tab.url.includes('localhost') ||
        tab.url.includes('127.0.0.1') ||
        tab.url.includes('streamlit');
    
    if (isRestrictedPage) {
      // Don't show alert, just silently fail and show friendly message
      console.log('Extension cannot run on this page:', tab.url);
      
      // Update UI to show message
      const statusText = document.getElementById('statusText');
      if (statusText) {
        statusText.textContent = '⚠️ Không hoạt động trên trang này (localhost/chrome pages)';
        statusText.style.color = '#ff9800';
      }
      
      // Don't toggle state
      return;
    }
    
    // Store previous state
    const previousState = isSelecting;
    isSelecting = !isSelecting;
    await saveState();
    updateUI(); // Update UI immediately for better UX

    // Always try to inject content script first
    let scriptReady = false;
    
    // Step 1: Try to ping first to see if script is already there
    try {
      const pingResponse = await Promise.race([
        chrome.tabs.sendMessage(tab.id, { action: 'ping' }),
        new Promise((_, reject) => setTimeout(() => reject(new Error('Ping timeout')), 500))
      ]);
      if (pingResponse && pingResponse.ready) {
        scriptReady = true;
        console.log('Content script already exists and ready');
      }
    } catch (pingError) {
      console.log('Content script not ready, will inject:', pingError.message);
    }
    
    // Step 2: If not ready, inject script
    if (!scriptReady) {
      try {
        await chrome.scripting.executeScript({
          target: { tabId: tab.id },
          files: ['content.js']
        });
        console.log('Content script injected successfully');
        // Wait longer after injection
        await new Promise(resolve => setTimeout(resolve, 500));
        
        // Step 3: Verify script is ready by pinging
        for (let pingAttempt = 0; pingAttempt < 5; pingAttempt++) {
          try {
            const verifyResponse = await Promise.race([
              chrome.tabs.sendMessage(tab.id, { action: 'ping' }),
              new Promise((_, reject) => setTimeout(() => reject(new Error('Ping timeout')), 500))
            ]);
            if (verifyResponse && verifyResponse.ready) {
              scriptReady = true;
              console.log('Content script verified ready after injection');
              break;
            }
          } catch (verifyError) {
            console.log(`Ping attempt ${pingAttempt + 1} failed, waiting...`);
            await new Promise(resolve => setTimeout(resolve, 200));
          }
        }
      } catch (injectError) {
        console.error('Failed to inject content script:', injectError);
        // Script might already be injected but not responding
        // Try pinging a few more times
        for (let pingAttempt = 0; pingAttempt < 3; pingAttempt++) {
          try {
            const verifyResponse = await Promise.race([
              chrome.tabs.sendMessage(tab.id, { action: 'ping' }),
              new Promise((_, reject) => setTimeout(() => reject(new Error('Ping timeout')), 500))
            ]);
            if (verifyResponse && verifyResponse.ready) {
              scriptReady = true;
              console.log('Content script found after injection error');
              break;
            }
          } catch (verifyError) {
            await new Promise(resolve => setTimeout(resolve, 200));
          }
        }
      }
    }
    
    // Now try to toggle selecting mode
    try {
      const response = await Promise.race([
        chrome.tabs.sendMessage(tab.id, {
          action: 'toggleSelecting',
          isSelecting: isSelecting,
          selectorTypePreference: selectorTypePreference
        }),
        new Promise((_, reject) => 
          setTimeout(() => reject(new Error('Timeout after 3 seconds')), 3000)
        )
      ]);
      
      if (response && response.success) {
        console.log('✅ Toggle successful:', response);
        // Success - UI already updated above
      } else {
        throw new Error('Content script did not return success');
      }
    } catch (toggleError) {
      console.error('❌ Toggle failed:', toggleError);
      
      // Revert state
      isSelecting = previousState;
      await saveState();
      updateUI();
      
      // Show friendly error
      alert('Không thể bật chế độ chọn.\n\n' +
            'Vui lòng:\n' +
            '1. Reload trang web (F5 hoặc Ctrl+R)\n' +
            '2. Đợi trang load hoàn toàn\n' +
            '3. Thử lại\n\n' +
            'Lỗi: ' + toggleError.message);
      return;
    }
  } catch (error) {
    console.error('Toggle error:', error);
    alert('Lỗi: ' + error.message);
  }
}

// Toggle edit panel
function toggleEditPanel(index) {
  const panel = document.getElementById(`edit-panel-${index}`);
  if (panel) {
    const isVisible = panel.style.display !== 'none';
    // Close all panels first
    document.querySelectorAll('.field-edit-panel').forEach(p => {
      p.style.display = 'none';
    });
    // Toggle current panel
    panel.style.display = isVisible ? 'none' : 'block';
    
    // Load preview when opening panel
    if (!isVisible) {
      previewFieldValue(index);
    }
  }
}

// Preview field value based on current settings
async function previewFieldValue(index) {
  const field = selectedFields[index];
  if (!field) return;

  const previewDiv = document.getElementById(`preview-${index}`);
  if (!previewDiv) return;

  const valueTypeSelect = document.querySelector(`#edit-panel-${index} .edit-value-type`);
  const selectorInput = document.querySelector(`#edit-panel-${index} .edit-selector`);
  const excludeInput = document.querySelector(`#edit-panel-${index} .edit-exclude-words`);

  const valueType = valueTypeSelect ? valueTypeSelect.value : (field.valueType || 'text');
  const selector = selectorInput ? selectorInput.value.trim() : (field.customSelector || field.selector);
  const excludeWords = excludeInput ? excludeInput.value.trim() : (field.excludeWords || '');

  if (!selector) {
    previewDiv.innerHTML = '<span class="preview-error">Chưa có selector</span>';
    return;
  }

  previewDiv.innerHTML = '<span class="preview-loading">Đang tải...</span>';

  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (!tab.url || tab.url.startsWith('chrome://') || tab.url.startsWith('edge://') || tab.url.startsWith('about:')) {
      previewDiv.innerHTML = '<span class="preview-error">Không thể preview trên trang này</span>';
      return;
    }

    const response = await chrome.tabs.sendMessage(tab.id, {
      action: 'previewValue',
      selector: selector,
      valueType: valueType,
      field: { ...field, excludeWords: excludeWords }
    });

    if (response && response.success) {
      const value = response.value;
      if (value === null || value === undefined) {
        previewDiv.innerHTML = '<span class="preview-error">Không tìm thấy giá trị</span>';
      } else if (response.isObject && typeof value === 'object' && !Array.isArray(value)) {
        // Display object preview for container values (valueType 'all')
        const count = response.count || Object.keys(value).length;
        const preview = Object.entries(value).slice(0, 10).map(([key, val]) => 
          `<strong>${escapeHtml(key)}</strong>: ${escapeHtml(String(val).substring(0, 50))}${String(val).length > 50 ? '...' : ''}`
        ).join('<br>') + (Object.keys(value).length > 10 ? `<br>... và ${Object.keys(value).length - 10} trường khác` : '');
        previewDiv.innerHTML = `<span class="preview-success"><strong>📦 Tìm thấy ${count} trường:</strong><br>${preview}</span>`;
      } else if (Array.isArray(value)) {
        // Display array preview for images
        const count = response.count || value.length;
        const preview = value.length > 0 
          ? value.slice(0, 5).map((v, i) => `${i + 1}. ${escapeHtml(String(v).substring(0, 60))}`).join('<br>') + (value.length > 5 ? `<br>... và ${value.length - 5} hình khác` : '')
          : '(Rỗng)';
        previewDiv.innerHTML = `<span class="preview-success"><strong>📷 Tìm thấy ${count} hình:</strong><br>${preview}</span>`;
      } else if (value === '') {
        previewDiv.innerHTML = '<span class="preview-empty">(Rỗng)</span>';
      } else {
        // Truncate long values
        const displayValue = typeof value === 'string' && value.length > 200 
          ? value.substring(0, 200) + '...' 
          : value;
        previewDiv.innerHTML = `<span class="preview-success">${escapeHtml(String(displayValue))}</span>`;
      }
    } else {
      previewDiv.innerHTML = '<span class="preview-error">Lỗi: ' + (response.error || 'Không thể lấy giá trị') + '</span>';
    }
  } catch (error) {
    previewDiv.innerHTML = '<span class="preview-error">Lỗi: ' + error.message + '</span>';
  }
}

// Escape HTML để hiển thị an toàn
function escapeHtml(text) {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

// Escape HTML attribute (dùng cho value="..." trong input)
function escapeAttr(text) {
  if (!text) return '';
  return String(text)
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

// Convert selector type (XPath <-> CSS)
async function convertSelectorType(index) {
  const field = selectedFields[index];
  if (!field) return;

  const selectorInput = document.querySelector(`#edit-panel-${index} .edit-selector`);
  if (!selectorInput) return;

  const currentSelector = selectorInput.value.trim();
  if (!currentSelector) return;

  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    if (!tab.url || tab.url.startsWith('chrome://') || tab.url.startsWith('edge://') || tab.url.startsWith('about:')) {
      alert('Không thể chuyển đổi selector trên trang này');
      return;
    }

    // Send message to content script to convert selector
    const response = await chrome.tabs.sendMessage(tab.id, {
      action: 'convertSelector',
      selector: currentSelector,
      field: field
    });

    if (response && response.success && response.convertedSelector) {
      selectorInput.value = response.convertedSelector;
      // Trigger preview update
      previewFieldValue(index);
    } else {
      alert('Không thể chuyển đổi selector. Có thể selector không hợp lệ hoặc không tìm thấy element.');
    }
  } catch (error) {
    alert('Lỗi khi chuyển đổi selector: ' + error.message);
  }
}

// Save field edit
async function saveFieldEdit(index) {
  const field = selectedFields[index];
  if (!field) return;

  const titleInput = document.querySelector(`#edit-panel-${index} .edit-title`);
  const valueTypeSelect = document.querySelector(`#edit-panel-${index} .edit-value-type`);
  const selectorInput = document.querySelector(`#edit-panel-${index} .edit-selector`);
  const excludeInput = document.querySelector(`#edit-panel-${index} .edit-exclude-words`);

  if (titleInput) {
    field.name = titleInput.value.trim() || field.name;
  }
  if (valueTypeSelect) {
    field.valueType = valueTypeSelect.value;
  }
  if (excludeInput) {
    const excludeWords = excludeInput.value.trim();
    if (excludeWords) {
      field.excludeWords = excludeWords;
    } else {
      delete field.excludeWords;
    }
  }
  if (selectorInput) {
    const newSelector = selectorInput.value.trim();
    if (newSelector) {
      field.customSelector = newSelector;
    } else {
      delete field.customSelector;
    }
  }

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

// Scrape với Crawl4AI
async function scrapeWithCrawl4AI() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  const resultDiv = document.getElementById('result');
  
  resultDiv.classList.remove('hidden');
  resultDiv.innerHTML = '<p class="loading">Đang cào với Crawl4AI...<br><small>Server: http://localhost:8765</small></p>';

  try {
    const response = await fetch('http://localhost:8765', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        action: 'scrape_with_fields',
        url: tab.url,
        fields: selectedFields
      })
    });

    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const result = await response.json();

    if (result.success) {
      resultDiv.innerHTML = `
        <h4>✅ Kết quả (Crawl4AI):</h4>
        <pre class="result-json">${JSON.stringify(result.data, null, 2)}</pre>
      `;
    } else {
      resultDiv.innerHTML = `<p class="error">❌ Lỗi: ${result.error}</p>`;
    }
  } catch (error) {
    resultDiv.innerHTML = `<p class="error">❌ Lỗi: ${error.message}<br><small>Đảm bảo server đang chạy: python extension_api_server.py</small></p>`;
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
      // response.data is already an object with field names as keys
      const data = {
        url: tab.url,
        scrapedAt: new Date().toISOString(),
        fields: response.data
      };
      
      const json = JSON.stringify(data, null, 2);
      const blob = new Blob([json], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `scraped_data_${Date.now()}.json`;
      a.click();
      URL.revokeObjectURL(url);
      
      alert('✅ Export thành công!');
    } else {
      alert('❌ Lỗi: Không thể lấy dữ liệu từ trang web');
    }
  } catch (error) {
    alert('❌ Lỗi khi export: ' + error.message);
  }
}

// Save Template
async function saveTemplate() {
  if (selectedFields.length === 0) {
    alert('❌ Chưa có trường nào để lưu template!');
    return;
  }
  
  const templateName = prompt('Nhập tên template:', 'template_' + Date.now());
  if (!templateName) return;
  
  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    
    const template = {
      name: templateName,
      url: tab.url,
      createdAt: new Date().toISOString(),
      fields: selectedFields.map(f => {
        const fieldObj = {
          name: f.name,
          valueType: f.valueType || 'text',
          textContent: f.textContent || ''  // Lưu textContent để dùng làm fallback khi Crawl4AI trả về null
        };
        
        // Giữ đúng loại selector user đã chọn cho từng field
        // Ưu tiên: customSelector -> selector -> fallback theo selectorType của field
        let selector = f.customSelector || f.selector || '';
        
        // Nếu user chọn XPath nhưng selector hiện tại là CSS (do fallback), dùng XPath đã lưu
        if (f.selectorType === 'xpath' && (!selector || !selector.startsWith('//')) && f.xpath) {
          selector = f.xpath;
        }
        
        // Nếu user chọn CSS nhưng selector hiện tại là XPath, ưu tiên cssSelector nếu có
        if (f.selectorType === 'css' && selector && selector.startsWith('//') && f.cssSelector) {
          selector = f.cssSelector;
        }
        
        // Fallback cuối cùng nếu vẫn chưa có selector
        if (!selector) {
          selector = f.xpath || f.cssSelector || '';
        }
        
        fieldObj.selector = selector;
        if (f.excludeWords) {
          fieldObj.excludeWords = f.excludeWords;
        }
        
        return fieldObj;
      })
    };
    
    // Save to storage
    const result = await chrome.storage.local.get(['templates']);
    const templates = result.templates || [];
    templates.push(template);
    await chrome.storage.local.set({ templates: templates });
    
    // Also download as JSON file
    const json = JSON.stringify(template, null, 2);
    const blob = new Blob([json], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${templateName}.json`;
    a.click();
    URL.revokeObjectURL(url);
    
    alert(`✅ Template "${templateName}" đã được lưu!`);
  } catch (error) {
    alert('❌ Lỗi khi lưu template: ' + error.message);
  }
}

// Open template modal
function openTemplateModal() {
  const modal = document.getElementById('templateModal');
  if (modal) {
    modal.style.display = 'block';
    // Reset form
    document.getElementById('templateFileInput').value = '';
    document.getElementById('urlsInput').value = '';
    document.getElementById('templateScrapeProgress').style.display = 'none';
  }
}

// Close template modal
function closeTemplateModal() {
  const modal = document.getElementById('templateModal');
  if (modal) {
    modal.style.display = 'none';
  }
}

// Scrape with template
let isScraping = false;
let scrapeCanceled = false;

async function startTemplateScrape() {
  if (isScraping) {
    alert('Đang cào dữ liệu, vui lòng đợi...');
    return;
  }

  const fileInput = document.getElementById('templateFileInput');
  const urlsInput = document.getElementById('urlsInput');
  const progressDiv = document.getElementById('templateScrapeProgress');
  const progressText = document.getElementById('templateProgressText');
  const progressBar = document.getElementById('templateProgressBar');
  const startBtn = document.getElementById('startTemplateScrapeBtn');

  // Validate inputs
  if (!fileInput.files || fileInput.files.length === 0) {
    alert('❌ Vui lòng chọn file template!');
    return;
  }

  const urls = urlsInput.value.trim().split('\n').filter(url => url.trim().length > 0);
  if (urls.length === 0) {
    alert('❌ Vui lòng nhập ít nhất một URL!');
    return;
  }

  // Read template file
  let template;
  try {
    const file = fileInput.files[0];
    const fileText = await file.text();
    template = JSON.parse(fileText);
    
    if (!template.fields || !Array.isArray(template.fields) || template.fields.length === 0) {
      alert('❌ Template không hợp lệ: không có fields!');
      return;
    }
    
    // Convert valueType to type for API compatibility
    template.fields = template.fields.map(field => {
      const convertedField = { ...field };
      // If field has valueType but no type, convert it
      if (convertedField.valueType && !convertedField.type) {
        convertedField.type = convertedField.valueType;
      }
      // Ensure type exists (default to 'text')
      if (!convertedField.type) {
        convertedField.type = 'text';
      }
      return convertedField;
    });
  } catch (error) {
    alert('❌ Lỗi đọc file template: ' + error.message);
    return;
  }

  // Start scraping
  isScraping = true;
  scrapeCanceled = false;
  startBtn.disabled = true;
  startBtn.textContent = 'Đang cào...';
  progressDiv.style.display = 'block';
  progressBar.style.width = '0%';

  const results = [];
  const totalUrls = urls.length;

  try {
    for (let i = 0; i < urls.length; i++) {
      if (scrapeCanceled) {
        progressText.textContent = 'Đã hủy';
        break;
      }

      const url = urls[i].trim();
      progressText.textContent = `Đang cào ${i + 1}/${totalUrls}: ${url.substring(0, 50)}...`;
      progressBar.style.width = `${((i + 1) / totalUrls) * 100}%`;

      try {
        // Send request to API server
        const response = await fetch('http://localhost:8765', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            action: 'scrape_with_template',
            template: template,
            url: url
          })
        });

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }

        const result = await response.json();
        
        if (result.success) {
          results.push({
            url: url,
            success: true,
            data: result.data,
            scrapedAt: new Date().toISOString()
          });
        } else {
          results.push({
            url: url,
            success: false,
            error: result.error || 'Unknown error',
            scrapedAt: new Date().toISOString()
          });
        }
      } catch (error) {
        results.push({
          url: url,
          success: false,
          error: error.message,
          scrapedAt: new Date().toISOString()
        });
      }

      // Small delay to avoid overwhelming the server
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    // Show results
    if (!scrapeCanceled) {
      progressText.textContent = `Hoàn thành! Đã cào ${results.filter(r => r.success).length}/${totalUrls} URL thành công.`;
      progressBar.style.width = '100%';

      // Download results as JSON
      const resultsJson = JSON.stringify({
        template: template.name,
        scrapedAt: new Date().toISOString(),
        totalUrls: totalUrls,
        successCount: results.filter(r => r.success).length,
        results: results
      }, null, 2);

      const blob = new Blob([resultsJson], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `scraped_with_template_${Date.now()}.json`;
      a.click();
      URL.revokeObjectURL(url);

      alert(`✅ Hoàn thành! Đã cào ${results.filter(r => r.success).length}/${totalUrls} URL thành công.\nFile JSON đã được tải xuống.`);
    }
  } catch (error) {
    alert('❌ Lỗi khi cào: ' + error.message);
    progressText.textContent = 'Lỗi: ' + error.message;
  } finally {
    isScraping = false;
    startBtn.disabled = false;
    startBtn.textContent = '🚀 Bắt đầu cào';
  }
}

// Cancel template scrape
function cancelTemplateScrape() {
  if (isScraping) {
    scrapeCanceled = true;
    const startBtn = document.getElementById('startTemplateScrapeBtn');
    if (startBtn) {
      startBtn.disabled = false;
      startBtn.textContent = '🚀 Bắt đầu cào';
    }
  }
  closeTemplateModal();
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
document.addEventListener('DOMContentLoaded', async () => {
  await loadState();
  
  // Tab switching
  const tabButtons = document.querySelectorAll('.tab-btn');
  if (tabButtons.length > 0) {
    tabButtons.forEach(btn => {
      btn.addEventListener('click', () => {
        const mode = btn.dataset.mode;
        switchMode(mode);
      });
    });
  }
  
  // Listing mode buttons
  const selectItemLinkBtn = document.getElementById('selectItemLinkBtn');
  const resetItemLinkBtn = document.getElementById('resetItemLinkBtn');
  const selectNextPageBtn = document.getElementById('selectNextPageBtn');
  const selectNextPageV1Btn = document.getElementById('selectNextPageV1Btn');
  const selectNextLiBtn = document.getElementById('selectNextLiBtn');
  const selectNextLastPaginationBtn = document.getElementById('selectNextLastPaginationBtn');
  const resetListingBtn = document.getElementById('resetListingBtn');
  const saveListingTemplateBtn = document.getElementById('saveListingTemplateBtn');
  
  if (selectItemLinkBtn) {
    selectItemLinkBtn.addEventListener('click', () => startListingSelection('itemLink'));
  }
  if (resetItemLinkBtn) {
    resetItemLinkBtn.addEventListener('click', () => resetItemLinkSelection());
  }
  if (selectNextPageBtn) {
    selectNextPageBtn.addEventListener('click', () => startListingSelection('nextPage'));
  }
  if (selectNextPageV1Btn) {
    selectNextPageV1Btn.addEventListener('click', () => startListingSelection('nextPageV1'));
  }
  if (selectNextLiBtn) {
    selectNextLiBtn.addEventListener('click', () => startListingSelection('nextLi'));
  }
  if (selectNextLastPaginationBtn) {
    selectNextLastPaginationBtn.addEventListener('click', () => startListingSelection('nextLastPagination'));
  }
  if (resetListingBtn) {
    resetListingBtn.addEventListener('click', resetListingMode);
  }
  if (saveListingTemplateBtn) {
    saveListingTemplateBtn.addEventListener('click', async (e) => {
      e.preventDefault();
      await saveListingTemplate();
    });
  }
  
  const toggleBtn = document.getElementById('toggleBtn');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', toggleSelecting);
  }
  
  // Selector type radio buttons
  const cssRadio = document.getElementById('selectorTypeCss');
  const xpathRadio = document.getElementById('selectorTypeXpath');
  if (cssRadio && xpathRadio) {
    cssRadio.addEventListener('change', async (e) => {
      if (e.target.checked) {
        selectorTypePreference = 'css';
        await saveState();
        await sendSelectorTypeToContent();
      }
    });
    xpathRadio.addEventListener('change', async (e) => {
      if (e.target.checked) {
        selectorTypePreference = 'xpath';
        await saveState();
        await sendSelectorTypeToContent();
      }
    });
  }
  const clearBtn = document.getElementById('clearBtn');
  if (clearBtn) {
    clearBtn.addEventListener('click', clearAll);
  }
  
  const scrapeBtn = document.getElementById('scrapeBtn');
  if (scrapeBtn) {
    scrapeBtn.addEventListener('click', scrapeData);
  }
  
  const scrapeCrawl4AIBtn = document.getElementById('scrapeCrawl4AIBtn');
  if (scrapeCrawl4AIBtn) {
    scrapeCrawl4AIBtn.addEventListener('click', scrapeWithCrawl4AI);
  }
  
  const exportBtn = document.getElementById('exportBtn');
  if (exportBtn) {
    exportBtn.addEventListener('click', exportJSON);
  }
  
  const saveTemplateBtn = document.getElementById('saveTemplateBtn');
  if (saveTemplateBtn) {
    saveTemplateBtn.addEventListener('click', saveTemplate);
  }
  
  // Template scraping buttons
  const scrapeWithTemplateBtn = document.getElementById('scrapeWithTemplateBtn');
  if (scrapeWithTemplateBtn) {
    scrapeWithTemplateBtn.addEventListener('click', openTemplateModal);
  }
  
  const closeTemplateModalBtn = document.getElementById('closeTemplateModal');
  if (closeTemplateModalBtn) {
    closeTemplateModalBtn.addEventListener('click', closeTemplateModal);
  }
  
  const startTemplateScrapeBtn = document.getElementById('startTemplateScrapeBtn');
  if (startTemplateScrapeBtn) {
    startTemplateScrapeBtn.addEventListener('click', startTemplateScrape);
  }
  
  const cancelTemplateScrapeBtn = document.getElementById('cancelTemplateScrapeBtn');
  if (cancelTemplateScrapeBtn) {
    cancelTemplateScrapeBtn.addEventListener('click', cancelTemplateScrape);
  }
  
  // Close modal when clicking outside
  const templateModal = document.getElementById('templateModal');
  if (templateModal) {
    templateModal.addEventListener('click', (e) => {
      if (e.target === templateModal) {
        closeTemplateModal();
      }
    });
  }
});
