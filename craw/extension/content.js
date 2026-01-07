// Content script - chạy trên trang web
// VERSION: 2025-12-11-v2 (force reload)
let isSelecting = false;
let selectedFields = [];
let highlightOverlay = null;
let selectorTypePreference = 'xpath'; // 'css' or 'xpath' - có thể toggle bằng phím X
let currentMode = 'detail'; // 'detail' or 'listing'
let listingSelectionType = null; // 'itemLink' or 'nextPage' when in listing selection mode
let firstElement = null; // First clicked element for two-click strategy
let secondElement = null; // Second clicked element for two-click strategy

// Handle keyboard shortcuts
function handleKeyPress(e) {
  if (e.key === 'Escape') {
    if (isSelecting) {
      isSelecting = false;
      document.removeEventListener('click', handleElementClick, true);
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('keydown', handleKeyPress);
      document.body.style.cursor = '';
      document.body.classList.remove('scraper-selecting');
      hideHighlight();
      document.querySelectorAll('.scraper-field-selected').forEach(el => {
        el.classList.remove('scraper-field-selected');
      });
      chrome.storage.local.set({ isSelecting: false });
    }
    return;
  }
  if (e.key === 'x' || e.key === 'X') {
    selectorTypePreference = selectorTypePreference === 'css' ? 'xpath' : 'css';
    const notification = document.createElement('div');
    notification.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #4CAF50; color: white; padding: 10px 20px; border-radius: 5px; z-index: 10000; font-size: 14px;';
    notification.textContent = `Selector: ${selectorTypePreference.toUpperCase()}`;
    document.body.appendChild(notification);
    setTimeout(() => notification.remove(), 2000);
  }
}

// Find label sibling - improved to find label in same parent
function findLabelSibling(element) {
  // First try previous sibling
  let sibling = element.previousElementSibling;
  let checkedCount = 0;
  while (sibling && checkedCount < 3) {
    const siblingText = extractText(sibling).trim();
    if (siblingText.length > 0 && siblingText.length < 50 && siblingText.length > 2) {
      return { element: sibling, text: siblingText };
    }
    sibling = sibling.previousElementSibling;
    checkedCount++;
  }
  
  // If not found, check all siblings in same parent
  const parent = element.parentElement;
  if (parent) {
    const allChildren = Array.from(parent.children);
    const elementIndex = allChildren.indexOf(element);
    
    // Check previous siblings in parent
    for (let i = elementIndex - 1; i >= 0 && i >= elementIndex - 5; i--) {
      const child = allChildren[i];
      const childText = extractText(child).trim();
      if (childText.length > 0 && childText.length < 50 && childText.length > 2) {
        // Check if it looks like a label (has class "title" or "label" or short text)
        const hasLabelClass = child.className && (
          child.className.includes('title') || 
          child.className.includes('label') ||
          child.className.includes('name')
        );
        if (hasLabelClass || (childText.length < 30 && !childText.match(/^\d+/))) {
          return { element: child, text: childText };
        }
      }
    }
  }
  
  return null;
}

// Hàm tìm phần tử bằng XPath (thay thế cho document.querySelector)
function getElementByXPath(xpath) {
  const result = document.evaluate(
    xpath, 
    document, 
    null, 
    XPathResult.FIRST_ORDERED_NODE_TYPE, 
    null
  );
  return result.singleNodeValue;
}

// Hàm tìm tất cả phần tử bằng XPath
function getElementsByXPath(xpath) {
  const result = document.evaluate(
    xpath,
    document,
    null,
    XPathResult.ORDERED_NODE_SNAPSHOT_TYPE,
    null
  );
  const elements = [];
  for (let i = 0; i < result.snapshotLength; i++) {
    elements.push(result.snapshotItem(i));
  }
  return elements;
}

// Extract lat,lng from map URLs (Google Maps embed: center=lat,lng or q=lat,lng)
function getLatLngFromUrl(url) {
  if (!url || typeof url !== 'string') return null;
  try {
    const decoded = decodeURIComponent(url);
    const m = decoded.match(/(?:center|q)=(-?\d+\.?\d*),\s*(-?\d+\.?\d*)/i);
    if (m) {
      const lat = parseFloat(m[1]);
      const lng = parseFloat(m[2]);
      if (!isNaN(lat) && !isNaN(lng)) return { lat, lng };
    }
    return null;
  } catch (e) {
    return null;
  }
}

// Try to read lat,lng from element or its ancestors (iframe/src/data-*)
function getLatLngFromElement(el) {
  if (!el) return null;
  const readAttrs = (node) => {
    if (!node || !node.getAttribute) return null;
    const src = node.getAttribute('src') || node.getAttribute('data-src');
    const coords = getLatLngFromUrl(src);
    if (coords) return coords;
    const dLat = node.getAttribute('data-lat');
    const dLng = node.getAttribute('data-lng');
    if (dLat && dLng && !isNaN(parseFloat(dLat)) && !isNaN(parseFloat(dLng))) {
      return { lat: parseFloat(dLat), lng: parseFloat(dLng) };
    }
    return null;
  };
  const selfCoords = readAttrs(el);
  if (selfCoords) return selfCoords;
  let p = el.parentElement;
  while (p && p !== document.body) {
    const pc = readAttrs(p);
    if (pc) return pc;
    p = p.parentElement;
  }
  return null;
}

// Helper function: Tạo XPath tuyệt đối cho element (dùng để tạo uniqueId)
function getAbsoluteXPath(element) {
  if (!element || element.nodeType !== 1) return null;
  
  const parts = [];
  let current = element;
  
  while (current && current.nodeType === 1) {
    let index = 1;
    let sibling = current.previousElementSibling;
    
    while (sibling) {
      if (sibling.nodeName === current.nodeName) {
        index++;
      }
      sibling = sibling.previousElementSibling;
    }
    
    const tagName = current.nodeName.toLowerCase();
    const xpathIndex = index > 1 ? `[${index}]` : '';
    parts.unshift(`${tagName}${xpathIndex}`);
    
    current = current.parentElement;
    
    // Dừng khi đến body hoặc html
    if (current && (current.nodeName === 'BODY' || current.nodeName === 'HTML')) {
      break;
    }
  }
  
  return '/' + parts.join('/');
}


function generateSelector(element) {
  // 1. Ưu tiên ID (ngon nhất) - luôn dùng CSS
  if (element.id && !element.id.includes('__next')) {
    return `#${element.id}`;
  }
  
  // 2. Ưu tiên Data Attributes (khá ngon) - luôn dùng CSS
  if (element.hasAttribute('data-id')) {
    return `${element.tagName.toLowerCase()}[data-id="${element.getAttribute('data-id')}"]`;
  }
  
  if (selectorTypePreference === 'xpath') {
    // Try to create XPath based on label text (more robust)
    const labelInfo = findLabelSibling(element);
    if (labelInfo) {
      const safeLabel = labelInfo.text.replace(/"/g, '\\"');
      const labelTag = labelInfo.element.tagName.toLowerCase();
      const valueTag = element.tagName.toLowerCase();
      
      // Get classes for better matching
      const labelClasses = labelInfo.element.className && typeof labelInfo.element.className === 'string'
        ? labelInfo.element.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
        : [];
      const valueClasses = element.className && typeof element.className === 'string'
        ? element.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
        : [];
      
      // Check if they are siblings in same parent
      const parent = element.parentElement;
      if (parent && labelInfo.element.parentElement === parent) {
        const allChildren = Array.from(parent.children);
        const labelIndex = allChildren.indexOf(labelInfo.element);
        const valueIndex = allChildren.indexOf(element);
        
        if (labelIndex >= 0 && valueIndex > labelIndex) {
          const parentTag = parent.tagName.toLowerCase();
          const parentClasses = parent.className && typeof parent.className === 'string'
            ? parent.className.trim().split(/\s+/).filter(c => c && c.length > 3 && !c.includes('scraper-'))
            : [];
          
          // Build XPath with classes if available
          let labelFilter = '';
          if (labelClasses.length > 0) {
            const labelClassFilter = labelClasses.map(c => `contains(@class, "${c}")`).join(' and ');
            labelFilter = `[${labelClassFilter}]`;
          }
          
          let valueFilter = '';
          if (valueClasses.length > 0) {
            const valueClassFilter = valueClasses.map(c => `contains(@class, "${c}")`).join(' and ');
            valueFilter = `[${valueClassFilter}]`;
          }
          
          // Try with parent class first
          if (parentClasses.length > 0) {
            const parentClassFilter = parentClasses.map(c => `contains(@class, "${c}")`).join(' and ');
            const xpath = `//${parentTag}[${parentClassFilter}]/${labelTag}${labelFilter}[contains(text(), "${safeLabel}")]/following-sibling::${valueTag}${valueFilter}[${valueIndex - labelIndex}]`;
            const testResult = getElementByXPath(xpath);
            if (testResult === element) {
              return xpath;
            }
          }
          
          // Try simpler version
          const xpath = `//${parentTag}/${labelTag}${labelFilter}[contains(text(), "${safeLabel}")]/following-sibling::${valueTag}${valueFilter}[${valueIndex - labelIndex}]`;
          const testResult = getElementByXPath(xpath);
          if (testResult === element) {
            return xpath;
          }
          
          // Try even simpler (just following-sibling)
          if (valueIndex - labelIndex === 1) {
            const simpleXpath = `//${labelTag}${labelFilter}[contains(text(), "${safeLabel}")]/following-sibling::${valueTag}${valueFilter}[1]`;
            const testResult2 = getElementByXPath(simpleXpath);
            if (testResult2 === element) {
              return simpleXpath;
            }
          }
        }
      }
    } else {
      // 2. Nếu không thấy label sibling, thử tìm label trong container (Uncle Strategy)
      // Cấu trúc: Value Element (strong) <-- trong container --> Label Container (div.a4ep88f) > Label (span)
      let container = element.parentElement; // div.abzctes hoặc div.col-xs-6
      if (container) {
        // Tìm label span - ưu tiên tìm trong div.a4ep88f hoặc span không nằm trong strong
        let labelSpan = null;
        
        // Ưu tiên 1: Tìm trong div.a4ep88f (label container)
        const labelContainer = container.querySelector('div.a4ep88f, [class*="a4ep88f"]');
        if (labelContainer) {
          labelSpan = Array.from(labelContainer.querySelectorAll('span')).find(s => {
            let t = extractText(s).trim();
            // Text ngắn, không phải số, không rỗng, không phải value
            return t.length > 0 && t.length < 50 && !t.match(/^\d/) && !t.match(/^\d+[.,]\d+/);
          });
        }
        
        // Ưu tiên 2: Tìm tất cả span trong container, loại bỏ span nằm trong strong
        if (!labelSpan) {
          labelSpan = Array.from(container.querySelectorAll('span')).find(s => {
            // Bỏ qua span nằm trong strong (có thể là value)
            if (s.closest('strong')) return false;
            let t = extractText(s).trim();
            // Text ngắn, không phải số, không rỗng
            return t.length > 0 && t.length < 50 && !t.match(/^\d/) && !t.match(/^\d+[.,]\d+/);
          });
        }

        if (labelSpan) {
          let labelText = extractText(labelSpan).trim();
          let safeLabel = labelText.replace(/"/g, '\\"');
          
          // Lấy class của container để tạo selector chính xác hơn
          let containerClasses = container.className && typeof container.className === 'string'
            ? container.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
            : [];
          
          // Lấy tag name của element value
          let valueTag = element.tagName.toLowerCase();
          
          // Lấy class của value element nếu có
          let valueClasses = element.className && typeof element.className === 'string'
            ? element.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
            : [];
          
          // Ưu tiên 1: Dùng itemprop để tạo selector chính xác cho từng trường riêng lẻ
          if (element.hasAttribute('itemprop')) {
            const itemprop = element.getAttribute('itemprop');
            
            // Thử với container class + itemprop (chính xác nhất)
            if (containerClasses.length > 0) {
              const containerClassFilter = containerClasses.map(c => `contains(@class, "${c}")`).join(' and ');
              // XPath: tìm span chứa label, leo lên container cụ thể, rồi tìm strong với itemprop chính xác
              const xpathWithItemprop = `//span[contains(normalize-space(), "${safeLabel}")]/ancestor::div[${containerClassFilter}]//${valueTag}[@itemprop="${itemprop}"]`;
              const allMatches = getElementsByXPath(xpathWithItemprop);
              // Chỉ dùng nếu match đúng 1 element và đó là element hiện tại
              if (allMatches.length === 1 && allMatches[0] === element) {
                return xpathWithItemprop;
              }
            }
            
            // Thử version đơn giản hơn: chỉ dùng label + itemprop (không cần container class)
            const xpathItemprop = `//span[contains(normalize-space(), "${safeLabel}")]/ancestor::div//${valueTag}[@itemprop="${itemprop}"]`;
            const allMatches2 = getElementsByXPath(xpathItemprop);
            // Chỉ dùng nếu match đúng 1 element
            if (allMatches2.length === 1 && allMatches2[0] === element) {
              return xpathItemprop;
            }
            
            // Thử version đơn giản nhất: chỉ dùng itemprop (nếu itemprop là unique)
            const xpathItempropOnly = `//${valueTag}[@itemprop="${itemprop}"]`;
            const allWithItemprop = getElementsByXPath(xpathItempropOnly);
            // Chỉ dùng nếu itemprop là unique (chỉ có 1 element)
            if (allWithItemprop.length === 1 && allWithItemprop[0] === element) {
              return xpathItempropOnly;
            }
          }
          
          // Ưu tiên 2: Dùng container class + value class (nếu không có itemprop)
          if (containerClasses.length > 0) {
            const containerClassFilter = containerClasses.map(c => `contains(@class, "${c}")`).join(' and ');
            let valueFilter = '';
            if (valueClasses.length > 0) {
              const valueClassFilter = valueClasses.map(c => `contains(@class, "${c}")`).join(' and ');
              valueFilter = `[${valueClassFilter}]`;
            }
            
            // XPath: tìm span chứa label, leo lên container, rồi tìm strong bên trong
            const xpath = `//span[contains(normalize-space(), "${safeLabel}")]/ancestor::div[${containerClassFilter}]//${valueTag}${valueFilter}`;
            const allMatches = getElementsByXPath(xpath);
            // Chỉ dùng nếu match đúng 1 element
            if (allMatches.length === 1 && allMatches[0] === element) {
              return xpath;
            }
          }
          
          // Ưu tiên 3: Version đơn giản (không cần container class)
          let valueFilter = '';
          if (valueClasses.length > 0) {
            const valueClassFilter = valueClasses.map(c => `contains(@class, "${c}")`).join(' and ');
            valueFilter = `[${valueClassFilter}]`;
          }
          
          const simpleXpath = `//span[contains(normalize-space(), "${safeLabel}")]/ancestor::div//${valueTag}${valueFilter}`;
          const allMatches4 = getElementsByXPath(simpleXpath);
          // Chỉ dùng nếu match đúng 1 element
          if (allMatches4.length === 1 && allMatches4[0] === element) {
            return simpleXpath;
          }
        }
      }
    }
    
    // Fallback 1: Tạo XPath dựa trên itemprop (nếu có) - chính xác cho từng trường
    if (element.hasAttribute('itemprop')) {
      const itemprop = element.getAttribute('itemprop');
      const valueTag = element.tagName.toLowerCase();
      
      // Thử với itemprop + class nếu có
      if (element.className && typeof element.className === 'string') {
        const valueClasses = element.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'));
        if (valueClasses.length > 0) {
          const valueClassFilter = valueClasses.map(c => `contains(@class, "${c}")`).join(' and ');
          const xpathWithClass = `//${valueTag}[@itemprop="${itemprop}" and ${valueClassFilter}]`;
          const testResult = getElementByXPath(xpathWithClass);
          if (testResult === element) {
            return xpathWithClass;
          }
        }
      }
      
      // Thử chỉ với itemprop
      const xpathItemprop = `//${valueTag}[@itemprop="${itemprop}"]`;
      const allWithItemprop = getElementsByXPath(xpathItemprop);
      // Chỉ dùng nếu itemprop là unique (chỉ có 1 element)
      if (allWithItemprop.length === 1 && allWithItemprop[0] === element) {
        return xpathItemprop;
      }
      
      // Nếu có nhiều element với cùng itemprop, thêm context từ parent
      const container = element.parentElement;
      if (container && container.className && typeof container.className === 'string') {
        const containerClasses = container.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'));
        if (containerClasses.length > 0) {
          const containerClassFilter = containerClasses.map(c => `contains(@class, "${c}")`).join(' and ');
          const xpathWithContainer = `//div[${containerClassFilter}]//${valueTag}[@itemprop="${itemprop}"]`;
          const allMatchesContainer = getElementsByXPath(xpathWithContainer);
          // Chỉ dùng nếu match đúng 1 element
          if (allMatchesContainer.length === 1 && allMatchesContainer[0] === element) {
            return xpathWithContainer;
          }
        }
      }
    }
    
    // Fallback 2: Tạo XPath dựa trên data-testid hoặc data attributes
    if (element.hasAttribute('data-testid')) {
      const dataTestId = element.getAttribute('data-testid');
      const valueTag = element.tagName.toLowerCase();
      const xpathDataTestId = `//${valueTag}[@data-testid="${dataTestId}"]`;
      const allMatchesDataTestId = getElementsByXPath(xpathDataTestId);
      // Chỉ dùng nếu match đúng 1 element
      if (allMatchesDataTestId.length === 1 && allMatchesDataTestId[0] === element) {
        return xpathDataTestId;
      }
    }
    
    // Fallback 3: Absolute XPath (chính xác nhất nhưng dài)
    const absoluteXPath = getAbsoluteXPath(element);
    if (absoluteXPath) {
      // Verify absolute XPath works
      const testResult = getElementByXPath(absoluteXPath);
      if (testResult === element) {
        return absoluteXPath;
      }
    }
    
    // Fallback 4: Tạo XPath dựa trên text content (nếu text ngắn và unique)
    const textContent = extractText(element).trim();
    if (textContent && textContent.length > 0 && textContent.length < 100) {
      const valueTag = element.tagName.toLowerCase();
      const safeText = textContent.replace(/"/g, '\\"').substring(0, 50);
      const xpathText = `//${valueTag}[contains(normalize-space(), "${safeText}")]`;
      const allWithText = getElementsByXPath(xpathText);
      // Chỉ dùng nếu text là unique
      if (allWithText.length === 1 && allWithText[0] === element) {
        return xpathText;
      }
    }
    
    // Nếu vẫn không tạo được XPath, fall through to CSS (nhưng nên tránh)
  }
  
  // CSS Selector logic (mặc định hoặc fallback)
  const elementClasses = element.className && typeof element.className === 'string'
    ? element.className.trim().split(/\s+/).filter(c => c && c.length > 5)
    : [];
  
  const distinctiveClass = elementClasses.find(c => 
    c.startsWith('js__') || 
    c.startsWith('re__') || 
    c.length > 4
  );
  
  if (distinctiveClass) {
    const classSelector = `.${distinctiveClass}`;
    const matches = document.querySelectorAll(classSelector);
    if (matches.length === 1 && matches[0] === element) {
      return classSelector;
    }
  }
  
  // Fallback: Dùng CSS Selector với logic kiểm tra unique
  return getUniqueCssPath(element);
}

// Hàm phụ trợ: Tạo CSS Path duy nhất - đơn giản hóa
function getUniqueCssPath(el) {
    if (!(el instanceof Element)) return '';
    const path = [];
    let maxDepth = 0;
    const startElement = el;
    
    while (el.nodeType === Node.ELEMENT_NODE && maxDepth < 6) {
        let selector = el.nodeName.toLowerCase();
        
        if (el.id && !el.id.includes('__next')) {
            selector = '#' + el.id;
            path.unshift(selector);
            break;
        } else if (el.className && typeof el.className === 'string') {
            const classes = el.className.trim().split(/\s+/);
            const distinctiveClass = classes.find(c => 
                c && c.length > 4 && 
                !c.includes('active') && !c.includes('selected') && 
                (c.startsWith('js__') || c.startsWith('re__') || c.includes('-'))
            );
            if (distinctiveClass) {
                selector = `.${distinctiveClass}`;
            }
        }
        
        path.unshift(selector);
        const currentPath = path.join(' > ');
        const matches = document.querySelectorAll(currentPath);
        
        if (matches.length === 1 && matches[0] === startElement) {
            break;
        }
        
        el = el.parentNode;
        maxDepth++;
    }
    
    return path.join(" > ");
}

// Get full path selector
function getFullSelector(element) {
  const path = [];
  while (element && element.nodeType === Node.ELEMENT_NODE) {
    let selector = element.nodeName.toLowerCase();
    if (element.id) {
      selector += `#${element.id}`;
      path.unshift(selector);
      break;
    } else {
      const parent = element.parentElement;
      if (parent) {
        const siblings = Array.from(parent.children);
        const index = siblings.indexOf(element) + 1;
        selector += `:nth-of-type(${index})`;
      }
      path.unshift(selector);
    }
    element = element.parentElement;
  }
  return path.join(' > ');
}

// Create highlight overlay
function createHighlightOverlay() {
  if (highlightOverlay) return;
  
  highlightOverlay = document.createElement('div');
  highlightOverlay.id = 'scraper-highlight';
  highlightOverlay.style.cssText = `
    position: fixed !important;
    border: 2px solid #4CAF50 !important;
    background: rgba(76, 175, 80, 0.1) !important;
    pointer-events: none !important;
    z-index: 999999 !important;
    display: none !important;
    box-sizing: border-box !important;
    margin: 0 !important;
    padding: 0 !important;
  `;
  document.body.appendChild(highlightOverlay);
}

// Show highlight on element
function highlightElement(element) {
  if (!highlightOverlay) createHighlightOverlay();
  
  // Skip if element is the overlay itself or its children
  if (element === highlightOverlay || highlightOverlay.contains(element)) {
    return;
  }
  
  // Get bounding rect relative to viewport (for fixed positioning)
  const rect = element.getBoundingClientRect();
  
  // Update overlay position and size
  highlightOverlay.style.display = 'block';
  highlightOverlay.style.left = rect.left + 'px';
  highlightOverlay.style.top = rect.top + 'px';
  highlightOverlay.style.width = rect.width + 'px';
  highlightOverlay.style.height = rect.height + 'px';
}

// Hide highlight
function hideHighlight() {
  if (highlightOverlay) {
    highlightOverlay.style.display = 'none';
  }
}

// Extract text from element
function extractText(element) {
  // Remove script and style elements
  const clone = element.cloneNode(true);
  const scripts = clone.querySelectorAll('script, style');
  scripts.forEach(s => s.remove());
  
  return clone.textContent.trim();
}

function parseExcludeWords(input) {
  if (!input || typeof input !== 'string') return [];
  return input
    .split(/[|,\\n]/)
    .map(t => t.trim())
    .filter(t => t.length > 0);
}

function applyExcludeWords(text, field) {
  if (!text || typeof text !== 'string' || !field || !field.excludeWords) return text;
  const terms = parseExcludeWords(field.excludeWords);
  if (terms.length === 0) return text;
  let result = text;
  terms.forEach(term => {
    const escaped = term.replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&');
    result = result.replace(new RegExp(escaped, 'gi'), '');
  });
  return result.replace(/\\s+/g, ' ').trim();
}

// Extract label and value from text (e.g., "Giá/m²133,17 triệu/m²" -> label: "Giá/m²", value: "133,17 triệu/m²")
function extractLabelAndValue(text) {
  // Pattern: LabelValue (no space between)
  const match = text.match(/^([^0-9]+?)(\d+.*)$/);
  if (match) {
    return { label: match[1].trim(), value: match[2].trim() };
  }
  
  // Pattern: "Label: Value" or "Label - Value"
  const colonMatch = text.match(/^([^:]+?):\s*(.+)$/);
  if (colonMatch) {
    return { label: colonMatch[1].trim(), value: colonMatch[2].trim() };
  }
  
  return null;
}

// Generate smart field name based on element context
function generateFieldName(element, text) {
  const tagName = element.tagName.toLowerCase();
  const textLower = text.toLowerCase().trim();
  
  // Try to extract label and value first
  const labelValue = extractLabelAndValue(text);
  let baseName = '';
  
  if (labelValue) {
    baseName = labelValue.label;
    // Clean up label
    baseName = baseName.replace(/\s+/g, ' ').trim();
  } else {
    baseName = text.replace(/\s+/g, ' ').trim();
  }
  
  // Use cleaned text if still no name
  if (!baseName || baseName.length === 0) {
    const cleaned = text.replace(/\s+/g, ' ').trim();
    if (cleaned.length <= 100) {
      baseName = cleaned;
    } else {
      baseName = cleaned.substring(0, 100) + '...';
    }
  } else {
    // Giới hạn 100 ký tự cho title
    if (baseName.length > 100) {
      baseName = baseName.substring(0, 100) + '...';
    }
  }
  
  // Check for duplicate names and add suffix if needed
  const existingNames = selectedFields.map(f => f.name);
  let finalName = baseName;
  let counter = 1;
  
  while (existingNames.includes(finalName)) {
    counter++;
    finalName = `${baseName} (${counter})`;
  }
  
  return finalName;
}

// Extract data from element based on field config
function extractFieldData(element, field) {
  const valueType = field.valueType || 'text';
  const selector = field.customSelector || field.selector;
  
  let value = null;
  
  // Extract value based on valueType
  switch (valueType) {
    case 'html':
      value = element.innerHTML.trim();
      break;
    case 'src':
      let src = element.getAttribute('data-src') || 
                element.getAttribute('src') || 
                element.getAttribute('data-lazy-src') || 
                element.getAttribute('data-original') || 
                null;
      if (!src && element.src && typeof element.src === 'string') {
        if (element.src.startsWith('http://') || element.src.startsWith('https://')) {
          src = element.src;
        }
      }
      if (src && (src.startsWith('data:') || src.startsWith('blob:') || src.includes('.svg'))) {
        src = null;
      }
      value = src;
      break;
    case 'href':
      value = element.href || element.getAttribute('href') || null;
      break;
    case 'alt':
      value = element.alt || element.getAttribute('alt') || null;
      break;
    case 'title':
      value = element.title || element.getAttribute('title') || null;
      break;
    case 'data-id':
      value = element.getAttribute('data-id') || null;
      break;
    case 'data-phone':
      value = element.getAttribute('data-phone') || null;
      break;
    case 'innerText':
      value = element.innerText.trim();
      break;
    case 'text':
    default:
      value = extractText(element);
      break;
  }
  
  if (typeof value === 'string' && (valueType === 'text' || valueType === 'innerText')) {
    value = applyExcludeWords(value, field);
  }
  
  const data = {
    value: value,
    selector: selector,
    valueType: valueType
  };
  
  // Also include other common attributes for reference
  if (element.hasAttribute('href')) {
    data.href = element.getAttribute('href');
  }
  if (element.hasAttribute('src')) {
    data.src = element.getAttribute('src');
  }
  if (element.hasAttribute('alt')) {
    data.alt = element.getAttribute('alt');
  }
  
  return data;
}

// Generate selector for ALL similar elements (for Item Link in Listing Mode)
// Get target link element - auto-climb to anchor tag
function getTargetLinkElement(clickedElement) {
  // 1. Look Up: Check closest <a> tag
  const closestA = clickedElement.closest('a[href]');
  if (closestA) return closestA;
  
  // 2. Look Down: Check if it's a container holding a title link
  const childTitleLink = clickedElement.querySelector('h3 a, h2 a, h1 a, .title a, [class*="title"] a, a[class*="title"]');
  if (childTitleLink) return childTitleLink;
  
  // 3. Look for any direct child link
  const directLink = clickedElement.querySelector('a[href]');
  if (directLink) return directLink;
  
  return clickedElement; // Fallback
}

// Generate pagination selector (Next Page) - avoids numbers, uses icons/position
// Updated to match nodriver strategy: find icon first, then find parent <a>
function generatePaginationSelector(element) {
  const tagName = element.tagName.toLowerCase();
  const classes = element.className && typeof element.className === 'string'
    ? element.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
    : [];
  
  // Từ khóa để nhận diện icon (dùng chung cho tất cả strategies)
  const iconKeywords = ['chevron', 'next', 'right', 'arrow', 'icon', 'forward', 'navigate', 'caret'];
  
  // Strategy 0 (NEW - nodriver method): Tìm icon và parent <a> một cách tổng quát
  // Logic: Icon thường nằm trong thẻ <a>, khi click icon thì event bubble lên <a>
  // Nên tốt nhất là tìm icon trước, sau đó tìm parent <a> để click
  
  // Hàm kiểm tra xem element có phải là icon không (dựa trên class hoặc tag)
  function isIconElement(el) {
    if (!el) return false;
    const elTag = el.tagName.toLowerCase();
    // Các tag thường dùng cho icon
    if (['i', 'svg', 'span'].includes(elTag)) {
      const elClasses = el.className && typeof el.className === 'string'
        ? el.className.trim().split(/\s+/).filter(c => c && c.length > 2)
        : [];
      // Kiểm tra xem có class chứa từ khóa icon không
      return elClasses.some(cls => 
        iconKeywords.some(keyword => cls.toLowerCase().includes(keyword))
      );
    }
    return false;
  }
  
  // Hàm tìm icon trong element hoặc parent
  function findIconElement(el) {
    if (!el) return null;
    // Nếu chính element là icon
    if (isIconElement(el)) return el;
    // Nếu element chứa icon (querySelector)
    const childIcon = el.querySelector('i, svg, span');
    if (childIcon && isIconElement(childIcon)) return childIcon;
    // Nếu element nằm trong icon (closest)
    const parentIcon = el.closest('i, svg, span');
    if (parentIcon && isIconElement(parentIcon)) return parentIcon;
    return null;
  }
  
  // Kiểm tra xem có liên quan đến icon không
  const iconEl = findIconElement(element);
  
  if (iconEl) {
    // Tìm thẻ <a> cha chứa icon (đây là element cần click)
    let targetLink = null;
    
    if (iconEl === element) {
      // Nếu element chính là icon, tìm thẻ <a> cha
      targetLink = element.closest('a[href]');
    } else if (tagName === 'a' && iconEl.parentElement === element) {
      // Nếu element là thẻ <a> có chứa icon, dùng chính nó
      targetLink = element;
    } else {
      // Nếu element nằm trong icon, tìm thẻ <a> cha
      targetLink = iconEl.closest('a[href]');
    }
    
    if (targetLink) {
      // Lấy class của icon và link để tạo selector tổng quát
      const iconClasses = iconEl.className && typeof iconEl.className === 'string'
        ? iconEl.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
        : [];
      const linkClasses = targetLink.className && typeof targetLink.className === 'string'
        ? targetLink.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
        : [];
      
      // Tìm class đặc trưng của icon (chứa từ khóa icon)
      const iconClass = iconClasses.find(cls => 
        iconKeywords.some(keyword => cls.toLowerCase().includes(keyword))
      );
      
      // Tìm class đặc trưng của link (thường có pagination, pager, page-nav, etc.)
      const linkClass = linkClasses.find(cls => 
        ['pagination', 'pager', 'page', 'nav'].some(keyword => cls.toLowerCase().includes(keyword))
      ) || linkClasses[0]; // Fallback về class đầu tiên
      
      // Tạo selector cho thẻ <a> có chứa icon
      if (selectorTypePreference === 'xpath') {
        if (iconClass && linkClass) {
          // XPath: tìm thẻ a có class và chứa icon có class
          return `//a[contains(@class, "${linkClass}")][.//*[contains(@class, "${iconClass}")]]`;
        } else if (linkClass) {
          return `//a[contains(@class, "${linkClass}")]`;
        } else {
          return `//a[.//*[contains(@class, "${iconClass}")]]`;
        }
      } else {
        // CSS selector: dùng :has() nếu có class icon, fallback về class link
        if (iconClass && linkClass) {
          try {
            // Test xem :has() có hoạt động không
            const testSelector = `a.${linkClass}:has(.${iconClass})`;
            const testMatch = document.querySelector(testSelector);
            if (testMatch && testMatch === targetLink) {
              return testSelector;
            }
          } catch (e) {
            // :has() không được hỗ trợ, dùng fallback
          }
          // Fallback: dùng class của link (nodriver sẽ tự tìm icon bên trong)
          return linkClass ? `a.${linkClass}` : `a:has(.${iconClass})`;
        } else if (linkClass) {
          return `a.${linkClass}`;
        } else if (iconClass) {
          // Nếu chỉ có class icon, trả về selector của icon (nodriver sẽ click icon)
          return `.${iconClass}`;
        }
      }
    }
    
    // Nếu không tìm thấy parent link, trả về selector icon trực tiếp (fallback)
    const iconClasses = iconEl.className && typeof iconEl.className === 'string'
      ? iconEl.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
      : [];
    const iconClass = iconClasses.find(cls => 
      iconKeywords.some(keyword => cls.toLowerCase().includes(keyword))
    ) || iconClasses[0];
    
    if (iconClass) {
      if (selectorTypePreference === 'xpath') {
        return `//*[contains(@class, "${iconClass}")]`;
      } else {
        return `.${iconClass}`;
      }
    }
  }
  
  // Find pagination container
  let paginationContainer = element.closest('[class*="pagination"], [class*="pager"], [class*="page-nav"], nav');
  if (!paginationContainer) {
    // Look for parent with multiple similar links (pagination group)
    const parent = element.parentElement;
    if (parent) {
      const siblings = Array.from(parent.children).filter(el => 
        el.tagName.toLowerCase() === tagName && el !== element
      );
      if (siblings.length > 0) {
        paginationContainer = parent;
      }
    }
  }
  
  // Strategy 1: Icon class priority (chevron, next, right, arrow, icon)
  // iconKeywords đã được khai báo ở đầu function
  for (const className of classes) {
    const classNameLower = className.toLowerCase();
    if (iconKeywords.some(keyword => classNameLower.includes(keyword))) {
      let selector;
      if (paginationContainer && paginationContainer.className) {
        const containerClasses = paginationContainer.className.trim().split(/\s+/)
          .filter(c => c && c.length > 2 && !c.includes('scraper-'))
          .slice(0, 1); // Use first meaningful class
        if (containerClasses.length > 0) {
          if (selectorTypePreference === 'xpath') {
            selector = `//div[contains(@class, "${containerClasses[0]}")]//${tagName}[contains(@class, "${className}")]`;
          } else {
            selector = `.${containerClasses[0]} ${tagName}.${className}`;
          }
        } else {
          if (selectorTypePreference === 'xpath') {
            selector = `//${tagName}[contains(@class, "${className}")]`;
          } else {
            selector = `${tagName}.${className}`;
          }
        }
      } else {
        if (selectorTypePreference === 'xpath') {
          selector = `//${tagName}[contains(@class, "${className}")]`;
        } else {
          selector = `${tagName}.${className}`;
        }
      }
      
      // Test selector
      const matches = selectorTypePreference === 'xpath' 
        ? getElementsByXPath(selector)
        : Array.from(document.querySelectorAll(selector));
      
      if (matches.length > 0 && matches.includes(element)) {
        return selector;
      }
    }
  }
  
  // Strategy 2: Position-based (last() in pagination group)
  if (paginationContainer) {
    const containerTag = paginationContainer.tagName.toLowerCase();
    const containerClasses = paginationContainer.className && typeof paginationContainer.className === 'string'
      ? paginationContainer.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-'))
      : [];
    
    if (selectorTypePreference === 'xpath') {
      if (containerClasses.length > 0) {
        const containerClassFilter = containerClasses.map(c => `contains(@class, "${c}")`).join(' and ');
        // Try last() position
        const xpathLast = `//${containerTag}[${containerClassFilter}]//${tagName}[last()]`;
        const matchesLast = getElementsByXPath(xpathLast);
        if (matchesLast.length === 1 && matchesLast[0] === element) {
          return xpathLast;
        }
        
        // Try following-sibling::a[last()]
        const xpathFollowing = `//${containerTag}[${containerClassFilter}]//${tagName}[position()=last()]`;
        const matchesFollowing = getElementsByXPath(xpathFollowing);
        if (matchesFollowing.length === 1 && matchesFollowing[0] === element) {
          return xpathFollowing;
        }
      }
    } else {
      if (containerClasses.length > 0) {
        const cssSelector = `.${containerClasses[0]} ${tagName}:last-child`;
        const matches = Array.from(document.querySelectorAll(cssSelector));
        if (matches.length === 1 && matches[0] === element) {
          return cssSelector;
        }
      }
    }
  }
  
  // Strategy 3: Data attributes (data-page, data-action, etc.)
  const dataAttrs = Array.from(element.attributes).filter(attr => 
    attr.name.startsWith('data-') && 
    (attr.name.includes('next') || attr.name.includes('page') || attr.name.includes('action'))
  );
  
  for (const attr of dataAttrs) {
    let selector;
    if (paginationContainer && paginationContainer.className) {
      const containerClasses = paginationContainer.className.trim().split(/\s+/)
        .filter(c => c && c.length > 2 && !c.includes('scraper-'))
        .slice(0, 1);
      if (containerClasses.length > 0) {
        if (selectorTypePreference === 'xpath') {
          selector = `//div[contains(@class, "${containerClasses[0]}")]//${tagName}[@${attr.name}="${attr.value}"]`;
        } else {
          selector = `.${containerClasses[0]} ${tagName}[${attr.name}="${attr.value}"]`;
        }
      }
    }
    
    if (!selector) {
      if (selectorTypePreference === 'xpath') {
        selector = `//${tagName}[@${attr.name}="${attr.value}"]`;
      } else {
        selector = `${tagName}[${attr.name}="${attr.value}"]`;
      }
    }
    
    const matches = selectorTypePreference === 'xpath'
      ? getElementsByXPath(selector)
      : Array.from(document.querySelectorAll(selector));
    
    if (matches.length > 0 && matches.includes(element)) {
      return selector;
    }
  }
  
  // Strategy 4: Avoid text-based selectors with numbers, use class/ID instead
  // Check if element has ID
  if (element.id && !element.id.includes('__next')) {
    return selectorTypePreference === 'xpath' 
      ? `//${tagName}[@id="${element.id}"]`
      : `#${element.id}`;
  }
  
  // Strategy 5: Use class without text (avoid contains(text()) with numbers)
  if (classes.length > 0) {
    // Filter out classes that might be page-specific
    const stableClasses = classes.filter(c => 
      !/^\d+$/.test(c) && // Not just numbers
      !c.match(/^page-\d+$/) && // Not page-1, page-2, etc.
      !c.match(/^\d+-page$/) // Not 1-page, 2-page, etc.
    );
    
    if (stableClasses.length > 0) {
      const className = stableClasses[0];
      let selector;
      if (paginationContainer && paginationContainer.className) {
        const containerClasses = paginationContainer.className.trim().split(/\s+/)
          .filter(c => c && c.length > 2 && !c.includes('scraper-'))
          .slice(0, 1);
        if (containerClasses.length > 0) {
          if (selectorTypePreference === 'xpath') {
            selector = `//div[contains(@class, "${containerClasses[0]}")]//${tagName}[contains(@class, "${className}")]`;
          } else {
            selector = `.${containerClasses[0]} ${tagName}.${className}`;
          }
        }
      }
      
      if (!selector) {
        if (selectorTypePreference === 'xpath') {
          selector = `//${tagName}[contains(@class, "${className}")]`;
        } else {
          selector = `${tagName}.${className}`;
        }
      }
      
      const matches = selectorTypePreference === 'xpath'
        ? getElementsByXPath(selector)
        : Array.from(document.querySelectorAll(selector));
      
      if (matches.length > 0 && matches.includes(element)) {
        return selector;
      }
    }
  }
  
  // Fallback: Use generateSelector but filter out text-based with numbers
  const fallbackSelector = generateSelector(element);
  if (fallbackSelector) {
    // Check if selector uses contains(text()) with numbers - if so, try CSS instead
    if (fallbackSelector.includes('contains(text()') || fallbackSelector.includes('contains(normalize-space()')) {
      const text = extractText(element);
      // If text looks like a number, avoid it
      if (/^\d+[.,]?\d*$/.test(text.trim())) {
        // Use CSS fallback
        return getUniqueCssPath(element) || `${tagName}`;
      }
    }
    return fallbackSelector;
  }
  
  // Final fallback
  return getUniqueCssPath(element) || tagName;
}

function generateListingSelector(element) {
  // Strategy: Find common class/attributes that match similar elements
  // Priority: class > tag + parent class > data attributes > tag only
  
  const tagName = element.tagName.toLowerCase();
  const classes = element.className && typeof element.className === 'string'
    ? element.className.trim().split(/\s+/).filter(c => c && c.length > 2 && !c.includes('scraper-') && !c.includes('__'))
    : [];
  
  // Try to find a class that matches multiple similar elements
  let bestSelector = null;
  let maxMatches = 0;
  
  // Strategy 1: Use class selector (most common for product listings)
  for (const className of classes) {
    const cssSelector = `${tagName}.${className}`;
    const matches = document.querySelectorAll(cssSelector);
    
    if (matches.length > maxMatches && matches.length > 1) {
      // Check if clicked element is in the matches
      if (Array.from(matches).includes(element)) {
        bestSelector = cssSelector;
        maxMatches = matches.length;
      }
    }
  }
  
  // Strategy 2: Use parent container class + tag pattern
  if (!bestSelector || maxMatches < 2) {
    let current = element.parentElement;
    let depth = 0;
    while (current && depth < 3) {
      if (current.className && typeof current.className === 'string') {
        const parentClasses = current.className.trim().split(/\s+/).filter(c => 
          c && c.length > 2 && !c.includes('scraper-') && !c.includes('__')
        );
        
        for (const parentClass of parentClasses) {
          const cssSelector = `.${parentClass} ${tagName}`;
          const matches = document.querySelectorAll(cssSelector);
          
          if (matches.length > maxMatches && matches.length > 1) {
            if (Array.from(matches).includes(element)) {
              bestSelector = cssSelector;
              maxMatches = matches.length;
            }
          }
        }
      }
      current = current.parentElement;
      depth++;
    }
  }
  
  // Strategy 3: Use data attributes if available
  if (!bestSelector || maxMatches < 2) {
    const dataAttrs = Array.from(element.attributes).filter(attr => 
      attr.name.startsWith('data-') && attr.value && attr.value.length < 50
    );
    
    for (const attr of dataAttrs) {
      const cssSelector = `${tagName}[${attr.name}="${attr.value}"]`;
      const matches = document.querySelectorAll(cssSelector);
      
      if (matches.length > maxMatches && matches.length > 1) {
        if (Array.from(matches).includes(element)) {
          bestSelector = cssSelector;
          maxMatches = matches.length;
        }
      }
    }
  }
  
  // Strategy 4: For links, try href pattern matching
  if (tagName === 'a' && element.href) {
    const href = element.href;
    // Try to extract common path pattern
    try {
      const url = new URL(href);
      const pathParts = url.pathname.split('/').filter(p => p);
      if (pathParts.length >= 2) {
        // Use last 2 path parts as pattern
        const pattern = '/' + pathParts.slice(-2).join('/');
        const cssSelector = `${tagName}[href*="${pattern}"]`;
        const matches = document.querySelectorAll(cssSelector);
        
        if (matches.length > maxMatches && matches.length > 1) {
          if (Array.from(matches).includes(element)) {
            bestSelector = cssSelector;
            maxMatches = matches.length;
          }
        }
      }
    } catch (e) {
      // Invalid URL, skip
    }
  }
  
  // Fallback: Use tag only if nothing else works
  if (!bestSelector || maxMatches < 2) {
    bestSelector = tagName;
    maxMatches = document.querySelectorAll(tagName).length;
  }
  
  return { selector: bestSelector, matchCount: maxMatches };
}

// Aggressive "Snap-to-Link" logic for Listing Mode
function snapToMainLink(clickedElement) {
  // Priority 0: If clicked element IS already a link -> Return it
  if (clickedElement.tagName === 'A' && clickedElement.href) {
    return clickedElement;
  }

  // Priority 1: If clicked element is inside a link -> Return the link
  const closestA = clickedElement.closest('a[href]');
  if (closestA) return closestA;

  // Priority 2: If clicked element IS A CONTAINER (div, li, article, etc.)
  // We search INSIDE it for the "Main Title Link"
  if (['DIV', 'LI', 'ARTICLE', 'SECTION', 'TR', 'TD', 'SPAN', 'P', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6'].includes(clickedElement.tagName)) {
    // Strategy 2.1: Look for product/item link classes (most specific)
    const productLink = clickedElement.querySelector(
      'a[class*="product-link"], a[class*="item-link"], a[class*="card-link"], a[class*="listing-link"], a.js__product-link-for-product-id, a[data-product-id], a[data-item-id]'
    );
    if (productLink && productLink.href) return productLink;
    
    // Strategy 2.2: Look for common title patterns
    const titleLink = clickedElement.querySelector(
      'h3 a[href], h2 a[href], h4 a[href], h1 a[href], .title a[href], [class*="title"] a[href], a[class*="title"][href]'
    );
    if (titleLink) return titleLink;
    
    // Strategy 2.3: Look for link with meaningful text content (prefer longer text)
    const allLinks = Array.from(clickedElement.querySelectorAll('a[href]'));
    if (allLinks.length > 0) {
      // Sort by text length (longer = more likely to be the main link)
      const sortedLinks = allLinks
        .filter(link => link.innerText.trim().length > 5)
        .sort((a, b) => b.innerText.trim().length - a.innerText.trim().length);
      
      if (sortedLinks.length > 0) {
        return sortedLinks[0];
      }
      
      // If no link with text, return first link with href
      return allLinks[0];
    }
  }

  // Priority 3: If clicked element is a text node or inside text, climb up to find container
  // Then search that container for links (AGGRESSIVE SEARCH)
  let current = clickedElement;
  for (let i = 0; i < 10 && current && current !== document.body; i++) {
    // Check if current element itself is a link
    if (current.tagName === 'A' && current.href) {
      return current;
    }
    
    // Check if current is a container with links
    if (['DIV', 'LI', 'ARTICLE', 'SECTION', 'SPAN', 'P', 'H1', 'H2', 'H3', 'H4', 'H5', 'H6'].includes(current.tagName)) {
      // First try product link
      const productLink = current.querySelector('a[class*="product-link"], a[class*="item-link"], a.js__product-link-for-product-id, a[data-product-id]');
      if (productLink && productLink.href) return productLink;
      
      // Then try any link with href
      const anyLink = current.querySelector('a[href]');
      if (anyLink) return anyLink;
    }
    
    current = current.parentElement;
  }

  return clickedElement; // If no link found, keep original
}

// Handle listing mode click
function handleListingClick(e) {
  if (!isSelecting || currentMode !== 'listing' || !listingSelectionType) return;
  
  e.preventDefault();
  e.stopPropagation();
  
  const element = e.target;
  
  // Skip if clicking on scraper UI elements
  if (element.closest('#scraper-highlight') || element.id === 'listing-selection-notification') {
    return;
  }
  
  // Remove notification
  const notification = document.getElementById('listing-selection-notification');
  if (notification) notification.remove();
  
  if (listingSelectionType === 'itemLink') {
    // Handle text node: if element is a text node, get its parent
    let actualElement = element;
    if (element.nodeType === Node.TEXT_NODE) {
      actualElement = element.parentElement;
    }
    
    // Đơn giản hóa: Tìm thẻ <a> có href đầu tiên
    let targetElement = null;
    
    // Nếu element đã là thẻ <a> có href
    if (actualElement.tagName === 'A' && actualElement.href) {
      targetElement = actualElement;
    } else {
      // Tìm thẻ <a> có href đầu tiên trong element hoặc container
      // 1. Tìm trong element hiện tại
      const linkInElement = actualElement.querySelector('a[href]');
      if (linkInElement) {
        targetElement = linkInElement;
      } else {
        // 2. Tìm trong parent elements (climb up)
        let container = actualElement;
        for (let i = 0; i < 10 && container && container !== document.body; i++) {
          const linkInContainer = container.querySelector('a[href]');
          if (linkInContainer) {
            targetElement = linkInContainer;
            break;
          }
          container = container.parentElement;
        }
        
        // 3. Fallback: Tìm closest <a>
        if (!targetElement) {
          targetElement = actualElement.closest('a[href]');
        }
      }
    }
    
    // Nếu vẫn không tìm thấy, dùng element gốc
    if (!targetElement) {
      targetElement = actualElement;
    }
    
    // Đơn giản hóa: CHỈ CẦN 1 CLICK - Tìm thẻ <a> và lấy class của nó
    // Đảm bảo targetElement là thẻ <a> có href
    let linkElement = targetElement;
    if (targetElement.tagName !== 'A' || !targetElement.href) {
      linkElement = targetElement.querySelector('a[href]') || targetElement.closest('a[href]');
    }
    
    // Nếu không tìm thấy thẻ <a>, báo lỗi
    if (!linkElement || linkElement.tagName !== 'A' || !linkElement.href) {
      const notification = document.getElementById('listing-selection-notification');
      if (notification) {
        notification.textContent = '❌ Không tìm thấy thẻ <a> có href trong item này.';
        notification.style.background = '#f44336';
      }
      return;
    }
    
    // Highlight thẻ <a> đã chọn
    linkElement.classList.add('scraper-field-selected');
    linkElement.style.outline = '3px solid #4CAF50';
    linkElement.style.outlineOffset = '2px';
    
    // Lấy class của thẻ <a> (không phải scraper-)
    let classSelector = null; // Class selector để gửi về sidepanel (có dấu chấm . ở đầu)
    let selector = null; // Selector đầy đủ để query (có a. prefix)
    
    if (linkElement.className) {
      const classes = linkElement.className.trim().split(/\s+/).filter(c => 
        c && c.length > 2 && !c.includes('scraper-')
      );
      if (classes.length > 0) {
        // Lấy class dài nhất (thường là class chính, vd: js__product-link-for-product-id)
        const mainClass = classes.sort((a, b) => b.length - a.length)[0];
        classSelector = `.${mainClass}`; // CSS class selector (có dấu chấm . ở đầu)
        selector = `a.${mainClass}`; // Selector đầy đủ để query
      }
    }
    
    // Nếu không có class, dùng tag selector
    if (!selector) {
      let parent = linkElement.parentElement;
      for (let i = 0; i < 5 && parent && parent !== document.body; i++) {
        if (parent.className && typeof parent.className === 'string') {
          const parentClasses = parent.className.trim().split(/\s+/).filter(c =>
            c && c.length > 2 && !c.includes('scraper-')
          );
          if (parentClasses.length > 0) {
            const parentClass = parentClasses[0];
            classSelector = `.${parentClass} a`;
            selector = `.${parentClass} a`;
            break;
          }
        }
        parent = parent.parentElement;
      }
    }

    if (!selector) {
      classSelector = 'a[href]';
      selector = 'a[href]';
    }
    
    // Find all matches with selector
    const matches = selector.startsWith('//')
      ? getElementsByXPath(selector)
      : Array.from(document.querySelectorAll(selector));
    
    // Remove highlight sau 1 giây
    setTimeout(() => {
      linkElement.classList.remove('scraper-field-selected');
      linkElement.style.outline = '';
      linkElement.style.outlineOffset = '';
    }, 1000);
    
    // Highlight tất cả matches trong 1 giây
    matches.forEach(match => {
      match.classList.add('scraper-field-selected');
      match.style.outline = '3px solid #4CAF50';
      match.style.outlineOffset = '2px';
      setTimeout(() => {
        match.classList.remove('scraper-field-selected');
        match.style.outline = '';
        match.style.outlineOffset = '';
      }, 1000);
    });
    
    // Extract URLs for preview
    const previewUrls = [];
    matches.slice(0, 10).forEach(match => {
      let url = '';
      if (match.tagName.toLowerCase() === 'a' && match.href) {
        url = match.href;
      } else {
        const linkTag = match.querySelector('a[href]');
        if (linkTag) {
          url = linkTag.href;
        }
      }
      if (url) {
        previewUrls.push(url);
      }
    });
    
    // Get preview URL từ linkElement đã chọn
    const previewUrl = linkElement.href || '';
    
    // Remove notification
    const notification = document.getElementById('listing-selection-notification');
    if (notification) notification.remove();
    
    // Send completion result (gửi classSelector - chỉ class name, không có a. prefix)
    chrome.runtime.sendMessage({
      action: 'listingSelectionComplete',
      selectionType: 'itemLink',
      step: 'complete',
      selector: classSelector, // Chỉ gửi class name, không có a. prefix
      matchedCount: matches.length,
      previewUrl: previewUrl,
      previewUrls: previewUrls
    });
    
    // Reset
    firstElement = null;
    secondElement = null;
    
    // Clean up
    isSelecting = false;
    listingSelectionType = null;
    document.removeEventListener('click', handleListingClick, true);
    document.removeEventListener('mousemove', handleMouseMove);
    document.body.style.cursor = '';
    document.body.classList.remove('scraper-selecting');
    hideHighlight(); 
    
  } else if (listingSelectionType === 'nextPage') {
    // Generate pagination selector (avoids numbers, uses icons/position)
    let selector = generatePaginationSelector(element);
    if (!selector || selector.trim() === '') {
      selector = getUniqueCssPath(element) || element.tagName.toLowerCase();
    }
    
    // Verify selector matches the element
    const matches = selector.startsWith('//')
      ? getElementsByXPath(selector)
      : Array.from(document.querySelectorAll(selector));
    
    if (matches.length > 0 && matches.includes(element)) {
      // Highlight only this button
      element.classList.add('scraper-field-selected');
      element.style.outline = '3px solid #4CAF50';
      element.style.outlineOffset = '2px';
      
      // Send result to sidepanel
      chrome.runtime.sendMessage({
        action: 'listingSelectionComplete',
        selectionType: 'nextPage',
        selector: selector,
        matchedCount: 1
      });
    } else {
      // Selector doesn't match, try fallback
      const fallbackSelector = getUniqueCssPath(element) || element.tagName.toLowerCase();
      element.classList.add('scraper-field-selected');
      element.style.outline = '3px solid #4CAF50';
      element.style.outlineOffset = '2px';
      
      chrome.runtime.sendMessage({
        action: 'listingSelectionComplete',
        selectionType: 'nextPage',
        selector: fallbackSelector,
        matchedCount: 1
      });
    }

  } else if (listingSelectionType === 'nextPageV1') {
    const selector = "nav.page li[aria-current='page'] + li a";
    const matches = Array.from(document.querySelectorAll(selector));

    // Highlight only this button
    element.classList.add('scraper-field-selected');
    element.style.outline = '3px solid #4CAF50';
    element.style.outlineOffset = '2px';

    chrome.runtime.sendMessage({
      action: 'listingSelectionComplete',
      selectionType: 'nextPageV1',
      selector: selector,
      matchedCount: matches.length || 1
    });

  } else if (listingSelectionType === 'nextLi') {
    let selector = null;
    let matches = [];
    const ul = element.closest('ul');
    if (ul) {
      selector = `${getUniqueCssPath(ul)} li:last-child a`;
      matches = Array.from(document.querySelectorAll(selector));
    }
    if (!matches || matches.length === 0) {
      selector = getUniqueCssPath(element) || element.tagName.toLowerCase();
      matches = Array.from(document.querySelectorAll(selector));
    }
    element.classList.add('scraper-field-selected');
    element.style.outline = '3px solid #4CAF50';
    element.style.outlineOffset = '2px';
    chrome.runtime.sendMessage({
      action: 'listingSelectionComplete',
      selectionType: 'nextLi',
      selector: selector,
      matchedCount: matches.length || 1
    });

  } else if (listingSelectionType === 'nextLastPagination') {
    let selector = null;
    let matches = [];
    let container = element.closest('[class*="pagination"], [class*="page"], nav');
    if (!container) {
      container = document.querySelector('[class*="pagination"], [class*="page"], nav');
    }
    if (container) {
      const liLast = container.querySelector('li:last-child a');
      if (liLast) {
        selector = `${getUniqueCssPath(container)} li:last-child a`;
        matches = Array.from(document.querySelectorAll(selector));
      } else {
        selector = `${getUniqueCssPath(container)} a:last-of-type`;
        matches = Array.from(document.querySelectorAll(selector));
      }
    }
    if (!matches || matches.length === 0) {
      selector = getUniqueCssPath(element) || element.tagName.toLowerCase();
      matches = Array.from(document.querySelectorAll(selector));
    }
    element.classList.add('scraper-field-selected');
    element.style.outline = '3px solid #4CAF50';
    element.style.outlineOffset = '2px';
    chrome.runtime.sendMessage({
      action: 'listingSelectionComplete',
      selectionType: 'nextLastPagination',
      selector: selector,
      matchedCount: matches.length || 1
    });
  }
  
  // Clean up (only for nextPage, itemLink handles its own cleanup)
  if (listingSelectionType === 'nextPage' || listingSelectionType === 'nextPageV1' || listingSelectionType === 'nextLi' || listingSelectionType === 'nextLastPagination') {
    isSelecting = false;
    listingSelectionType = null;
    document.removeEventListener('click', handleListingClick, true);
    document.removeEventListener('mousemove', handleMouseMove);
    document.body.style.cursor = '';
    document.body.classList.remove('scraper-selecting');
    hideHighlight();
  }
}

// Find common selector between two elements (Two-Click Strategy)
function findCommonSelector(element1, element2) {
  // Đơn giản hóa: Tìm thẻ <a> có href đầu tiên từ mỗi element
  function findFirstLinkWithHref(el) {
    // Nếu đã là thẻ <a> có href
    if (el.tagName === 'A' && el.href) {
      return el;
    }
    // Tìm trong element
    const link = el.querySelector('a[href]');
    if (link) return link;
    // Tìm trong parent
    return el.closest('a[href]') || el;
  }
  
  const final1 = findFirstLinkWithHref(element1);
  const final2 = findFirstLinkWithHref(element2);
  
  const tag1 = final1.tagName.toLowerCase();
  const tag2 = final2.tagName.toLowerCase();
  
  // Must have same tag name (should be 'a')
  if (tag1 !== tag2 || tag1 !== 'a') {
    return null;
  }
  
  // Strategy: Lấy class của thẻ <a> có href đầu tiên
  // ƯU TIÊN: Lấy class của thẻ <a> đầu tiên (final1) và kiểm tra xem nó có match cả 2 thẻ không
  // KHÔNG filter bỏ class có '__' vì đó là class chính của thẻ <a> (vd: js__product-link-for-product-id)
  const classes1 = final1.className && typeof final1.className === 'string'
    ? final1.className.trim().split(/\s+/).filter(c => 
        c && c.length > 2 && 
        !c.includes('scraper-') // Chỉ filter bỏ class do extension thêm vào
      )
    : [];
  
  // Ưu tiên: Thử từng class của thẻ <a> đầu tiên
  if (classes1.length > 0) {
    // Sắp xếp theo độ dài (class dài nhất thường là class chính)
    const sortedClasses = classes1.sort((a, b) => b.length - a.length);
    for (const class1 of sortedClasses) {
      const selector = `a.${class1}`;
      const matches = Array.from(document.querySelectorAll(selector));
      // Kiểm tra xem selector này có match cả 2 thẻ <a> không
      if (matches.includes(final1) && matches.includes(final2)) {
        return selector;
      }
    }
  }
  
  // Fallback: Tìm class chung giữa 2 thẻ <a>
  const classes2 = final2.className && typeof final2.className === 'string'
    ? final2.className.trim().split(/\s+/).filter(c => 
        c && c.length > 2 && 
        !c.includes('scraper-') // Chỉ filter bỏ class do extension thêm vào
      )
    : [];
  
  // Find common classes
  const commonClasses = classes1.filter(c => classes2.includes(c));
  
  if (commonClasses.length > 0) {
    // Ưu tiên class dài nhất (thường là class chính)
    const sortedClasses = commonClasses.sort((a, b) => b.length - a.length);
    for (const commonClass of sortedClasses) {
      const selector = `a.${commonClass}`;
      const matches = Array.from(document.querySelectorAll(selector));
      if (matches.includes(final1) && matches.includes(final2)) {
        return selector;
      }
    }
  }
  
  // Strategy 2: Check for common data attributes
  const attrs1 = Array.from(final1.attributes).filter(attr => 
    attr.name.startsWith('data-') && attr.value && attr.value.length < 50
  );
  const attrs2 = Array.from(final2.attributes).filter(attr => 
    attr.name.startsWith('data-') && attr.value && attr.value.length < 50
  );
  
  for (const attr1 of attrs1) {
    const matchingAttr = attrs2.find(attr2 => attr1.name === attr2.name && attr1.value === attr2.value);
    if (matchingAttr) {
      const selector = `${tag1}[${attr1.name}="${attr1.value}"]`;
      const matches = Array.from(document.querySelectorAll(selector));
      if (matches.includes(final1) && matches.includes(final2)) {
        return selector;
      }
    }
  }
  
  // Strategy 3: Structural Match (NEW - Fallback when no common classes)
  // Find common ancestor and build structural path
  const commonAncestor = findCommonAncestor(final1, final2);
  
  if (commonAncestor) {
    // Try to find a stable container class in the ancestor hierarchy
    let container = commonAncestor;
    let containerSelector = null;
    
    // Look for listing/list container classes
    for (let i = 0; i < 10 && container && container !== document.body; i++) {
      const containerClasses = container.className && typeof container.className === 'string'
        ? container.className.trim().split(/\s+/).filter(c => 
            c && c.length > 3 && 
            !c.includes('scraper-') &&
            (c.includes('list') || c.includes('container') || c.includes('grid') || c.includes('items'))
          )
        : [];
      
      if (containerClasses.length > 0) {
        containerSelector = `.${containerClasses[0]}`;
        break;
      }
      
      container = container.parentElement;
    }
    
    // Build path from container to target
    if (containerSelector) {
      // Try: container > tag
      let selector = `${containerSelector} ${tag1}`;
      let matches = Array.from(document.querySelectorAll(selector));
      if (matches.includes(final1) && matches.includes(final2)) {
        return selector;
      }
      
      // Try: container > * > tag (one level deeper)
      selector = `${containerSelector} > * > ${tag1}`;
      matches = Array.from(document.querySelectorAll(selector));
      if (matches.includes(final1) && matches.includes(final2)) {
        return selector;
      }
    }
    
    // Build structural path from common ancestor
    const path1 = getPathFromAncestor(commonAncestor, final1);
    const path2 = getPathFromAncestor(commonAncestor, final2);
    
    // If paths are identical, use that path
    if (path1 === path2 && path1) {
      const selector = containerSelector 
        ? `${containerSelector} ${path1}`
        : path1;
      
      const matches = Array.from(document.querySelectorAll(selector));
      if (matches.includes(final1) && matches.includes(final2)) {
        return selector;
      }
    }
    
    // Fallback: Just use tag within common ancestor
    const ancestorTag = commonAncestor.tagName.toLowerCase();
    const selector = `${ancestorTag} ${tag1}`;
    const matches = Array.from(commonAncestor.querySelectorAll(tag1));
    if (matches.includes(final1) && matches.includes(final2)) {
      return selector;
    }
  }
  
  // Strategy 4: If both are links, try href pattern
  if (tag1 === 'a' && tag2 === 'a' && final1.href && final2.href) {
    try {
      const url1 = new URL(final1.href);
      const url2 = new URL(final2.href);
      
      // Check if they share same path pattern
      const path1Parts = url1.pathname.split('/').filter(p => p);
      const path2Parts = url2.pathname.split('/').filter(p => p);
      
      if (path1Parts.length >= 2 && path2Parts.length >= 2) {
        // Try last 2 path parts
        const pattern1 = '/' + path1Parts.slice(-2).join('/');
        const pattern2 = '/' + path2Parts.slice(-2).join('/');
        
        if (pattern1 === pattern2) {
          const selector = `a[href*="${pattern1}"]`;
          const matches = Array.from(document.querySelectorAll(selector));
          if (matches.includes(final1) && matches.includes(final2)) {
            return selector;
          }
        }
      }
    } catch (e) {
      // Invalid URLs, skip
    }
  }
  
  // Strategy 5: Last resort - simple tag selector if both are in same parent
  if (final1.parentElement === final2.parentElement) {
    const selector = tag1;
    const matches = Array.from(document.querySelectorAll(selector));
    // Only use if there are multiple matches (likely a list)
    if (matches.length > 1 && matches.includes(final1) && matches.includes(final2)) {
      return selector;
    }
  }
  
  // No common pattern found
  return null;
}

// Helper: Find common ancestor between two elements
function findCommonAncestor(element1, element2) {
  // Build ancestor chain for element1
  const ancestors1 = [];
  let current = element1;
  while (current && current !== document.body) {
    ancestors1.push(current);
    current = current.parentElement;
    if (ancestors1.length > 20) break; // Safety limit
  }
  
  // Check element2's ancestors against element1's
  current = element2;
  while (current && current !== document.body) {
    if (ancestors1.includes(current)) {
      return current;
    }
    current = current.parentElement;
  }
  
  return null;
}

// Helper: Get CSS path from ancestor to element
function getPathFromAncestor(ancestor, element) {
  const path = [];
  let current = element;
  
  while (current && current !== ancestor && current !== document.body) {
    const tag = current.tagName.toLowerCase();
    const index = Array.from(current.parentElement.children).indexOf(current) + 1;
    
    // Try to use class if available
    if (current.className && typeof current.className === 'string') {
      const classes = current.className.trim().split(/\s+/).filter(c => 
        c && c.length > 2 && !c.includes('scraper-') && !/^(vip|normal|premium|basic)$/i.test(c)
      );
      if (classes.length > 0) {
        path.unshift(`${tag}.${classes[0]}`);
      } else {
        path.unshift(tag);
      }
    } else {
      path.unshift(tag);
    }
    
    current = current.parentElement;
    if (path.length > 10) break; // Safety limit
  }
  
  return path.join(' > ');
}

// Get element path (tag names from root to element)
function getElementPath(element) {
  const path = [];
  let current = element;
  
  while (current && current !== document.body && path.length < 10) {
    path.unshift(current.tagName.toLowerCase());
    current = current.parentElement;
  }
  
  return path;
}

// Handle element click
function handleElementClick(e) {
  if (!isSelecting || currentMode !== 'detail') return;
  
  e.preventDefault();
  e.stopPropagation();
  
  let element = e.target;
  let forcedMapCoords = null;
  let forceXpath = false;
  
  // Skip if clicking on scraper UI elements
  if (element.closest('#scraper-highlight')) {
    return;
  }
  
  // Nếu click vào container có iframe map, ưu tiên iframe để tránh quét cả box
  if (element && element.tagName !== 'IFRAME') {
    const mapIframe = element.querySelector && element.querySelector('iframe');
    if (mapIframe) {
      const mapCoordsChild = getLatLngFromElement(mapIframe);
      const iframeSrc = (mapIframe.getAttribute && mapIframe.getAttribute('src')) || '';
      const isMapFrame = !!mapCoordsChild || /maps\.google\.com|google\.com\/maps|output=embed/i.test(iframeSrc);
      if (isMapFrame) {
        element = mapIframe;
        forcedMapCoords = mapCoordsChild || null;
      }
    }
  }
  
  let selector = '';

  // Prefer XPath when clicking <td> that contains a <b> label
  if (selectorTypePreference === 'xpath') {
    const td = element && element.closest ? element.closest('td') : null;
    if (td) {
      const labelEl = td.querySelector('b');
      if (labelEl) {
        let labelText = (labelEl.textContent || '').replace(/\s+/g, ' ').trim();
        labelText = labelText.replace(/:$/g, '').trim();
        if (labelText) {
          const safeLabel = labelText.replace(/"/g, "'");
          selector = `//td[b[contains(normalize-space(), "${safeLabel}")]]`;
          element = td;
          forceXpath = true;
        }
      }
    }
  }

  // Prefer XPath when clicking <p> or <li> that contains a <b> label
  if (selectorTypePreference === 'xpath' && !selector) {
    const rowEl = element && element.closest ? (element.closest('p') || element.closest('li')) : null;
    if (rowEl) {
      const labelEl = rowEl.querySelector('b');
      if (labelEl) {
        let labelText = (labelEl.textContent || '').replace(/\s+/g, ' ').trim();
        labelText = labelText.replace(/:$/g, '').trim();
        if (labelText) {
          const safeLabel = labelText.replace(/"/g, "'");
          const tagName = rowEl.tagName.toLowerCase();
          selector = `//${tagName}[b[contains(normalize-space(), "${safeLabel}")]]`;
          element = rowEl;
          forceXpath = true;
        }
      }
    }
  }

  if (!selector) {
    selector = generateSelector(element);
  }

  // Map iframe: prefer parent class + iframe to avoid generic div > iframe
  if (element && element.tagName === 'IFRAME') {
    let parent = element.parentElement;
    let parentClass = '';
    for (let i = 0; i < 5 && parent && parent !== document.body; i++) {
      if (parent.className && typeof parent.className === 'string') {
        const parentClasses = parent.className.trim().split(/\s+/).filter(c =>
          c && c.length > 2 && !c.includes('scraper-')
        );
        if (parentClasses.length > 0) {
          parentClass = parentClasses[0];
          break;
        }
      }
      parent = parent.parentElement;
    }
    if (parentClass) {
      if (selectorTypePreference === 'xpath') {
        selector = `//div[contains(@class,"${parentClass}")]//iframe`;
      } else {
        selector = `.${parentClass} iframe`;
      }
    }
  }
  
  // Ensure selector is valid
  if (!selector || selector.trim() === '') {
    // Fallback: use CSS selector
    selector = getUniqueCssPath(element) || element.tagName.toLowerCase();
  }
  
  const fullSelector = getFullSelector(element);
  let text = extractText(element);

  // Special case: map iframe or container -> extract lat,lng from src/data-src/data-lat/data-lng
  const mapCoords = forcedMapCoords || getLatLngFromElement(element);
  if (mapCoords) {
    text = `${mapCoords.lat},${mapCoords.lng}`;
  } else if (element && element.tagName === 'IFRAME') {
    const iframeSrc = element.getAttribute && (element.getAttribute('data-src') || element.getAttribute('src'));
    if (iframeSrc) {
      text = iframeSrc;
    }
  }

  // Normalize selector for môi giới (Batdongsan) để dùng chung normal + VIP
  (() => {
    const isBds = location.hostname && location.hostname.includes('batdongsan.com.vn');
    if (!isBds) return;
    const inAgentInfo = element.closest && element.closest('.re__agent-infor');
    if (!inAgentInfo) return;
    const hasAgentClass = element.classList && (element.classList.contains('re__contact-name') || element.classList.contains('js__agent-contact-name'));
    const hasAgentChild = element.querySelector && element.querySelector('.re__contact-name, .js__agent-contact-name');
    if (!hasAgentClass && !hasAgentChild) return;
    const cssCombined = '.re__agent-infor :is(.re__contact-name, .js__agent-contact-name)';
    const xpathCombined = '//div[contains(@class,"re__agent-infor")]//*[contains(@class,"re__contact-name") or contains(@class,"js__agent-contact-name")]';
    selector = selectorTypePreference === 'xpath' ? xpathCombined : cssCombined;
  })();
  
  let allMatches = [];
  if (selector.startsWith('//')) {
    allMatches = getElementsByXPath(selector);
    // If XPath returns nothing, try CSS fallback
    if (allMatches.length === 0 && !forceXpath) {
      const cssFallback = getUniqueCssPath(element);
      if (cssFallback) {
        allMatches = document.querySelectorAll(cssFallback);
        if (allMatches.length > 0) {
          selector = cssFallback; // Use CSS instead
        }
      }
    }
  } else {
    allMatches = document.querySelectorAll(selector);
  }

  if (allMatches.length > 50) {
    alert('Selector quá rộng, sẽ lấy quá nhiều phần tử. Vui lòng chọn phần tử cụ thể hơn.');
    return;
  }
  
  // Ensure we found the element
  if (!forceXpath && (allMatches.length === 0 || !Array.from(allMatches).includes(element))) {
    // Try to find element using CSS as fallback
    const cssFallback = getUniqueCssPath(element);
    if (cssFallback) {
      const cssMatches = document.querySelectorAll(cssFallback);
      if (cssMatches.length > 0 && Array.from(cssMatches).includes(element)) {
        selector = cssFallback;
        allMatches = cssMatches;
      }
    }
  }

  // If selector matches multiple elements, tighten it to the clicked element
  if (!forceXpath && allMatches.length > 1 && Array.from(allMatches).includes(element)) {
    let preferredSelector = null;
    if (selector.startsWith('//') || selectorTypePreference === 'xpath') {
      preferredSelector = getAbsoluteXPath(element);
    } else {
      preferredSelector = getFullSelector(element) || getUniqueCssPath(element);
    }

    if (!preferredSelector && selector.startsWith('//')) {
      preferredSelector = getUniqueCssPath(element);
    }

    if (preferredSelector) {
      const preferredMatches = preferredSelector.startsWith('//')
        ? getElementsByXPath(preferredSelector)
        : Array.from(document.querySelectorAll(preferredSelector));

      if (preferredMatches.length > 0 && preferredMatches[0] === element) {
        selector = preferredSelector;
        allMatches = preferredMatches;
      }
    }
  }
  
  // ... (Phần còn lại của hàm giữ nguyên) ...
  
  // Generate a unique identifier for this specific element
  // STRATEGY: Dùng nhiều cách để đảm bảo unique, ưu tiên các cách chắc chắn nhất
  let uniqueId = selector;
  let elementReference = null;
  
  // Priority 1: Dùng attribute duy nhất (src, href, data-id) - chắc chắn nhất
  if (element.src) {
    uniqueId += `[src="${element.src}"]`;
    elementReference = { type: 'src', value: element.src };
  } else if (element.href) {
    uniqueId += `[href="${element.href}"]`;
    elementReference = { type: 'href', value: element.href };
  } else if (element.getAttribute('data-id')) {
    const dataId = element.getAttribute('data-id');
    uniqueId += `[data-id="${dataId}"]`;
    elementReference = { type: 'data-id', value: dataId };
  } else {
    // Priority 2: Dùng XPath tuyệt đối (nếu có thể) - rất chắc chắn
    // Tạo XPath dựa trên vị trí trong DOM tree
    const absoluteXPath = getAbsoluteXPath(element);
    if (absoluteXPath) {
      uniqueId += `[xpath="${absoluteXPath}"]`;
      elementReference = { type: 'xpath', value: absoluteXPath };
    } else {
      // Priority 3: Dùng index trong một selector chung (nếu có thể tìm selector chung)
      // Tìm selector chung của các element tương tự (cùng class/tag)
      let commonSelector = null;
      if (element.className && typeof element.className === 'string') {
        const classes = element.className.trim().split(/\s+/).filter(c => c && c.length > 3);
        if (classes.length > 0) {
          // Dùng class đầu tiên làm selector chung
          commonSelector = `.${classes[0]}`;
        }
      }
      
      if (!commonSelector) {
        // Fallback: dùng tag name
        commonSelector = element.tagName.toLowerCase();
      }
      
      // Query tất cả elements có selector chung này
      const allCommonElements = document.querySelectorAll(commonSelector);
      const indexInCommon = Array.from(allCommonElements).indexOf(element);
      
      if (indexInCommon >= 0) {
        uniqueId += `[commonSelector="${commonSelector}"]:eq(${indexInCommon})`;
        elementReference = { type: 'index', value: indexInCommon, commonSelector: commonSelector };
      } else {
        // Priority 4: Dùng fullSelector hoặc text làm fallback
        if (fullSelector && fullSelector !== selector) {
          uniqueId += `[fullSelector="${fullSelector}"]`;
          elementReference = { type: 'fullSelector', value: fullSelector };
        } else if (text.length > 0 && text.length < 100) {
          // Dùng text làm fallback cuối cùng (nhưng vẫn có thể trùng nếu 2 field có cùng text)
          uniqueId += `[text="${text.substring(0, 50)}"]`;
          elementReference = { type: 'text', value: text };
        }
      }
    }
  }
  
  const existingIndex = selectedFields.findIndex(f => {
    if (f.uniqueId === uniqueId) {
      return true;
    }
    if (f.elementReference && elementReference && 
        f.elementReference.type === elementReference.type &&
        f.elementReference.value === elementReference.value) {
      return true;
    }
    return false;
  });
  
  if (existingIndex >= 0) {
    selectedFields.splice(existingIndex, 1);
  } else {
    const fieldName = generateFieldName(element, text);
    
    let cssSelector = selector;
    if (selector.startsWith('//')) {
      cssSelector = fullSelector || getUniqueCssPath(element);
    }
    
    // Check if element is a slider/gallery container - auto set valueType to 'src'
    const isSliderContainer = element.className && typeof element.className === 'string' && (
      element.className.includes('slider') ||
      element.className.includes('gallery') ||
      element.className.includes('media-slide') ||
      element.className.includes('media-preview') ||
      element.className.includes('swiper') ||
      element.className.includes('image') ||
      element.className.includes('photo') ||
      element.querySelector('img') !== null
    );
    
    // Lưu XPath riêng nếu selector là XPath, hoặc nếu selectorTypePreference là xpath nhưng selector là CSS
    let xpathSelector = null;
    if (selector.startsWith('//')) {
      xpathSelector = selector;
    } else if (selectorTypePreference === 'xpath') {
      // Nếu user chọn XPath mode nhưng selector là CSS (do fallback), thử tạo XPath
      // Tạo XPath từ element
      const absoluteXPath = getAbsoluteXPath(element);
      if (absoluteXPath) {
        xpathSelector = absoluteXPath;
      }
    }
    
    const field = {
      name: fieldName,
      selector: selector,
      cssSelector: cssSelector,
      xpath: xpathSelector,  // Lưu XPath riêng
      fullSelector: fullSelector,
      uniqueId: uniqueId,
      tagName: element.tagName.toLowerCase(),
      elementReference: elementReference,
      textContent: text.substring(0, 2000),
      selectorType: selector.startsWith('//') ? 'xpath' : 'css',
      valueType: isSliderContainer ? 'src' : undefined,
      excludeWords: ''
    };
    
    selectedFields.push(field);
  }
  
  chrome.storage.local.set({ selectedFields: selectedFields });
  
  if (selectedFields.length > 0) {
    chrome.runtime.sendMessage({
      action: 'fieldSelected',
      field: selectedFields[selectedFields.length - 1]
    });
  }
  
  updateFieldHighlights();
}

// Handle mouse move for highlighting
function handleMouseMove(e) {
  if (!isSelecting) {
    hideHighlight();
    return;
  }
  
  // In listing mode, snap highlight to the actual link instead of the entire card
  let element = e.target;
  if (element && element.nodeType === Node.TEXT_NODE) {
    element = element.parentElement;
  }
  if (currentMode === 'listing' && listingSelectionType === 'itemLink') {
    // Prefer the main link so the highlight and preview stay on the href
    let target = snapToMainLink(element);
    if (target && target.tagName !== 'A') {
      const fallbackLink = getTargetLinkElement(target);
      if (fallbackLink) {
        target = fallbackLink;
      }
    }
    if (target) {
      element = target;
    }
  }
  
  if (!element) {
    hideHighlight();
    return;
  }
  
  // Skip overlay and its children
  if (element.id === 'scraper-highlight' || element.closest('#scraper-highlight')) {
    return;
  }
  
  // Skip script, style, and other non-visible elements
  if (element.tagName === 'SCRIPT' || element.tagName === 'STYLE' || 
      element.tagName === 'NOSCRIPT' || element.tagName === 'META') {
    return;
  }
  
  // Only highlight if element is visible
  const rect = element.getBoundingClientRect();
  if (rect.width === 0 && rect.height === 0) {
    hideHighlight();
    return;
  }
  
  highlightElement(element);
}

// Update highlights for selected fields
function updateFieldHighlights() {
  // Remove old highlights
  document.querySelectorAll('.scraper-field-selected').forEach(el => {
    el.classList.remove('scraper-field-selected');
  });
  
  // Add highlights for selected fields
  selectedFields.forEach(field => {
    const elements = field.selector && field.selector.startsWith('//')
      ? getElementsByXPath(field.selector)
      : Array.from(document.querySelectorAll(field.selector));
    
    elements.forEach(el => {
      if (el) el.classList.add('scraper-field-selected');
    });
  });
}

// Scrape data based on selected fields (improved to get exact element)
function scrapeFields(fields) {
  const result = {};
  
  fields.forEach(field => {
    let elements = [];
      
      // Try to find element by elementReference first (most accurate)
      if (field.elementReference) {
        // Check if selector is XPath or CSS
        const allWithSelector = field.selector && field.selector.startsWith('//')
          ? getElementsByXPath(field.selector)
          : Array.from(document.querySelectorAll(field.selector));
        
        switch (field.elementReference.type) {
          case 'src':
            elements = Array.from(allWithSelector).filter(el => el.src === field.elementReference.value);
            break;
          case 'href':
            elements = Array.from(allWithSelector).filter(el => el.href === field.elementReference.value);
            break;
          case 'data-id':
            elements = Array.from(allWithSelector).filter(el => 
              el.getAttribute('data-id') === field.elementReference.value
            );
            break;
          case 'index':
            const index = field.elementReference.value;
            if (allWithSelector[index]) {
              elements = [allWithSelector[index]];
            }
            break;
          case 'text':
            // Match by text content (fuzzy match)
            const targetText = field.elementReference.value.trim();
            elements = Array.from(allWithSelector).filter(el => {
              const elText = extractText(el).trim();
              return elText === targetText || elText.includes(targetText) || targetText.includes(elText);
            });
            break;
        }
      }
      
      if (elements.length === 0) {
        const selectorToUse = field.customSelector || field.selector;
        if (selectorToUse) {
          elements = selectorToUse.startsWith('//')
            ? getElementsByXPath(selectorToUse)
            : Array.from(document.querySelectorAll(selectorToUse));
        }
        if (elements.length === 0 && field.fullSelector) {
          elements = field.fullSelector.startsWith('//')
            ? getElementsByXPath(field.fullSelector)
            : Array.from(document.querySelectorAll(field.fullSelector));
        }
      }
      
      // Extract data from found elements
      if (elements.length > 0) {
        const firstElement = elements[0];
        const selectorToCheck = field.customSelector || field.selector || '';
        
        // Check if it's a slider/gallery container (by valueType, tagName, class, selector, or contains img)
        const isSliderContainer = (
          field.valueType === 'src' ||
          field.tagName === 'img' ||
          (firstElement.className && typeof firstElement.className === 'string' && (
            firstElement.className.includes('slider') ||
            firstElement.className.includes('gallery') ||
            firstElement.className.includes('media-slide') ||
            firstElement.className.includes('media-preview') ||
            firstElement.className.includes('swiper') ||
            firstElement.className.includes('image') ||
            firstElement.className.includes('photo')
          )) ||
          selectorToCheck.includes('slider') ||
          selectorToCheck.includes('gallery') ||
          selectorToCheck.includes('media-slide') ||
          selectorToCheck.includes('swiper') ||
          firstElement.querySelector('img') !== null
        );
        
        if (isSliderContainer) {
          // Find all img tags inside container(s)
          let allImages = [];
          elements.forEach(el => {
            if (el.tagName && el.tagName.toLowerCase() === 'img') {
              allImages.push(el);
            } else if (el.querySelectorAll) {
              // Priority 1: Find full-size images from swiper slides
              const fullSizeImgs = el.querySelectorAll('.swiper-slide .pr-img, .swiper-slide img.pr-img, .re__overlay .pr-img, .swiper-slide img:not(.re__media-thumb-item img)');
              if (fullSizeImgs.length > 0) {
                allImages.push(...Array.from(fullSizeImgs));
              } else {
                // Priority 2: Get all images but exclude thumbnails
                const allImgs = el.querySelectorAll('img');
                Array.from(allImgs).forEach(img => {
                  // Skip thumbnails by checking parent or src
                  const src = img.getAttribute('src') || img.getAttribute('data-src') || '';
                  const isThumbnail = src.includes('/resize/200x200/') || 
                                     src.includes('/resize/100x100/') || 
                                     src.includes('/resize/50x50/') ||
                                     img.closest('.re__media-thumb-item') !== null ||
                                     img.closest('.js__media-thumbs') !== null ||
                                     img.closest('.slick-slide') !== null && !img.closest('.swiper-slide');
                  if (!isThumbnail) {
                    allImages.push(img);
                  }
                });
              }
            }
          });
          
          if (allImages.length > 0) {
            const imageUrls = allImages.map(el => {
              let src = el.getAttribute('data-src') || el.getAttribute('src') || el.getAttribute('data-lazy-src') || el.getAttribute('data-original') || el.getAttribute('href') || null;
              if (!src && el.src && typeof el.src === 'string' && (el.src.startsWith('http://') || el.src.startsWith('https://'))) {
                src = el.src;
              }
              if (src && !src.startsWith('data:') && !src.startsWith('blob:') && !src.includes('.svg')) {
                return src;
              }
              return null;
            }).filter(src => {
              if (!src || src === '') return false;
              // Filter out thumbnails (200x200, 100x100, 50x50, etc.)
              if (src.includes('/resize/200x200/') || 
                  src.includes('/resize/100x100/') || 
                  src.includes('/resize/50x50/') ||
                  src.includes('/200x200/') ||
                  src.includes('/100x100/') ||
                  src.includes('/50x50/')) {
                return false;
              }
              return true;
            });
            
            // Remove duplicates and return
            const uniqueUrls = [...new Set(imageUrls)];
            result[field.name] = uniqueUrls.length > 0 ? uniqueUrls : [];
          } else {
            const extracted = extractFieldData(elements[0], field);
            result[field.name] = extracted.value !== null ? extracted.value : extracted;
          }
        } else if (elements.length === 1 || (field.elementReference && elements.length > 0)) {
          const extracted = extractFieldData(elements[0], field);
          result[field.name] = extracted.value !== null ? extracted.value : extracted;
        } else if (elements.length > 1) {
          if (field.valueType === 'src' || field.tagName === 'img') {
            const imageUrls = elements.map(el => el.src || el.getAttribute('src') || el.getAttribute('data-src') || null).filter(src => src !== null && src !== '');
            result[field.name] = [...new Set(imageUrls)];
          } else {
            result[field.name] = elements.map(el => {
              const extracted = extractFieldData(el, field);
              return extracted.value !== null ? extracted.value : extracted;
            });
          }
        }
      } else {
        result[field.name] = null;
      }
    });
  
  return result;
}

// Message listener - MUST be set up immediately, not inside init()
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (message.action === 'ping') {
    // Simple ping to check if content script is ready
    sendResponse({ success: true, ready: true });
    return true;
  }
  
  if (message.action === 'setMode') {
    currentMode = message.mode || 'detail';
    listingSelectionType = null;
    firstElement = null;
    secondElement = null;
    
    // Clean up listing selection state
    if (isSelecting && currentMode === 'detail') {
      document.removeEventListener('click', handleListingClick, true);
      isSelecting = false;
      document.body.style.cursor = '';
      document.body.classList.remove('scraper-selecting');
      hideHighlight();
    }
    
    // Remove notification if exists
    const notification = document.getElementById('listing-selection-notification');
    if (notification) notification.remove();
    
    // Clear highlights
    document.querySelectorAll('.scraper-field-selected').forEach(el => {
      el.classList.remove('scraper-field-selected');
      el.style.outline = '';
      el.style.outlineOffset = '';
    });
    
    chrome.storage.local.set({ currentMode: currentMode });
    sendResponse({ success: true, mode: currentMode });
    return true;
  }
  
  if (message.action === 'resetItemLinkSelection') {
    // Reset two-click state
    if (firstElement) {
      firstElement.classList.remove('scraper-field-selected');
      firstElement.style.outline = '';
      firstElement.style.outlineOffset = '';
    }
    if (secondElement) {
      secondElement.classList.remove('scraper-field-selected');
      secondElement.style.outline = '';
      secondElement.style.outlineOffset = '';
    }
    
    // Clear all highlights
    document.querySelectorAll('.scraper-field-selected').forEach(el => {
      el.classList.remove('scraper-field-selected');
      el.style.outline = '';
      el.style.outlineOffset = '';
    });
    
    firstElement = null;
    secondElement = null;
    
    // Remove notification
    const notification = document.getElementById('listing-selection-notification');
    if (notification) notification.remove();
    
    sendResponse({ success: true });
    return true;
  }
  
  if (message.action === 'startListingSelection') {
    currentMode = 'listing';
    listingSelectionType = message.selectionType; // 'itemLink' or 'nextPage'
    
    // Enable selection mode
    isSelecting = true;
    document.addEventListener('click', handleListingClick, true);
    document.addEventListener('mousemove', handleMouseMove);
    document.body.style.cursor = 'crosshair';
    document.body.classList.add('scraper-selecting');
    createHighlightOverlay();
    
    // Show notification
    const notification = document.createElement('div');
    notification.style.cssText = 'position: fixed; top: 20px; right: 20px; background: #4CAF50; color: white; padding: 15px 20px; border-radius: 5px; z-index: 10000; font-size: 14px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);';
    notification.textContent = listingSelectionType === 'itemLink' 
      ? '🎯 Click vào Item 1 (link sản phẩm đầu tiên)'
      : '🎯 Click vào nút Next Page';
    notification.id = 'listing-selection-notification';
    document.body.appendChild(notification);
    
    // Reset two-click state
    if (listingSelectionType === 'itemLink') {
      firstElement = null;
      secondElement = null;
    }
    
    sendResponse({ success: true });
    return true;
  }
  
  if (message.action === 'getState') {
    // Return current state
    sendResponse({ 
      success: true, 
      isSelecting: isSelecting,
      fieldsCount: selectedFields.length 
    });
    return true;
  }
  
  if (message.action === 'toggleSelecting') {
    isSelecting = message.isSelecting;
    if (message.selectorTypePreference) {
      selectorTypePreference = message.selectorTypePreference;
    }
    
    if (isSelecting) {
      document.addEventListener('click', handleElementClick, true);
      document.addEventListener('mousemove', handleMouseMove);
      document.addEventListener('keydown', handleKeyPress);
      document.body.style.cursor = 'crosshair';
      document.body.classList.add('scraper-selecting');
      createHighlightOverlay();
      updateFieldHighlights();
    } else {
      document.removeEventListener('click', handleElementClick, true);
      document.removeEventListener('mousemove', handleMouseMove);
      document.removeEventListener('keydown', handleKeyPress);
      document.body.style.cursor = '';
      document.body.classList.remove('scraper-selecting');
      hideHighlight();
      document.querySelectorAll('.scraper-field-selected').forEach(el => {
        el.classList.remove('scraper-field-selected');
      });
    }
    
    sendResponse({ success: true, isSelecting: isSelecting });
    return true;
  }
  
  if (message.action === 'setSelectorType') {
    selectorTypePreference = message.selectorType || 'xpath';
    chrome.storage.local.set({ selectorTypePreference: selectorTypePreference });
    sendResponse({ success: true, selectorType: selectorTypePreference });
    return true;
  }
  
  if (message.action === 'convertSelector') {
    const selector = message.selector;
    const field = message.field;
    let convertedSelector = null;
    
    try {
      // Check if selector is XPath (starts with //)
      if (selector.startsWith('//')) {
        // Convert XPath to CSS: Find element by XPath, then generate CSS selector
        const elements = getElementsByXPath(selector);
        if (elements.length > 0) {
          const element = elements[0];
          // Try to get CSS selector from field if available
          if (field && field.cssSelector) {
            convertedSelector = field.cssSelector;
          } else {
            // Generate CSS selector
            if (element.id && !element.id.includes('__next')) {
              convertedSelector = `#${element.id}`;
            } else {
              convertedSelector = getUniqueCssPath(element);
            }
          }
        }
      } else {
        // Convert CSS to XPath: Find element by CSS, then generate XPath
        const elements = Array.from(document.querySelectorAll(selector));
        if (elements.length > 0) {
          const element = elements[0];
          // Try to get XPath from field if available
          if (field && field.selector && field.selector.startsWith('//')) {
            convertedSelector = field.selector;
          } else {
            // Generate XPath
            convertedSelector = getAbsoluteXPath(element);
          }
        }
      }
      
      if (convertedSelector) {
        sendResponse({ success: true, convertedSelector: convertedSelector });
      } else {
        sendResponse({ success: false, error: 'Không thể chuyển đổi selector' });
      }
    } catch (error) {
      sendResponse({ success: false, error: error.message });
    }
    return true;
  }
  
  if (message.action === 'updateFields') {
    selectedFields = message.fields || [];
    chrome.storage.local.set({ selectedFields: selectedFields });
    updateFieldHighlights();
    sendResponse({ success: true });
    return true;
  }
  
  if (message.action === 'scrape') {
    const data = scrapeFields(message.fields || selectedFields);
    sendResponse({ success: true, data: data });
    return true;
  }
  
  if (message.action === 'previewValue') {
    const selector = message.selector;
    const valueType = message.valueType || 'text';
    const field = message.field;
      
    let elements = [];
    
    if (field && field.elementReference) {
      const allWithSelector = selector && selector.startsWith('//')
        ? getElementsByXPath(selector)
        : Array.from(document.querySelectorAll(selector));
      switch (field.elementReference.type) {
        case 'src':
          elements = Array.from(allWithSelector).filter(el => el.src === field.elementReference.value);
          break;
        case 'href':
          elements = Array.from(allWithSelector).filter(el => el.href === field.elementReference.value);
          break;
        case 'data-id':
          elements = Array.from(allWithSelector).filter(el => 
            el.getAttribute('data-id') === field.elementReference.value
          );
          break;
        case 'index':
          const index = field.elementReference.value;
          if (allWithSelector[index]) {
            elements = [allWithSelector[index]];
          }
          break;
        case 'text':
          const targetText = field.elementReference.value.trim();
          elements = Array.from(allWithSelector).filter(el => {
            const elText = extractText(el).trim();
            return elText === targetText || elText.includes(targetText.substring(0, 20));
          });
          break;
      }
    }
    
    if (elements.length === 0) {
      if (selector.startsWith('//')) {
        elements = getElementsByXPath(selector);
      } else {
        elements = Array.from(document.querySelectorAll(selector));
      }
    }
    
    if (elements.length === 0) {
      sendResponse({ success: true, value: null });
      return true;
    }
    
    if (valueType === 'src') {
      // Check if it's a container
      const firstElement = elements[0];
      const isContainer = firstElement && firstElement.querySelectorAll && firstElement.querySelector('img') !== null;
      
      let allImages = [];
      if (isContainer) {
        // Find all images in container, prioritize full-size
        const fullSizeImgs = firstElement.querySelectorAll('.swiper-slide .pr-img, .swiper-slide img.pr-img, .re__overlay .pr-img');
        if (fullSizeImgs.length > 0) {
          allImages = Array.from(fullSizeImgs);
        } else {
          const allImgs = firstElement.querySelectorAll('img');
          Array.from(allImgs).forEach(img => {
            const src = img.getAttribute('src') || img.getAttribute('data-src') || '';
            const isThumbnail = src.includes('/resize/200x200/') || 
                               img.closest('.re__media-thumb-item') !== null ||
                               img.closest('.js__media-thumbs') !== null;
            if (!isThumbnail) {
              allImages.push(img);
            }
          });
        }
      } else {
        allImages = elements.filter(el => el.tagName && el.tagName.toLowerCase() === 'img');
      }
      
      const imageUrls = allImages.map(element => {
        let src = element.getAttribute('data-src') || 
                  element.getAttribute('src') || 
                  element.getAttribute('data-lazy-src') || 
                  element.getAttribute('data-original') || 
                  null;
        
        if (!src && element.src && typeof element.src === 'string') {
          if (element.src.startsWith('http://') || element.src.startsWith('https://')) {
            src = element.src;
          }
        }
        
        if (src && !src.startsWith('data:') && !src.startsWith('blob:') && !src.includes('.svg')) {
          // Filter thumbnails
          if (src.includes('/resize/200x200/') || src.includes('/resize/100x100/') || src.includes('/resize/50x50/')) {
            return null;
          }
          return src;
        }
        return null;
      }).filter(src => src !== null && src !== '');
      
      const uniqueUrls = [...new Set(imageUrls)];
      if (uniqueUrls.length > 1 || isContainer) {
        sendResponse({ success: true, value: uniqueUrls, count: uniqueUrls.length, isArray: true });
      } else {
        sendResponse({ success: true, value: uniqueUrls[0] || null });
      }
      return true;
    }
    
    const element = elements[0];
    let value = null;
    
    // Handle 'all' or 'container' valueType - extract all strong[@itemprop] values
    if (valueType === 'all' || valueType === 'container') {
      const container = element;
      const strongElements = container.querySelectorAll('strong[itemprop]');
      const containerData = {};
      
      strongElements.forEach(strong => {
        const itemprop = strong.getAttribute('itemprop');
        const textValue = extractText(strong).trim();
        if (itemprop && textValue) {
          containerData[itemprop] = textValue;
        }
      });
      
      if (Object.keys(containerData).length > 0) {
        sendResponse({ 
          success: true, 
          value: containerData, 
          count: Object.keys(containerData).length,
          isObject: true 
        });
        return true;
      } else {
        sendResponse({ success: true, value: null });
        return true;
      }
    }
    
    switch (valueType) {
      case 'html':
        value = element.innerHTML.trim();
        break;
      case 'src':
        value = element.src || element.getAttribute('src') || element.getAttribute('data-src') || element.getAttribute('data-lazy-src') || null;
        break;
      case 'href':
        value = element.href || element.getAttribute('href') || null;
        break;
      case 'alt':
        value = element.alt || element.getAttribute('alt') || null;
        break;
      case 'title':
        value = element.title || element.getAttribute('title') || null;
        break;
      case 'data-id':
        value = element.getAttribute('data-id') || null;
        break;
      case 'data-phone':
        value = element.getAttribute('data-phone') || null;
        break;
      case 'innerText':
        value = element.innerText.trim();
        break;
      case 'text':
      default:
        value = extractText(element);
        break;
    }

    if (typeof value === 'string' && (valueType === 'text' || valueType === 'innerText')) {
      value = applyExcludeWords(value, field);
    }
    
    sendResponse({ success: true, value: value });
    return true;
  }
  
  return false;
});

// Initialize on page load
function init() {
  const hostname = window.location.hostname;
  if (hostname === 'localhost' || hostname === '127.0.0.1' || hostname.includes('streamlit')) {
    return;
  }
  
  if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
    chrome.storage.local.get(['isSelecting', 'selectedFields', 'selectorTypePreference'], (result) => {
      if (chrome.runtime.lastError) {
        return;
      }
      
      isSelecting = result.isSelecting || false;
      selectedFields = result.selectedFields || [];
      selectorTypePreference = result.selectorTypePreference || 'xpath';
      
      if (isSelecting) {
        document.addEventListener('click', handleElementClick, true);
        document.addEventListener('mousemove', handleMouseMove);
        document.addEventListener('keydown', handleKeyPress);
        document.body.style.cursor = 'crosshair';
        document.body.classList.add('scraper-selecting');
        createHighlightOverlay();
        if (selectedFields.length > 0) {
          updateFieldHighlights();
        }
      }
    });
  }
}

// Run when DOM is ready
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', init);
} else {
  init();
}
