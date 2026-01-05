// Background service worker
try {
  chrome.runtime.onInstalled.addListener((details) => {
    console.log('Web Scraper Extension installed', details);
  });

  chrome.runtime.onStartup.addListener(() => {
    console.log('Web Scraper Extension started');
  });

  // Mở side panel khi click vào icon extension
  chrome.action.onClicked.addListener((tab) => {
    chrome.sidePanel.open({ tabId: tab.id });
  });
} catch (error) {
  console.error('Background script error:', error);
}

