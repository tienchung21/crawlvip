import asyncio
import sys
import os
import re

# Fix encoding cho Windows console
if sys.platform == "win32":
    try:
        # Set UTF-8 encoding cho stdout
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(encoding='utf-8')
        if hasattr(sys.stderr, 'reconfigure'):
            sys.stderr.reconfigure(encoding='utf-8')
        # Hoặc set environment variable
        os.environ['PYTHONIOENCODING'] = 'utf-8'
    except:
        pass

import nodriver as uc

# Cấu hình tiết kiệm cho nodriver (chặn ảnh, tắt audio để giảm lag và tiết kiệm bandwidth)
BROWSER_CONFIG_TIET_KIEM = [
    "--blink-settings=imagesEnabled=false", 
    "--disable-images",
    "--mute-audio",
]

# Cấu hình: Đặt False nếu muốn giữ browser mở mãi (đóng thủ công)
AUTO_CLOSE_BROWSER = True
WAIT_TIME_BEFORE_CLOSE = 300  # Giây (5 phút)

# Helper để in text an toàn (không dùng emoji trên Windows)
def safe_print(*args, **kwargs):
    """Print với encoding an toàn"""
    try:
        print(*args, **kwargs)
    except UnicodeEncodeError:
        # Thay emoji bằng text nếu lỗi encoding
        text = ' '.join(str(arg) for arg in args)
        text = text.replace('🚀', '[*]').replace('✅', '[OK]').replace('⏳', '[...]')
        text = text.replace('💡', '[!]').replace('🔍', '[?]').replace('📄', '[F]')
        text = text.replace('⚠️', '[!]').replace('🔄', '[R]').replace('➡️', '[>]')
        text = text.replace('📍', '[L]').replace('❌', '[X]').replace('🔒', '[L]')
        print(text, **kwargs)


async def main():
    browser = None
    try:
        print("[*] Dang khoi dong browser (che do tiet kiem - chan anh)...")
        browser = await uc.start(headless=False, browser_args=BROWSER_CONFIG_TIET_KIEM)
        print("[OK] Browser da khoi dong")
        
        url = "https://batdongsan.com.vn/nha-dat-cho-thue"
        print(f"[*] Dang mo: {url}")
        page = await browser.get(url)
        print("[OK] Da mo trang")

        # Chờ load, lấy tiêu đề
        await asyncio.sleep(5)
        title = await page.evaluate("document.title")
        print(f"✅ Tiêu đề trang: {title}")
        
        # Chờ element pagination xuất hiện (có thể trang load chậm)
        print("⏳ Chờ pagination xuất hiện...")
        for i in range(10):  # Chờ tối đa 10 giây
            has_pagination = await page.evaluate(
                "document.querySelector('a.re__pagination-icon') !== null"
            )
            if has_pagination:
                print(f"✅ Pagination đã xuất hiện sau {i+1} giây")
                break
            await asyncio.sleep(1)
        else:
            print("⚠️ Pagination chưa xuất hiện sau 10 giây, tiếp tục tìm...")

        # Debug chi tiết: Kiểm tra tất cả các element pagination
        try:
            debug_info = await page.evaluate(
            """
() => {
  const result = {
    hasPaginationIcon: document.querySelector('a.re__pagination-icon') !== null,
    allPaginationIcons: document.querySelectorAll('a.re__pagination-icon').length,
    allPaginationLinks: document.querySelectorAll('a.re__pagination-icon[href]').length,
    hasChevronRight: document.querySelector('.re__icon-chevron-right--sm') !== null,
    paginationHTML: '',
    allLinks: []
  };
  
  // Lấy HTML của pagination area
  const paginationArea = document.querySelector('.re__pagination') || 
                        document.querySelector('[class*="pagination"]') ||
                        document.querySelector('a.re__pagination-icon')?.parentElement;
  if (paginationArea) {
    result.paginationHTML = paginationArea.outerHTML.substring(0, 500);
  }
  
  // Lấy tất cả link pagination
  const links = document.querySelectorAll('a.re__pagination-icon');
  links.forEach(link => {
    result.allLinks.push({
      href: link.href,
      text: link.textContent.trim(),
      hasIcon: link.querySelector('.re__icon-chevron-right--sm') !== null,
      className: link.className
    });
  });
  
  return result;
}
"""
            )
        except Exception as e:
            print(f"[!] Loi khi lay debug info: {e}")
            debug_info = None
        
        if debug_info:
            print(f"[?] Debug pagination:")
            print(f"  - Co element a.re__pagination-icon: {debug_info.get('hasPaginationIcon', False)}")
            print(f"  - So luong a.re__pagination-icon: {debug_info.get('allPaginationIcons', 0)}")
            print(f"  - So luong co href: {debug_info.get('allPaginationLinks', 0)}")
            print(f"  - Co icon chevron-right: {debug_info.get('hasChevronRight', False)}")
            print(f"  - Tat ca links: {debug_info.get('allLinks', [])}")
            print(f"  - HTML pagination area (500 ky tu dau):\n{debug_info.get('paginationHTML', '')[:200]}")
            
            # Nếu không tìm thấy, thử tìm bằng các selector khác
            if not debug_info.get('hasPaginationIcon', False):
                print("\n[?] Thu tim bang selector khac...")
                alternative_selectors = [
                    'a[class*="pagination"]',
                    'a[href*="/p"]',
                    '.re__pagination a',
                    '[class*="pagination"] a',
                    'a:has(.re__icon-chevron-right)',
                    'a:has(.re__icon-chevron-right--sm)'
                ]
                for selector in alternative_selectors:
                    try:
                        found = await page.evaluate(
                            f"document.querySelector('{selector}') !== null"
                        )
                        if found:
                            count = await page.evaluate(
                                f"document.querySelectorAll('{selector}').length"
                            )
                            print(f"  [OK] Tim thay voi '{selector}': {count} element(s)")
                    except:
                        pass

        # Tìm và click nút Next - Click từ trang 1 -> 2 -> 3
        WAIT_BEFORE_CLICK = 5 # Giây chờ trước khi click next
        TARGET_PAGE = 3  # Trang đích (1 -> 2 -> 3)
        
        current_url = await page.evaluate("window.location.href")
        print(f"[L] URL hien tai: {current_url}")
        
        # Lấy số trang hiện tại
        current_page_num = await page.evaluate("window.location.href.match(/\\/p(\\d+)/)?.[1] || '1'")
        current_page_num = int(current_page_num)
        print(f"[*] Trang hien tai: {current_page_num}, Muc tieu: Trang {TARGET_PAGE}")
        
        # Vòng lặp click next cho đến khi đến trang 3
        for target_page in range(current_page_num + 1, TARGET_PAGE + 1):
            print(f"\n{'='*50}")
            print(f"[*] Dang chuyen tu trang {current_page_num} sang trang {target_page}...")
            print(f"{'='*50}")
            
            # Chờ trước khi click next
            print(f"[...] Cho {WAIT_BEFORE_CLICK} giay truoc khi click next...")
            await asyncio.sleep(WAIT_BEFORE_CLICK)
            
            clicked = False
            try:
                # 1. Tìm icon mũi tên, sau đó tìm thẻ <a> cha
                icon = await page.select('.re__icon-chevron-right--sm', timeout=10)
                
                if icon:
                    print("[OK] Da tim thay icon mui ten")
                    
                    # Tìm thẻ <a> cha chứa icon này
                    parent_link = await page.evaluate(
                        """
() => {
  const icon = document.querySelector('.re__icon-chevron-right--sm');
  if (icon) {
    const link = icon.closest('a.re__pagination-icon');
    if (link) {
      return {
        href: link.href,
        found: true
      };
    }
  }
  return {found: false};
}
"""
                    )
                    
                    if parent_link.get('found'):
                        next_url = parent_link['href']
                        print(f"[OK] Tim thay link cha: {next_url}")
                        
                        # Lấy số trang hiện tại và số trang tiếp theo
                        current_page = await page.evaluate("window.location.href.match(/\\/p(\\d+)/)?.[1] || '1'")
                        next_match = re.search(r'/p(\d+)', next_url) if next_url else None
                        next_page = next_match.group(1) if next_match else None
                        print(f"[*] Trang hien tai: {current_page}, Trang tiep theo: {next_page}")
                        
                        # Cách 1: Click vào thẻ <a> bằng JavaScript (đáng tin cậy hơn)
                        print("[>] Thu click bang JavaScript...")
                        click_result = await page.evaluate(
                        """
() => {
  const icon = document.querySelector('.re__icon-chevron-right--sm');
  if (icon) {
    const link = icon.closest('a.re__pagination-icon');
    if (link) {
      link.scrollIntoView({behavior: 'instant', block: 'center'});
      // Thử nhiều cách click
      link.click();
      return true;
    }
  }
  return false;
}
"""
                        )
                        
                        if click_result:
                            clicked = True
                            print("[>] Da click bang JavaScript!")
                        else:
                            # Cách 2: Dùng nodriver click vào thẻ <a>
                            print("[>] Thu click bang nodriver API...")
                            try:
                                link_element = await page.select('a.re__pagination-icon:has(.re__icon-chevron-right--sm)', timeout=3)
                                if not link_element:
                                    # Fallback: Tìm tất cả link và chọn cái có icon
                                    all_links = await page.select_all('a.re__pagination-icon', timeout=3)
                                    for link in all_links:
                                        has_icon = await page.evaluate(
                                            """
() => {
          const links = document.querySelectorAll('a.re__pagination-icon');
          for (let link of links) {
            if (link.querySelector('.re__icon-chevron-right--sm')) {
              return true;
            }
          }
          return false;
        }
"""
                                        )
                                        if has_icon:
                                            link_element = link
                                            break
                                
                                if link_element:
                                    await link_element.scroll_into_view()
                                    await asyncio.sleep(0.5)
                                    await link_element.click()
                                    clicked = True
                                    print("[>] Da click bang nodriver API!")
                            except Exception as e:
                                print(f"[!] Loi click nodriver: {e}")
                                # Cách 3: Navigate trực tiếp đến URL
                                print("[>] Thu navigate truc tiep den URL...")
                                try:
                                    await page.get(next_url)
                                    clicked = True
                                    print(f"[>] Da navigate truc tiep den: {next_url}")
                                except Exception as e2:
                                    print(f"[X] Navigate that bai: {e2}")
                        
                        # Chờ và kiểm tra
                        if clicked:
                            print("[...] Da click, cho chuyen trang...")
                            await asyncio.sleep(5)  # Chờ lâu hơn
                            
                            # Kiểm tra URL
                            new_url = await page.evaluate("window.location.href")
                            print(f"[L] URL sau click: {new_url}")
                            
                            # Kiểm tra xem có phải AJAX không (nội dung thay đổi nhưng URL không đổi)
                            page_number_after = await page.evaluate(
                                """
() => {
  const active = document.querySelector('.re__pagination-number.re__actived');
  return active ? active.textContent.trim() : null;
}
"""
                            )
                            print(f"[*] So trang active sau click: {page_number_after}")
                            
                            # Cập nhật số trang hiện tại
                            if new_url != current_url:
                                print("[OK] URL da thay doi - Navigation thanh cong!")
                                # Lấy số trang từ URL mới
                                url_match = re.search(r'/p(\d+)', new_url)
                                if url_match:
                                    current_page_num = int(url_match.group(1))
                                else:
                                    current_page_num = 2  # Mặc định nếu không tìm thấy
                            elif page_number_after:
                                print(f"[OK] Trang da chuyen (AJAX) - Tu trang {current_page_num} sang trang {page_number_after}!")
                                current_page_num = int(page_number_after)
                            else:
                                print("[!] URL va trang deu chua thay doi")
                                # Thử kiểm tra xem có popup che không
                                has_popup = await page.evaluate(
                                    "document.querySelector('.modal, .popup, [class*=\"overlay\"]') !== null"
                                )
                                if has_popup:
                                    print("[!] Co the co popup/quang cao dang che nut Next")
                                # Nếu không chuyển được, dừng vòng lặp
                                print("[X] Khong the chuyen trang, dung lai")
                                break
                            
                            # Kiểm tra xem đã đến trang đích chưa
                            if current_page_num >= TARGET_PAGE:
                                print(f"[OK] Da den trang {TARGET_PAGE} thanh cong!")
                                break
                            else:
                                print(f"[*] Hien tai o trang {current_page_num}, tiep tuc den trang {TARGET_PAGE}...")
                        else:
                            print("[X] Khong the click next, dung lai")
                            break
                    else:
                        print("[!] Khong tim thay link cha cua icon")
                        # Thử fallback
                        raise Exception("Khong tim thay link cha")
                else:
                    print("[!] Khong tim thay icon mui ten")
                    # Thử fallback
                    raise Exception("Khong tim thay icon")
                        
            except Exception as e:
                # Fallback: Nếu không tìm thấy icon, thử tìm thẻ A chứa href /p2
                print(f"[!] Loi: {e}")
                print(f"[R] Thu fallback: Tim link trang {target_page}...")
                try:
                    # Tìm thẻ a có href chứa /p{target_page}
                    link_selector = f'a[href*="/p{target_page}"]'
                    link_p = await page.select(link_selector, timeout=5)
                    if link_p:
                        await link_p.scroll_into_view()
                        await asyncio.sleep(0.5)
                        await link_p.click()
                        clicked = True
                        print(f"[OK] Da click link trang {target_page} (Fallback)")
                        await asyncio.sleep(5)
                        current_page_num = target_page
                        if current_page_num >= TARGET_PAGE:
                            print(f"[OK] Da den trang {TARGET_PAGE} thanh cong!")
                            break
                except Exception as e2:
                    print(f"[X] Bo tay. Khong tim thay nut Next nao: {e2}")
                    break

        if clicked:
            print("⏳ Chờ trang load...")
            await asyncio.sleep(5)
            new_title = await page.evaluate("document.title")
            new_url = await page.evaluate("window.location.href")
            print(f"✅ Tiêu đề sau click: {new_title}")
            print(f"🔗 URL sau click: {new_url}")

        print("\n" + "="*50)
        print("✅ Script đã chạy xong!")
        print("="*50)
        
        if AUTO_CLOSE_BROWSER:
            print(f"⏳ Browser sẽ giữ mở {WAIT_TIME_BEFORE_CLOSE} giây để bạn kiểm tra...")
            print("💡 Đóng browser thủ công hoặc chờ để tự đóng...")
            await asyncio.sleep(WAIT_TIME_BEFORE_CLOSE)
        else:
            print("⏳ Browser sẽ giữ mở mãi...")
            print("💡 Đóng browser thủ công khi xong...")
            # Chờ vô hạn (hoặc đến khi có lỗi)
            try:
                while True:
                    await asyncio.sleep(60)
            except KeyboardInterrupt:
                print("\n⚠️ Nhận tín hiệu dừng...")
        
    except KeyboardInterrupt:
        print("\n⚠️ Người dùng dừng script (Ctrl+C)")
    except Exception as e:
        print(f"\n❌ Lỗi: {str(e)}")
        import traceback
        traceback.print_exc()
        print("\n⏳ Chờ 30s trước khi đóng...")
        await asyncio.sleep(30)
    finally:
        if browser:
            if AUTO_CLOSE_BROWSER:
                print("\n[L] Dang dong browser...")
                try:
                    browser.stop()
                except Exception as e:
                    print(f"[!] Loi khi dong browser: {e}")
                print("[OK] Da dong browser")
            else:
                print("\n💡 Browser vẫn mở (AUTO_CLOSE_BROWSER = False)")
                print("   Đóng browser thủ công khi xong...")
        else:
            print("⚠️ Browser chưa được khởi tạo")


if __name__ == "__main__":
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    uc.loop().run_until_complete(main())
