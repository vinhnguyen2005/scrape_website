import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import threading
import re

browser_init_lock = threading.Lock()

# ============= CONFIG =============
LINKS_FILE = "batdongsan_links.jsonl"
BASE_URL = "https://batdongsan.com.vn/nha-dat-ban"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://batdongsan.com.vn/nha-dat-ban/',
}



def init_browser(headless=True, worker_id=0):
    options = Options()
    
    if headless:
        options.add_argument('--headless=new')
        options.add_argument('--disable-gpu')
    else:

        offset = worker_id * 50
        options.add_argument(f'--window-position={offset},{offset}')
    
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument('--disable-notifications')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def load_links_from_file(file_name):
    links = []
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                links.append(data)
        print(f"Loaded {len(links)} links from {file_name}")
    except Exception as e:
        print(f"Error loading links: {e}")
    return links


def load_existing_urls(file_name):
    existing_urls = set()
    try:
        with open(file_name, 'r', encoding='utf-8') as f:
            for line in f:
                data = json.loads(line)
                existing_urls.add(data['url'])
        print(f"Loaded {len(existing_urls)} existing URLs")
    except FileNotFoundError:
        print("No existing file, starting fresh")
    return existing_urls


def save_batch_to_jsonl(items, file_name):
    with open(file_name, 'a', encoding='utf-8') as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

def crawl_one_page_selenium(driver, page, base_url, worker_id=0):
    url = base_url if page == 1 else f"{base_url}/p{page}"
    print(f"[Worker {worker_id}] Page {page}: {url}")

    try:
        driver.get(url)
        
        # Đợi items load
        wait = WebDriverWait(driver, 15)  # Tăng từ 10 lên 15 giây
        try:
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '.js__product-link-for-product-id, [data-product-id]'))
            )
        except:
            print(f"[Worker {worker_id}] Timeout waiting for items")
        
        time.sleep(random.uniform(3, 5))  # Tăng delay
        
        # Scroll nhiều lần để load lazy content
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
        time.sleep(1)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        
        # Parse HTML
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # TÌM TẤT CẢ CÁC SELECTOR CÓ THỂ
        items = soup.find_all('div', class_='js__product-link-for-product-id')
        print(f"[Worker {worker_id}] Found {len(items)} items with 'js__product-link-for-product-id'")
        
        if not items:
            items = soup.find_all('div', attrs={'data-product-id': True})
            print(f"[Worker {worker_id}] Found {len(items)} items with 'data-product-id'")
        
        if not items:
            items = soup.find_all('div', class_=lambda x: x and 're__card' in x)
            print(f"[Worker {worker_id}] Found {len(items)} items with 're__card'")
        
        if not items:
            items = soup.find_all('div', class_=lambda x: x and 'js__card' in x)
            print(f"[Worker {worker_id}] Found {len(items)} items with 'js__card'")
        
        # NẾU VẪN KHÔNG CÓ → DEBUG
        if not items:
            print(f"[Worker {worker_id}] ⚠️ NO ITEMS FOUND - DEBUGGING...")
            
            debug_file = f"debug_page_{page}_worker_{worker_id}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(driver.page_source)
            print(f"[Worker {worker_id}] → Saved HTML to {debug_file}")
            
            screenshot_file = f"debug_page_{page}_worker_{worker_id}.png"
            driver.save_screenshot(screenshot_file)
            print(f"[Worker {worker_id}] → Saved screenshot to {screenshot_file}")
            
            print(f"[Worker {worker_id}] → Page title: {soup.find('title').get_text() if soup.find('title') else 'No title'}")
            print(f"[Worker {worker_id}] → URL hiện tại: {driver.current_url}")
            
            all_divs = soup.find_all('div', class_=True)[:10] 
            print(f"[Worker {worker_id}] → First 10 div classes:")
            for i, div in enumerate(all_divs, 1):
                classes = div.get('class', [])
                print(f"   {i}. {' '.join(classes)}")
            
            return []

        results = []
        for item in items:
            try:
                link_node = item.find('a', href=True)
                if not link_node:
                    continue

                href = link_node['href']
                if href.startswith('/'):
                    full_url = f"https://batdongsan.com.vn{href}"
                elif href.startswith('http'):
                    full_url = href
                else:
                    full_url = f"https://batdongsan.com.vn/{href}"

                results.append({
                    "url": full_url,
                    "page": page
                })

            except Exception as e:
                continue

        print(f"[Worker {worker_id}] ✓ Found {len(results)} items")
        return results

    except Exception as e:
        print(f"[Worker {worker_id}] Error: {e}")
        import traceback
        traceback.print_exc()
        return []

def worker_crawl_batch(pages, base_url, worker_id, headless=True):
    """Mỗi worker crawl một batch pages"""
    print(f"\n[Worker {worker_id}] Starting with {len(pages)} pages...")
    
    driver = None
    results = []
    
    try:
        # Init browser với lock để tránh conflict
        with browser_init_lock:
            print(f"[Worker {worker_id}] Initializing browser...")
            driver = init_browser(headless=headless, worker_id=worker_id)
            time.sleep(2)
        
        # Crawl từng trang
        for i, page in enumerate(pages, 1):
            print(f"\n[Worker {worker_id}] [{i}/{len(pages)}] Crawling page {page}...")
            
            page_data = crawl_one_page_selenium(driver, page, base_url, worker_id)
            results.extend(page_data)
            
            # Delay giữa các requests
            time.sleep(random.uniform(2, 4))
        
        print(f"\n[Worker {worker_id}] Completed: {len(results)} items total")
        return (worker_id, results)
        
    except Exception as e:
        print(f"\n[Worker {worker_id}] Error: {e}")
        import traceback
        traceback.print_exc()
        return (worker_id, results)
        
    finally:
        if driver:
            print(f"[Worker {worker_id}] Closing browser...")
            try:
                driver.quit()
            except:
                pass


def get_list_links_selenium_parallel(
    base_url,
    max_pages=100,
    max_workers=2,
    batch_size=10,
    resume=True,
    headless=True
):
    """Crawl nhiều trang song song bằng Selenium"""
    print(f"Starting PARALLEL Selenium crawl: {base_url}")
    print(f"Workers: {max_workers} | Max pages: {max_pages} | Batch size: {batch_size}")
    print("=" * 70)

    if resume:
        seen_urls = load_existing_urls(LINKS_FILE)
    else:
        seen_urls = set()
        try:
            open(LINKS_FILE, "w").close()
        except:
            pass

    all_data = []
    consecutive_empty = 0
    max_consecutive_empty = 10

    # Crawl theo từng batch
    for batch_start in range(1, max_pages + 1, batch_size):
        batch_end = min(batch_start + batch_size, max_pages + 1)
        pages_in_batch = list(range(batch_start, batch_end))
        
        print(f"\n{'='*70}")
        print(f"BATCH: Pages {batch_start}-{batch_end-1}")
        print(f"{'='*70}")
        
        # Chia pages trong batch cho các workers
        pages_per_worker = len(pages_in_batch) // max_workers
        if pages_per_worker == 0:
            pages_per_worker = 1
        
        worker_chunks = []
        for i in range(0, len(pages_in_batch), pages_per_worker):
            chunk = pages_in_batch[i:i + pages_per_worker]
            if chunk:
                worker_chunks.append(chunk)
        
        # Giới hạn số workers
        if len(worker_chunks) > max_workers:
            worker_chunks = worker_chunks[:max_workers]
        
        print(f"Split into {len(worker_chunks)} workers:")
        for i, chunk in enumerate(worker_chunks):
            print(f"  Worker {i+1}: pages {chunk}")
        
        # Chạy parallel cho batch này
        batch_results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(worker_crawl_batch, chunk, base_url, i+1, headless): i+1
                for i, chunk in enumerate(worker_chunks)
            }
            
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    _, worker_results = future.result()
                    batch_results.extend(worker_results)
                except Exception as e:
                    print(f"Worker {worker_id} error: {e}")
        
        # Lọc và lưu URLs mới
        new_items = []
        for item in batch_results:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                new_items.append(item)
        
        if new_items:
            all_data.extend(new_items)
            save_batch_to_jsonl(new_items, LINKS_FILE)
            print(f"\n✓ Saved {len(new_items)} new URLs | Total: {len(seen_urls)}")
            consecutive_empty = 0
        else:
            print(f"\n✗ No new URLs in this batch")
            consecutive_empty += 1
        
        if consecutive_empty >= 3:  # 3 batches liên tiếp rỗng
            print(f"\nStopping: {consecutive_empty} consecutive empty batches")
            break
        
        # Nghỉ giữa các batches
        if batch_end < max_pages + 1:
            rest_time = random.uniform(5, 10)
            print(f"\n>>> Resting {rest_time:.0f}s before next batch...\n")
            time.sleep(rest_time)

    return all_data

def get_list_links_selenium(
    base_url, 
    max_pages=100, 
    batch_size=10, 
    resume=True,
    headless=True
):
    """Crawl nhiều trang bằng Selenium (TUẦN TỰ - chậm)"""
    print(f"Starting Selenium crawl: {base_url}")
    print(f"Max pages: {max_pages} | Batch size: {batch_size}")
    print("=" * 70)

    if resume:
        seen_urls = load_existing_urls(LINKS_FILE)
    else:
        seen_urls = set()
        try:
            open(LINKS_FILE, "w").close()
        except:
            pass

    # Khởi tạo browser
    driver = init_browser(headless=headless, worker_id=0)
    
    try:
        all_data = []
        consecutive_empty = 0
        max_consecutive_empty = 10

        for page in range(1, max_pages + 1):
            print(f"\nPage {page}/{max_pages}")
            
            page_data = crawl_one_page_selenium(driver, page, base_url)
            
            if not page_data:
                consecutive_empty += 1
            else:
                consecutive_empty = 0
            
            new_items = []
            for item in page_data:
                if item["url"] not in seen_urls:
                    seen_urls.add(item["url"])
                    new_items.append(item)
            
            if new_items:
                all_data.extend(new_items)
                save_batch_to_jsonl(new_items, LINKS_FILE)
                print(f"Saved {len(new_items)} new URLs | Total: {len(seen_urls)}")
            else:
                print(f"No new URLs")
            
            if consecutive_empty >= max_consecutive_empty:
                print(f"\nStopping: {consecutive_empty} consecutive empty pages")
                break
            
            # Delay giữa các requests
            time.sleep(random.uniform(2, 4))
            
            # Progress report
            if page % batch_size == 0:
                print(f"\n>>> Progress: {page}/{max_pages} pages | {len(all_data)} new URLs")

        return all_data
        
    finally:
        print("\nClosing browser...")
        driver.quit()


def extract_bg_image(style: str):
    if not style:
        return None
    match = re.search(r'url\(["\']?(.*?)["\']?\)', style)
    return match.group(1) if match else None


def parse_images(soup: BeautifulSoup):
    images = set()
    slide_imgs = soup.select(
        'div.re__pr-media-slide li.js__media-item-container div.re__pr-image-cover'
    )

    for div in slide_imgs:
        style = div.get('style', '')
        img_url = extract_bg_image(style)
        if img_url:
            images.add(img_url)

    thumb_imgs = soup.select(
        'div.re__media-thumbs div.re__media-thumb-item'
    )

    for div in thumb_imgs:
        style = div.get('style', '')
        img_url = extract_bg_image(style)
        if img_url:
            images.add(img_url)

    return list(images)


if __name__ == "__main__":
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Ví dụ các URL categories
    
    print(f"\n{'='*70}")
    print(f"Crawling: {BASE_URL}")
    print(f"{'='*70}\n")
    
    all_links = get_list_links_selenium_parallel(
        base_url=BASE_URL,
        max_pages=200,      # Crawl tối đa 100 trang
        max_workers=2,      # 2 workers song song
        batch_size=10,      # Mỗi batch 10 trang
        resume=True         # Tiếp tục từ lần chạy trước
    )
    
    # all_links = get_list_links_selenium(
    #     base_url=BASE_URL,
    #     max_pages=100,
    #     batch_size=10,
    #     resume=True
    # )
    
    save_batch_to_jsonl(all_links, LINKS_FILE)
        
    print(f"\nTotal links collected: {len(all_links)}")
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")