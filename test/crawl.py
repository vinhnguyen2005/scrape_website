import requests
from bs4 import BeautifulSoup
import time
import json
from datetime import datetime
import random
import threading

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

LINKS_FILE = "alonhadat_links.jsonl"
DETAILS_FILE = "alonhadat_details.jsonl"
BASE_URL = "https://alonhadat.com.vn"

browser_init_lock = threading.Lock()

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Referer': 'https://alonhadat.com.vn/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7',
}

DELAY_CONFIG = {
    'page_crawl': (5, 7),           # Delay giữa các trang listing (tăng từ 3-10)
    'batch_crawl': (5, 10),          # Delay giữa các batch (tăng từ 4)
    'detail_initial_load': (3, 5),  # Delay sau khi load trang detail (tăng từ 5-12)
    'detail_between_items': (8, 12),# Delay giữa các item detail (tăng từ 10-20)
    'homepage_visit': (3, 5),       # Delay sau khi visit homepage
    'retry_delay': (30, 60),         # Delay khi gặp lỗi cần retry
    'captcha_recheck': 2,            # Delay khi check captcha
}

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

def crawl_one_page(page, base_url):
    url = base_url if page == 1 else f"{base_url}/trang-{page}"
    print(f"Page {page}: {url}")
    time.sleep(random.uniform(3, 10)) 
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        
        if response.status_code != 200:
            print(f"HTTP {response.status_code}")
            return []
            
        soup = BeautifulSoup(response.content, 'html.parser')
        items = soup.find_all('article', class_='property-item')
        
        if not items:
            print(f"No items found")
            return []

        results = []
        for item in items:
            try:
                link_node = item.find('a', class_='link')
                
                if not link_node or not link_node.has_attr('href'):
                    continue

                href = link_node['href']
                full_url = f"{BASE_URL}{href}" if href.startswith('/') else href

                post_date = "N/A"
                time_node = item.find('time', class_='created-date')
                if time_node and time_node.has_attr('datetime'):
                    post_date = time_node['datetime']

                results.append({
                    "url": full_url,
                    "post_date": post_date,
                    "page": page
                })

            except Exception as e:
                print(f"Error parsing item: {e}")
                continue

        print(f"Found {len(results)} items")
        return results

    except Exception as e:
        print(f"Error: {e}")
        return []


def get_list_links_parallel(
    base_url, max_pages=100, max_workers=2, batch_size=20, resume=True
):
    print(f"Starting crawl: {base_url}")
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

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch_start in range(1, max_pages + 1, batch_size):
            batch_end = min(batch_start + batch_size, max_pages + 1)

            print(f"\nBatch {batch_start}-{batch_end-1}")

            futures = {
                executor.submit(crawl_one_page, page, base_url): page
                for page in range(batch_start, batch_end)
            }

            batch_data = []
            batch_empty_count = 0

            for future in as_completed(futures):
                page_data = future.result()

                if not page_data:
                    batch_empty_count += 1
                    consecutive_empty += 1
                else:
                    consecutive_empty = 0

                for item in page_data:
                    if item["url"] not in seen_urls:
                        seen_urls.add(item["url"])
                        batch_data.append(item)

            if batch_data:
                all_data.extend(batch_data)
                save_batch_to_jsonl(batch_data, LINKS_FILE)
                print(
                    f"Saved {len(batch_data)} new URLs | Total unique: {len(seen_urls)}"
                )
            
            else:
                print(f"No new URLs in this batch")

            print(f"Empty pages: {batch_empty_count}/{batch_size}")

            if consecutive_empty >= max_consecutive_empty:
                print(f"\nStopping: {consecutive_empty} consecutive empty pages")
                break

            if batch_end < max_pages + 1:
                time.sleep(4)

    return all_data


def save_batch_to_jsonl(items, file_name):
    with open(file_name, 'a', encoding='utf-8') as f:
        for item in items:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


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


def init_selenium_browser(headless=True, worker_id=0):
    chrome_options = Options()
    
    chrome_options.add_argument('--disable-blink-features=AutomationControlled')
    chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    if headless:
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-gpu')
    else:
        offset = worker_id * 60
        chrome_options.add_argument(f'--window-position={offset},{offset}')

    chrome_options.add_argument('--disable-notifications')
    chrome_options.add_argument('--disable-popup-blocking')
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    if not headless:
        driver.set_window_size(1200, 800)
    
    return driver


def check_and_handle_captcha(driver, worker_id=0):
    try:
        if "xac-thuc-nguoi-dung" in driver.current_url:
            print(f"\n[Worker {worker_id}] " + "="*60)
            print(f"[Worker {worker_id}] !!! CAPTCHA DETECTED !!!")
            print(f"[Worker {worker_id}] " + "="*60)
            
            # Lưu cookies và URL
            cookies = driver.get_cookies()
            captcha_url = driver.current_url
            
            # Đóng headless browser
            print(f"[Worker {worker_id}] Restarting browser with UI...")
            driver.quit()
            
            # Mở browser MỚI với UI
            driver = init_selenium_browser(headless=False, worker_id=worker_id)
            
            # Restore cookies
            driver.get('https://alonhadat.com.vn')
            time.sleep(DELAY_CONFIG['captcha_recheck'])
            for cookie in cookies:
                try:
                    driver.add_cookie(cookie)
                except:
                    pass
            
            # Quay lại trang CAPTCHA
            driver.get(captcha_url)
            time.sleep(DELAY_CONFIG['captcha_recheck'])
            
            print(f"[Worker {worker_id}] Browser opened! Please solve CAPTCHA manually.")
            print(f"[Worker {worker_id}] Waiting for CAPTCHA to be solved...")
            
            # Đợi user giải CAPTCHA
            while "xac-thuc-nguoi-dung" in driver.current_url:
                time.sleep(1)
            
            print(f"[Worker {worker_id}] CAPTCHA solved! Continuing...")
            time.sleep(DELAY_CONFIG['captcha_recheck'])
            
            return driver
        
        return driver
        
    except Exception as e:
        print(f"[Worker {worker_id}] Error handling CAPTCHA: {e}")
        return driver


def parse_detail_info_table(soup):
    detail_info = {}
    table = soup.find('table', cellspacing='0')
    if not table:
        return detail_info
    
    rows = table.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        for i in range(0, len(cols), 2):
            if i + 1 < len(cols):
                key = cols[i].get_text(strip=True)
                value_td = cols[i + 1]
                img_tag = value_td.find('img')
                if img_tag:
                    detail_info[key] = True
                    continue

                value = value_td.get_text(strip=True)
                if value == '---':
                    detail_info[key] = False
                elif value:
                    detail_info[key] = value
    
    return detail_info


def parse_images(soup):
    images = []
    image_section = soup.find('section', class_='images')
    if not image_section:
        return images
    
    main_img = image_section.find('div', class_='imageview')
    if main_img:
        img_tag = main_img.find('img', id='limage')
        if img_tag and img_tag.has_attr('src'):
            src = img_tag['src']
            full_url = f"{BASE_URL}{src}" if src.startswith('/') else src
            images.append(full_url)
    
    image_list = image_section.find('ul', class_='image-list')
    if image_list:
        img_tags = image_list.find_all('img', class_='limage')
        for img in img_tags:
            if img.has_attr('src'):
                src = img['src']
                full_url = f"{BASE_URL}{src}" if src.startswith('/') else src
                if full_url not in images:
                    images.append(full_url)
    
    return images


def parse_category_from_soup(soup):
    try:
        nav = soup.find('nav', {'id': 'ctl00_content_top_link'})
        if not nav:
            return None
        
        list_items = nav.find_all('li', {'itemprop': 'itemListElement'})
        categories = []
        
        for index, li in enumerate(list_items):
            if index == 0:
                continue
            
            link = li.find('a')
            if link:
                span = link.find('span', {'itemprop': 'name'})
                text = span.get_text(strip=True) if span else link.get_text(strip=True)
                categories.append(text)
        
        return ', '.join(categories)
    except Exception as e:
        return None

def get_detail_with_selenium(driver, url, worker_id=0, retry_count=0):
    max_retries = 2
    
    try:
        print(f"[Worker {worker_id}] Navigating to: {url}")
        driver.get(url)

        delay = random.uniform(*DELAY_CONFIG['detail_initial_load'])
        print(f"[Worker {worker_id}] Waiting {delay:.1f}s for page load...")
        time.sleep(delay)
        
        driver = check_and_handle_captcha(driver, worker_id)
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        header = soup.find('header', class_='title')
        if header:
            h1 = header.find('h1')
            title = h1.get_text(strip=True) if h1 else "N/A"
        else:
            title = "N/A"
        
        if title == "N/A" and retry_count < max_retries:
            print(f"[Worker {worker_id}]  Page not fully loaded, retrying...")
            extra_delay = random.uniform(5, 10)
            time.sleep(extra_delay)
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            header = soup.find('header', class_='title')
            if header:
                h1 = header.find('h1')
                title = h1.get_text(strip=True) if h1 else "N/A"
        
        time_tag = soup.find('time', class_='date')
        post_date = time_tag.get('datetime') if time_tag and time_tag.has_attr('datetime') else "N/A"
        
        description_section = soup.find('section', class_='detail text-content')
        if description_section:
            description_p = description_section.find('p', {'itemprop': 'description'})
            description = description_p.get_text(strip=True) if description_p else "N/A"
        else:
            description = "N/A"
        
        price_span = soup.find('span', class_='price')
        if price_span:
            price_data = price_span.find('data', itemprop='price')
            price = price_data.get('value') if price_data and price_data.has_attr('value') else "N/A"
        else:
            price = "N/A"
        
        area_span = soup.find('span', class_='area')
        if area_span:
            area_value = area_span.find('span', class_='value')
            area = area_value.get_text(strip=True) if area_value else "N/A"
        else:
            area = "N/A"

        address_tag = soup.find('address', class_='current-address')
        if address_tag:
            street = address_tag.find('span', itemprop='streetAddress')
            locality = address_tag.find('span', itemprop='addressLocality')
            region = address_tag.find('span', itemprop='addressRegion')
            
            address = {
                'street': street.get_text(strip=True) if street else "N/A",
                'locality': locality.get_text(strip=True) if locality else "N/A",
                'region': region.get_text(strip=True) if region else "N/A"
            }
        else:
            address = {'street': "N/A", 'locality': "N/A", 'region': "N/A"}

        old_address = "N/A"
        old_address_tag = soup.find('p', class_='old-address')
        if old_address_tag:
            old_address = old_address_tag.get_text(strip=True)
            
        address['old_address'] = old_address
        category = parse_category_from_soup(soup)
        detail_info = parse_detail_info_table(soup)
        images = parse_images(soup)
        
        return (driver, {
            'url': url,
            'category': category,
            'title': title,
            'post_date': post_date,
            'description': description,
            'price': price,
            'area': area,
            'address': address,
            'detail_info': detail_info,
            'images': images
        })
        
    except Exception as e:
        print(f"[Worker {worker_id}] Error: {e}")

        if retry_count < max_retries:
            retry_delay = random.uniform(*DELAY_CONFIG['retry_delay'])
            print(f"[Worker {worker_id}] Retrying in {retry_delay:.1f}s... (attempt {retry_count + 1}/{max_retries})")
            time.sleep(retry_delay)
            return get_detail_with_selenium(driver, url, worker_id, retry_count + 1)
        
        return (driver, None)


def worker_crawl(links_chunk, worker_id, start_headless=True):
    print(f"\n[Worker {worker_id}] Starting with {len(links_chunk)} URLs...")
    
    driver = None
    success_count = 0
    error_count = 0
    
    try:
        with browser_init_lock:
            print(f"[Worker {worker_id}] Initializing browser...")
            driver = init_selenium_browser(headless=start_headless, worker_id=worker_id)
            time.sleep(2)
        
        print(f"[Worker {worker_id}] Visiting homepage...")
        driver.get('https://alonhadat.com.vn')
        time.sleep(random.uniform(5, 8))
        delay = random.uniform(*DELAY_CONFIG['homepage_visit'])
        print(f"[Worker {worker_id}]  Waiting {delay:.1f}s...")
        time.sleep(delay)
        driver = check_and_handle_captcha(driver, worker_id)
        
        for i, link in enumerate(links_chunk, 1):
            print(f"\n[Worker {worker_id}] [{i}/{len(links_chunk)}]")
            
            driver, detail = get_detail_with_selenium(driver, link['url'], worker_id)
            
            if detail and detail['title'] != 'N/A':
                save_batch_to_jsonl([detail], DETAILS_FILE)
                success_count += 1
                print(f"[Worker {worker_id}] {detail['title'][:40]}...")
            else:
                error_count += 1
                print(f"[Worker {worker_id}] Failed")
            
            delay = random.uniform(10, 20)
            time.sleep(delay)
        
        print(f"\n[Worker {worker_id}] Completed: {success_count} success, {error_count} failed")
        return (worker_id, success_count, error_count)
        
    except Exception as e:
        print(f"\n[Worker {worker_id}] Error: {e}")
        import traceback
        traceback.print_exc()
        return (worker_id, success_count, error_count)
        
    finally:
        if driver:
            print(f"[Worker {worker_id}] Closing browser...")
            try:
                driver.quit()
            except:
                pass


def crawl_with_selenium_parallel(links, max_workers=2, limit=None, start_headless=True):
    print(f"\nStarting PARALLEL Selenium crawl")
    print(f"Workers: {max_workers}")
    print("="*70)
    
    crawled_urls = load_existing_urls(DETAILS_FILE)
    links_to_crawl = [link for link in links if link['url'] not in crawled_urls]
    
    if limit:
        links_to_crawl = links_to_crawl[:limit]
    
    print(f"URLs to crawl: {len(links_to_crawl)}")
    
    if not links_to_crawl:
        print("No new URLs to crawl")
        return

    chunk_size = max(1, len(links_to_crawl) // max_workers)
    chunks = []
    for i in range(0, len(links_to_crawl), chunk_size):
        chunks.append(links_to_crawl[i:i + chunk_size])

    if len(chunks) > max_workers:
        chunks = chunks[:max_workers]
    
    print(f"Split into {len(chunks)} chunks:")
    for i, chunk in enumerate(chunks):
        print(f"  Worker {i+1}: {len(chunk)} URLs")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(worker_crawl, chunk, i+1, start_headless): i+1
            for i, chunk in enumerate(chunks)
        }
        
        total_success = 0
        total_error = 0
        
        for future in as_completed(futures):
            worker_id = futures[future]
            try:
                result = future.result()
                _, success, error = result
                total_success += success
                total_error += error
            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
    
    print(f"\n{'='*70}")
    print(f"ALL WORKERS COMPLETED")
    print(f"{'='*70}")
    print(f"Total success: {total_success}")
    print(f"Total failed: {total_error}")
    if len(links_to_crawl) > 0:
        print(f"Success rate: {total_success/len(links_to_crawl)*100:.1f}%")


if __name__ == "__main__":
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    base = 'https://alonhadat.com.vn/can-ban-nha-dat'
    # all_links = get_list_links_parallel(
    #     base_url=base,
    #     max_pages=200,
    #     max_workers=2,
    #     batch_size=20,
    #     resume=True
    # )
    # print(f"\nTotal links collected: {len(all_links)}")
    
    all_links = load_links_from_file(LINKS_FILE)
    
    if all_links:
        crawl_with_selenium_parallel(
            all_links, 
            max_workers=2,        
            limit=len(all_links),             
            start_headless=True   
        )
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")