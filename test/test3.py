import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
import random
import re
import os

BASE_URL = "https://batdongsan.vn/ban-nha-dat"
OUTPUT_FILE = "batdongsan_urls_vn.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
}

results = []
results_lock = None 


def crawl_page(page: int):
    if page == 1:
        url = BASE_URL
    else:
        url = f"{BASE_URL}/p{page}"

    print(f"[Page {page}] Crawling {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            print(f"[Page {page}] Status {resp.status_code}")
            return []

        soup = BeautifulSoup(resp.text, "html.parser")

        items = []
        for a in soup.select("div.card-container a.card-cm"):
            href = a.get("href")
            if href and href.startswith("http"):
                items.append({
                    "url": href,
                    "page": page
                })

        print(f"[Page {page}] Found {len(items)} URLs")
        time.sleep(random.uniform(1.5, 3.0))
        return items

    except Exception as e:
        print(f"[Page {page}] Error: {e}")
        return []


def crawl_parallel(start_page=1, end_page=5, max_workers=3):
    all_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(crawl_page, page): page
            for page in range(start_page, end_page + 1)
        }

        for future in as_completed(futures):
            page = futures[future]
            try:
                data = future.result()
                all_results.extend(data)
            except Exception as e:
                print(f"[Page {page}] Future error: {e}")

    return all_results

def get_category(soup: BeautifulSoup):
    breadcrumbs = soup.find("div", class_="title mb-3 re__breadcrumb")
    categories = []
    a_tags = breadcrumbs.find_all("a") if breadcrumbs else []
    
    for i, a in enumerate(a_tags):
        if i == 0:
            continue
        
        category = a.get_text(strip=True)
        print(f"Category found: {category}")
        categories.append(category)
        
    return ", ".join(categories)


def clean_description(text: str):
    lines = []
    for line in text.splitlines():
        line = re.sub(r"^[·\-\*\s]+", "", line) 
        line = re.sub(r"\s+", " ", line)        
        if line:
            lines.append(line)
    return "\n".join(lines)

def parse_description(soup):
    desc_div = soup.select_one("div#more1")
    if not desc_div:
        return ""

    parts = []

    for elem in desc_div.contents:
        if isinstance(elem, str):
            text = elem.strip()
            if text:
                parts.append(text)
        elif elem.name == "br":
            continue

    return "\n".join(parts)


def parse_detail_info_table(soup):
    detail_info = {}
    lines = soup.select("div.line")
    for line in lines:
        label = line.select_one(".line-label")
        value = line.select_one(".line-text")
        
        if label and value:
            key = label.get_text(strip=True)
            val = value.get_text(strip=True)
            detail_info[key] = val
    return detail_info

def parse_images(soup):
    images = []

    swiper_wrapper = soup.select_one("div.swiper-wrapper")
    
    if not swiper_wrapper:
        print("Container swiper-wrapper not found")
        return images

    img_tags = swiper_wrapper.select("img")

    for i, img in enumerate(img_tags):

        lazy_src = img.get("lazy-src")
        data_src = img.get("data-src")
        src = img.get("src")

        final_src = lazy_src or data_src or src

        if not final_src or final_src.startswith("data:image"):
            print("Skip")
            continue

        if not final_src.startswith("http"):
            final_src = f"{BASE_URL}{final_src}"

        if final_src not in images:
            images.append(final_src)
            
    return images

def write_to_file(file_path, data):
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error writing to file: {e}")

def load_from_file(file_path):
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data
    except Exception as e:
        print(f"Error loading from file: {e}")
        return []

        
def get_detail(soup: BeautifulSoup):
    try:
        data = {}

        header = soup.select_one("div.content h1")
        data["title"] = header.get_text(strip=True) if header else ""

        footer = soup.select_one("div.footer")
        if not footer:
            return data

        address_text = footer.find(text=True, recursive=False)
        data["address"] = address_text.strip() if address_text else ""

        for box in footer.select("div.box-text"):
            label = box.select_one("div.label")
            value = box.select_one("div.value")

            if not label or not value:
                continue

            key = label.get_text(strip=True)
            val = value.get_text(strip=True)

            if "giá" in key.lower():
                data["price"] = val
            elif "diện tích" in key.lower():
                data["area"] = val

        data['description'] = clean_description(parse_description(soup))
        data['category'] = get_category(soup)
        data['images'] = parse_images(soup)
        data['detail_info'] = parse_detail_info_table(soup)
        
        date = soup.find("div", class_="label", string="Ngày đăng").find_next_sibling("div", class_="value").text
        data['date_posted'] = date.strip() if date else ""
        return data

    except Exception as e:
        print(f"Error getting detail: {e}")
        return {}

def crawl_detail(url):
    print(f"[DETAIL] Crawling {url}")

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        if resp.status_code != 200:
            print(f"[DETAIL] Status {resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        data = get_detail(soup)

        data["url"] = url

        time.sleep(random.uniform(1.2, 2.5))  
        return data

    except Exception as e:
        print(f"[DETAIL] Error {url}: {e}")
        return None

def crawl_details_parallel(urls, max_workers=2):
    results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(crawl_detail, url)
            for url in urls
        ]

        for future in as_completed(futures):
            data = future.result()
            if data:
                results.append(data)

    return results


if __name__ == "__main__":
    INPUT_FILE = "batdongsan_urls_vn.json"
    OUTPUT_FILE = "batdongsan_detail_vn.json"
    raw_data = load_from_file(INPUT_FILE)
    all_urls = [item["url"] for item in raw_data if "url" in item]
    print(f"Total URLs in input file: {len(all_urls)}")

    existing_data = []
    crawled_urls = set()
    
    if os.path.exists(OUTPUT_FILE):
        existing_data = load_from_file(OUTPUT_FILE)
        crawled_urls = {item["url"] for item in existing_data if "url" in item}
        print(f" Found {len(crawled_urls)} URLs already crawled")
    else:
        print(f"No existing output file found, will crawl all URLs")

    urls_to_crawl = [url for url in all_urls if url not in crawled_urls]
    print(f"URLs to crawl: {len(urls_to_crawl)}")

    if not urls_to_crawl:
        print("All URLs have been crawled!")
    else:
        new_details = crawl_details_parallel(urls_to_crawl, max_workers=2)
        print(f"\n Successfully crawled {len(new_details)} new listings")
        all_details = existing_data + new_details

        write_to_file(OUTPUT_FILE, all_details)
        print(f"Total {len(all_details)} listings saved to {OUTPUT_FILE}")
        print(f"- Previous: {len(existing_data)}")
        print(f"- New: {len(new_details)}")