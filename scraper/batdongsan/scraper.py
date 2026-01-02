"""
BatDongSan.vn Scraper - Fixed version that actually works!
Only crawls today's listings
"""

import requests
from bs4 import BeautifulSoup
import json
import time
import random
import re
import os
import logging
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta

from .config import BatDongSanConfig


class BatDongSanScraper:
    """Scraper for BatDongSan.vn - Only today's listings"""
    
    def __init__(self, config: Optional[BatDongSanConfig] = None):
        self.config = config or BatDongSanConfig()
        self.logger = self._setup_logger()
        self.today = date.today()
        
    def _setup_logger(self) -> logging.Logger:
        """Setup logger with console output"""
        logger = logging.getLogger(self.__class__.__name__)
        if logger.handlers:
            return logger
            
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        return logger
    
    # ========================================================================
    # PUBLIC API - LISTING CRAWLING
    # ========================================================================
    
    def crawl_listings(
        self, 
        start_page: int = 1, 
        end_page: int = 50, 
        resume: bool = True,
        only_today: bool = True
    ) -> List[Dict]:
        """
        Crawl listing pages to collect property URLs
        
        Args:
            start_page: Starting page number (1-based)
            end_page: Ending page number (inclusive)
            resume: If True, skip URLs that already exist in file
            only_today: If True, only collect listings posted today
            
        Returns:
            List of newly collected URL dictionaries (today only)
        """
        self.logger.info(f"Starting listings crawl: pages {start_page}-{end_page}")
        if only_today:
            self.logger.info(f"Filter: Only listings from {self.today}")
        
        links_path = self._get_filepath(self.config.links_file)
        
        existing_data = []
        crawled_urls = set()
        
        if resume and os.path.exists(links_path):
            existing_data = self._load_json(links_path)
            crawled_urls = {item["url"] for item in existing_data if "url" in item}
            self.logger.info(f"Loaded {len(crawled_urls)} existing URLs")
        
        all_results = []
        found_old_post = False
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {
                executor.submit(self._crawl_single_listing_page, page, only_today): page
                for page in range(start_page, end_page + 1)
            }
            
            for future in as_completed(futures):
                page = futures[future]
                try:
                    page_data, has_old_posts = future.result()
                    
                    new_items = [
                        item for item in page_data 
                        if item["url"] not in crawled_urls
                    ]
                    
                    all_results.extend(new_items)
                    crawled_urls.update(item["url"] for item in new_items)
                    
                    if has_old_posts and only_today:
                        found_old_post = True
                    
                except Exception as e:
                    self.logger.error(f"[Page {page}] Failed: {e}")
        
        if all_results:
            combined_data = existing_data + all_results
            self._save_json(combined_data, links_path)
            self.logger.info(
                f"Collected {len(all_results)} new URLs (today) | "
                f"Total: {len(combined_data)}"
            )
        else:
            self.logger.info("No new URLs found today")
        
        if found_old_post and only_today:
            self.logger.info("Found old posts - stopping crawl (reached yesterday's listings)")
        
        return all_results
    
    # ========================================================================
    # PUBLIC API - DETAIL CRAWLING
    # ========================================================================
    
    def crawl_details(
        self, 
        urls: Optional[List[str]] = None, 
        resume: bool = True
    ) -> List[Dict]:
        """
        Crawl detail pages for property information
        
        Args:
            urls: List of URLs to crawl. If None, loads from links file
            resume: If True, skip URLs already in details file
            
        Returns:
            List of newly crawled property detail dictionaries
        """
        links_path = self._get_filepath(self.config.links_file)
        details_path = self._get_filepath(self.config.details_file)

        if urls is None:
            raw_data = self._load_json(links_path)
            urls = [item["url"] for item in raw_data if "url" in item]
        
        if not urls:
            self.logger.warning("No URLs to crawl!")
            return []
            
        self.logger.info(f"Total URLs available: {len(urls)}")
        
        existing_data = []
        crawled_urls = set()
        
        if resume and os.path.exists(details_path):
            existing_data = self._load_json(details_path)
            crawled_urls = {item["url"] for item in existing_data if "url" in item}
            self.logger.info(f"Already crawled: {len(crawled_urls)} URLs")

        urls_to_crawl = [url for url in urls if url not in crawled_urls]
        self.logger.info(f"URLs to crawl: {len(urls_to_crawl)}")
        
        if not urls_to_crawl:
            self.logger.info("All URLs have been crawled!")
            return []
        
        new_details = []
        total = len(urls_to_crawl)
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = [
                executor.submit(self._crawl_single_detail_page, url) 
                for url in urls_to_crawl
            ]

            for i, future in enumerate(as_completed(futures), 1):
                try:
                    data = future.result()
                    if data:
                        new_details.append(data)
                    
                    if i % 10 == 0 or i == total:
                        self.logger.info(
                            f"Progress: {i}/{total} "
                            f"({len(new_details)} successful)"
                        )
                        
                except Exception as e:
                    self.logger.error(f"Future error: {e}")
        
        if new_details:
            all_details = existing_data + new_details
            self._save_json(all_details, details_path)
            self.logger.info(
                f"Crawled {len(new_details)} new details | "
                f"Total: {len(all_details)}"
            )
        else:
            self.logger.warning("No new details collected")
        
        return new_details
    
    # ========================================================================
    # PRIVATE - LISTING PAGE CRAWLING
    # ========================================================================
    
    def _crawl_single_listing_page(self, page: int, only_today: bool = True) -> tuple:
        """
        Crawl a single listing page
        
        Args:
            page: Page number (1-based)
            only_today: If True, only collect today's listings
            
        Returns:
            Tuple of (list of items, has_old_posts flag)
        """
        if page == 1:
            url = f"{self.config.base_url}?sortValue=1"
        else:
            url = f"{self.config.base_url}/p{page}?sortValue=1"
        
        self.logger.debug(f"[Page {page}] Requesting {url}")
        
        try:
            response = requests.get(
                url, 
                headers=self.config.headers,
                timeout=self.config.request_timeout
            )
            
            if response.status_code != 200:
                self.logger.warning(f"[Page {page}] HTTP {response.status_code}")
                return [], False

            soup = BeautifulSoup(response.text, "html.parser")
            items = []
            has_old_posts = False
            
            # FIXED: Find <a class="card-cm"> directly (not div.card-container)
            cards = soup.find_all("a", class_="card-cm")
            
            for card in cards:
                # Card itself is <a> tag, get href directly
                href = card.get("href")
                if not href or not href.startswith("http"):
                    continue
                
                # Extract post date
                post_date_str = self._extract_post_date(card)
                post_date = self._parse_post_date(post_date_str)
                
                # Filter by date if only_today=True
                if only_today:
                    if post_date is None:
                        self.logger.debug(f"Could not parse date: '{post_date_str}'")
                        continue
                    
                    if post_date < self.today:
                        has_old_posts = True
                        self.logger.debug(f"[Page {page}] Old post: {post_date}")
                        continue
                    elif post_date > self.today:
                        # Future date? Skip
                        continue
                
                # Add to results
                items.append({
                    "url": href,
                    "page": page,
                    "post_date": post_date_str,
                    "parsed_date": str(post_date) if post_date else None,
                    "collected_at": datetime.now().isoformat()
                })
            
            if only_today:
                self.logger.info(f"[Page {page}] Found {len(items)} URLs (today only)")
            else:
                self.logger.info(f"[Page {page}] Found {len(items)} URLs")
            
            time.sleep(random.uniform(*self.config.page_delay))
            
            return items, has_old_posts
            
        except requests.exceptions.Timeout:
            self.logger.error(f"[Page {page}] Request timeout")
            return [], False
        except Exception as e:
            self.logger.error(f"[Page {page}] Error: {e}")
            return [], False
    
    def _extract_post_date(self, card_soup: BeautifulSoup) -> str:
        """
        Extract post date from listing card
        
        Args:
            card_soup: BeautifulSoup of a single card (<a> tag)
            
        Returns:
            Date string (e.g., "10 giờ trước", "1 ngày trước")
        """
        # Find <div class="time">
        time_div = card_soup.find("div", class_="time")
        if time_div:
            return time_div.get_text(strip=True)
        
        return ""
    
    def _parse_post_date(self, date_str: str) -> Optional[date]:
        """
        Parse date string to date object
        
        Args:
            date_str: Date string like "10 giờ trước", "1 ngày trước"
            
        Returns:
            date object or None if cannot parse
        """
        if not date_str:
            return None
        
        today = self.today
        
        # "Hôm nay"
        if "hôm nay" in date_str.lower():
            return today
        
        # "Hôm qua"
        if "hôm qua" in date_str.lower():
            return today - timedelta(days=1)
        
        # "X giờ trước" - still today
        if re.search(r"\d+\s*giờ\s*trước", date_str, re.IGNORECASE):
            return today
        
        # "X ngày trước"
        match = re.search(r"(\d+)\s*ngày\s*trước", date_str, re.IGNORECASE)
        if match:
            days_ago = int(match.group(1))
            return today - timedelta(days=days_ago)
        
        # "X tháng trước"
        match = re.search(r"(\d+)\s*tháng\s*trước", date_str, re.IGNORECASE)
        if match:
            months_ago = int(match.group(1))
            # Approximate: 1 month = 30 days
            return today - timedelta(days=months_ago * 30)
        
        # "X năm trước"
        match = re.search(r"(\d+)\s*năm\s*trước", date_str, re.IGNORECASE)
        if match:
            years_ago = int(match.group(1))
            # Approximate: 1 year = 365 days
            return today - timedelta(days=years_ago * 365)
        
        # "DD/MM/YYYY"
        match = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", date_str)
        if match:
            day, month, year = map(int, match.groups())
            try:
                return date(year, month, day)
            except ValueError:
                return None
        
        return None
    
    # ========================================================================
    # PRIVATE - DETAIL PAGE CRAWLING
    # ========================================================================
    
    def _crawl_single_detail_page(self, url: str) -> Optional[Dict]:
        """Crawl a single detail page"""
        try:
            response = requests.get(
                url,
                headers=self.config.headers,
                timeout=self.config.request_timeout
            )
            
            if response.status_code != 200:
                self.logger.warning(f"[DETAIL] HTTP {response.status_code} for {url}")
                return None
            
            soup = BeautifulSoup(response.text, "html.parser")
            data = self._parse_detail_page(soup)
            
            data["url"] = url
            data["crawled_at"] = datetime.now().isoformat()

            time.sleep(random.uniform(*self.config.detail_delay))
            
            return data
            
        except requests.exceptions.Timeout:
            self.logger.error(f"[DETAIL] Timeout for {url}")
            return None
        except Exception as e:
            self.logger.error(f"[DETAIL] Error for {url}: {e}")
            return None
    
    # ========================================================================
    # PRIVATE - HTML PARSING
    # ========================================================================
    
    def _parse_detail_page(self, soup: BeautifulSoup) -> Dict:
        """Parse detail page HTML"""
        data = {}
        
        try:
            header = soup.select_one("div.content h1")
            data["title"] = header.get_text(strip=True) if header else ""

            footer = soup.select_one("div.footer")
            if footer:
                address_text = footer.find(text=True, recursive=False)
                data["address"] = address_text.strip() if address_text else ""
                
                for box in footer.select("div.box-text"):
                    label = box.select_one("div.label")
                    value = box.select_one("div.value")
                    
                    if not label or not value:
                        continue
                    
                    key = label.get_text(strip=True).lower()
                    val = value.get_text(strip=True)
                    
                    if "giá" in key:
                        data["price"] = val
                    elif "diện tích" in key:
                        data["area"] = val
            
            data["description"] = self._parse_description(soup)
            data["category"] = self._parse_category(soup)
            data["images"] = self._parse_images(soup)
            data["detail_info"] = self._parse_detail_info(soup)
            
            date_elem = soup.find("div", class_="label", string="Ngày đăng")
            if date_elem:
                date_value = date_elem.find_next_sibling("div", class_="value")
                data["date_posted"] = date_value.text.strip() if date_value else ""
            else:
                data["date_posted"] = ""
            
        except Exception as e:
            self.logger.error(f"Error parsing detail: {e}")
        
        return data
    
    def _parse_description(self, soup: BeautifulSoup) -> str:
        """Parse description"""
        desc_div = soup.select_one("div#more1")
        if not desc_div:
            return ""
        
        parts = []
        for elem in desc_div.contents:
            if isinstance(elem, str):
                text = elem.strip()
                if text:
                    parts.append(text)
        
        description = "\n".join(parts)
        lines = []
        for line in description.splitlines():
            line = re.sub(r"\s+", " ", line)
            if line:
                lines.append(line)
        
        return "\n".join(lines)
    
    def _parse_category(self, soup: BeautifulSoup) -> str:
        """Parse category"""
        breadcrumbs = soup.find("div", class_="title mb-3 re__breadcrumb")
        categories = []
        
        if breadcrumbs:
            a_tags = breadcrumbs.find_all("a")
            for i, a in enumerate(a_tags):
                if i == 0:
                    continue
                category = a.get_text(strip=True)
                categories.append(category)
        
        return ", ".join(categories)
    
    def _parse_images(self, soup: BeautifulSoup) -> List[str]:
        """Parse images"""
        images = []
        swiper_wrapper = soup.select_one("div.swiper-wrapper")
        
        if not swiper_wrapper:
            return images
        
        img_tags = swiper_wrapper.select("img")
        
        for img in img_tags:
            lazy_src = img.get("lazy-src")
            data_src = img.get("data-src")
            src = img.get("src")
            
            final_src = lazy_src or data_src or src
            
            if not final_src or final_src.startswith("data:image"):
                continue
            
            if not final_src.startswith("http"):
                final_src = f"https://batdongsan.vn{final_src}"
            
            if final_src not in images:
                images.append(final_src)
        
        return images
    
    def _parse_detail_info(self, soup: BeautifulSoup) -> Dict:
        """Parse detail info table"""
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
    
    # ========================================================================
    # PRIVATE - FILE I/O
    # ========================================================================
    
    def _get_filepath(self, filename: str) -> str:
        """Get full filepath"""
        return os.path.join(self.config.output_dir, filename)
    
    def _load_json(self, filepath: str) -> List[Dict]:
        """Load JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return []
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {filepath}: {e}")
            return []
        except Exception as e:
            self.logger.error(f"Error loading {filepath}: {e}")
            return []
    
    def _save_json(self, data: List[Dict], filepath: str) -> None:
        """Save to JSON file"""
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            self.logger.debug(f"Saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving to {filepath}: {e}")
            raise
    
    # ========================================================================
    # PUBLIC API - PIPELINE
    # ========================================================================
    
    def run_full_pipeline(
        self, 
        start_page: int = 1, 
        end_page: int = 50,
        only_today: bool = True
    ) -> Dict:
        """Run full pipeline - only today's listings"""
        self.logger.info("=" * 70)
        self.logger.info("STARTING BATDONGSAN.VN SCRAPING PIPELINE")
        self.logger.info(f"Date: {self.today}")
        self.logger.info("=" * 70)
        
        start_time = datetime.now()
        
        self.logger.info("\nSTEP 1: Crawling Listings (Today Only)")
        new_listings = self.crawl_listings(
            start_page=start_page, 
            end_page=end_page,
            only_today=only_today
        )
        
        self.logger.info("\nSTEP 2: Crawling Details")
        new_details = self.crawl_details()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("PIPELINE COMPLETED")
        self.logger.info("=" * 70)
        self.logger.info(f"Duration: {duration:.2f}s ({duration/60:.1f} minutes)")
        self.logger.info(f"New listings (today): {len(new_listings)}")
        self.logger.info(f"New details: {len(new_details)}")
        
        return {
            "status": "success",
            "date": str(self.today),
            "new_listings": len(new_listings),
            "new_details": len(new_details),
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat()
        }


if __name__ == "__main__":
    scraper = BatDongSanScraper()
    result = scraper.run_full_pipeline(start_page=1, end_page=5, only_today=True)
    print(f"Date: {result['date']}")
    print(f"New listings: {result['new_listings']}")