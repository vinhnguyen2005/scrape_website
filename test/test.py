import re
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


URL = "https://batdongsan.com.vn/ban-nha-biet-thu-lien-ke-xa-long-hung-prj-vinhomes-ocean-park-2/chiet-khau-20-25-quy-can-dep-va-re-t-du-an-nop-30-n-goi-xem-du-an-pr44861715"


def parse_images(soup: BeautifulSoup, resize_to='1200x900'):
    images = []
    
    thumbs_container = soup.find('div', class_=lambda x: x and 're__media-thumbs' in x and 'js__media-thumbs' in x)
    
    if not thumbs_container:
        print("Không tìm thấy container re__media-thumbs js__media-thumbs")
        return images
    
    print(f"✓ Found thumbs container")
    
    # Tìm tất cả img trong container này
    img_tags = thumbs_container.find_all('img')
    print(f"✓ Found {len(img_tags)} img tags")
    
    for i, img in enumerate(img_tags, 1):
        # Ưu tiên data-src, nếu không có thì lấy src
        img_url = img.get('data-src') or img.get('src')
        
        if img_url:
            # Thay đổi kích thước nếu cần
            if resize_to:
                # Thay thế phần /resize/200x200/ bằng size mới
                img_url = re.sub(r'/resize/\d+x\d+/', f'/resize/{resize_to}/', img_url)
            
            if img_url not in images:
                images.append(img_url)
                print(f"  {i}. {img_url}")
    
    return images


def main():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    driver = webdriver.Chrome(options=options)
    
    try:
        print(f"Navigating to: {URL}\n")
        driver.get(URL)

        print("Waiting for page to load...")
        WebDriverWait(driver, 15).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div.re__media-thumbs"))
        )

        time.sleep(3)
        
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(1)

        soup = BeautifulSoup(driver.page_source, "html.parser")

        print("\n=== EXTRACTING IMAGES FROM re__media-thumbs ===\n")
        
        images = parse_images(soup, resize_to='1200x900')
        
        print(f"\n=== TOTAL: {len(images)} images ===\n")
        
        if images:
            print("Final list:")
            for i, img in enumerate(images, 1):
                print(f"{i}. {img}")
        else:
            print("No images found!")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()