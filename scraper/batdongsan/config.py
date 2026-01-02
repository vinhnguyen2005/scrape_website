
from pathlib import Path
from typing import Dict, Tuple
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class BatDongSanConfig:
    """Configuration for BatDongSan.vn scraper"""
    
    # URLs
    base_url: str = "https://batdongsan.vn/ban-nha-dat"
    
    # Base directory
    output_dir: str = "data/batdongsan/raw"
    links_file_pattern: str = "batdongsan_links_{date}.json"
    details_file_pattern: str = "batdongsan_details_{date}.json"
    date_format: str = "%Y-%m-%d"
    
    # Crawling parameters
    max_workers: int = 2
    request_timeout: int = 20
    
    # Delays (min, max) in seconds
    page_delay: Tuple[float, float] = (1.5, 3.0)
    detail_delay: Tuple[float, float] = (1.2, 2.5)
    
    # HTTP headers
    headers: Dict[str, str] = field(default_factory=dict)
    
    # Runtime properties (set after init)
    _links_file: str = field(init=False, default="")
    _details_file: str = field(init=False, default="")
    
    def __post_init__(self):
        """Initialize after dataclass creation"""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        if not self.headers:
            self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                             "AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            }
        
        today = datetime.now().strftime(self.date_format)
        self._links_file = self.links_file_pattern.format(date=today)
        self._details_file = self.details_file_pattern.format(date=today)
    
    @property
    def links_file(self) -> str:
        return self._links_file
    
    @property
    def details_file(self) -> str:
        return self._details_file
    
    def set_date(self, date: str):
        self._links_file = self.links_file_pattern.format(date=date)
        self._details_file = self.details_file_pattern.format(date=date)
    
    def get_links_path(self) -> str:
        return str(Path(self.output_dir) / self.links_file)
    
    def get_details_path(self) -> str:
        return str(Path(self.output_dir) / self.details_file)


# ============================================================================
# USAGE EXAMPLES
# ============================================================================

if __name__ == "__main__":
    print("="*70)
    print("BatDongSanConfig Examples")
    print("="*70)
    
    # Example 1: Default config (today's date)
    print("\n1. Default config:")
    config1 = BatDongSanConfig()
    print(f"   Output dir: {config1.output_dir}")
    print(f"   Links file: {config1.links_file}")
    print(f"   Details file: {config1.details_file}")
    print(f"   Full links path: {config1.get_links_path()}")
    print(f"   Full details path: {config1.get_details_path()}")
    
    # Example 2: Custom date
    print("\n2. Custom date:")
    config2 = BatDongSanConfig()
    config2.set_date("2024-12-31")
    print(f"   Links file: {config2.links_file}")
    print(f"   Details file: {config2.details_file}")
    
    # Example 3: Custom output directory
    print("\n3. Custom output directory:")
    config3 = BatDongSanConfig(
        output_dir="custom_data/batdongsan",
        max_workers=4
    )
    print(f"   Output dir: {config3.output_dir}")
    print(f"   Max workers: {config3.max_workers}")
    print(f"   Links file: {config3.links_file}")
    
    # Example 4: Different file naming pattern
    print("\n4. Custom file pattern:")
    config4 = BatDongSanConfig(
        links_file_pattern="links_{date}.json",
        details_file_pattern="details_{date}.json"
    )
    print(f"   Links file: {config4.links_file}")
    print(f"   Details file: {config4.details_file}")
    
    print("\n" + "="*70)