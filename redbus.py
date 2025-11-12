from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

from selenium.common.exceptions import NoSuchElementException
import time
import random
import pandas as pd
import json
import os
import sys
from datetime import datetime

# Available routes (using placeholders for date components)
AVAILABLE_ROUTES = {
    "Jakarta-Semarang": "https://www.redbus.id/tiket-bus/jakarta-ke-semarang?fromCityName=Jakarta&fromCityId=193490&toCityName=Semarang%20(Semua%20Lokasi)&toCityId=193470&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
    "Jakarta-Surabaya": "https://www.redbus.id/tiket-bus/jakarta-ke-surabaya?fromCityName=Jakarta&fromCityId=193490&toCityName=Surabaya%20(Semua%20Lokasi)&toCityId=194354&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
    "Jakarta-Malang": "https://www.redbus.id/tiket-bus/jakarta-ke-malang?fromCityName=Jakarta&fromCityId=193490&toCityName=Malang&toCityId=194349&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
    "Jakarta-Lampung": "https://www.redbus.id/tiket-bus/jakarta-ke-bandar-lampung?fromCityName=Jakarta&fromCityId=193490&toCityName=Bandar%20Lampung&toCityId=194674&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
}

# Keep these for backward compatibility with web_crawler_unified.py
routes = AVAILABLE_ROUTES 
dates = ["15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]


def load_config():
    """Load configuration from config files"""
    config_files = ['config_vps.json', 'config.json']
    
    for config_file in config_files:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                return json.load(f)
    
    # Default config if no file exists
    return {
        "selenium": {
            "headless": True,  # Default to headless for Redbus
            "disable_gpu": False,
            "no_sandbox": False,
            "disable_dev_shm": False
        }
    }


def initialize_driver(headless=None):
    """
    Initialize Chrome WebDriver with configurable headless mode
    
    Args:
        headless: Override headless setting (True/False). If None, uses config file.
    """
    config = load_config()
    
    # Get selenium config
    selenium_config = config.get('selenium', {})
    
    # Use parameter if provided, otherwise use config
    if headless is None:
        headless = selenium_config.get('headless', True)  # Default True for Redbus
    
    options = Options()
    
    # Headless mode for VPS
    if headless:
        options.add_argument('--headless=new')  # New headless mode
        options.add_argument('--disable-gpu') if selenium_config.get('disable_gpu', True) else None
        options.add_argument('--no-sandbox') if selenium_config.get('no_sandbox', True) else None
        options.add_argument('--disable-dev-shm-usage') if selenium_config.get('disable_dev_shm', True) else None
        options.add_argument('--window-size=1920,1080')
    
    # Anti-detection settings (works for both headless and visible)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    return driver

def load_page(driver, url):
    driver.get(url)
    time.sleep(5)

def format_url_with_date(url, date_str):
    """
    Format URL with date components
    Args:
        url: URL template with [[DAY]], [[MONTH]], [[YEAR]] placeholders
        date_str: Date string in format YYYY-MM-DD
    Returns:
        Formatted URL
    """
    try:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        day = str(date_obj.day).zfill(2)
        month = date_obj.strftime("%b")  # Short month name (Jan, Feb, etc.)
        year = str(date_obj.year)
        
        formatted_url = url.replace("[[DAY]]", day)
        formatted_url = formatted_url.replace("[[MONTH]]", month)
        formatted_url = formatted_url.replace("[[YEAR]]", year)
        
        return formatted_url
    except ValueError:
        print(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")
        return url

def get_bus_detail(driver, url, route, date_str):
    """
    Get bus details for a specific route and date
    Args:
        driver: Selenium WebDriver
        url: URL template
        route: Route name
        date_str: Date in YYYY-MM-DD format
    """
    try:
        formatted_url = format_url_with_date(url, date_str)
        print(f"Accessing URL: {formatted_url}")
        driver.get(formatted_url)
        
        wait = WebDriverWait(driver, 30)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(random.uniform(1.5, 3.5))

        bus_item_selector = "li.row-sec.clearfix"
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, bus_item_selector)))
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # === LOGIKA SCROLL DINAMIS DIMULAI DI SINI ===
        print("Memulai proses scroll untuk memuat semua data...")
        print(route, date_str)
        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2.5, 4.5))
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                print("Telah mencapai akhir halaman. Semua data telah dimuat.")
                break # Keluar dari loop jika tinggi halaman tidak lagi bertambah
            last_height = new_height
        # === LOGIKA SCROLL DINAMIS SELESAI ===
        bus_items = driver.find_elements(By.CSS_SELECTOR, bus_item_selector)
        time.sleep(random.uniform(2, 4))
        print(f"Menemukan {len(bus_items)} bus untuk diproses.")
        print("===============================================")
        bus_details = []
        for item in bus_items:
            try:
                bus_name = item.find_element(By.CSS_SELECTOR, ".travels.lh-24.f-bold.d-color").text
                bus_type = item.find_element(By.CSS_SELECTOR, ".bus-type.f-12.m-top-16.l-color").text
                departing_time = item.find_element(By.CSS_SELECTOR, ".dp-time.f-19.d-color.f-bold").text
                duration = item.find_element(By.CSS_SELECTOR, ".dur.l-color.lh-24").text
                reaching_time = item.find_element(By.CSS_SELECTOR, ".bp-time.f-19.d-color.disp-Inline").text
                price_text = item.find_element(By.CSS_SELECTOR, ".fare.d-block, div.seat-fare > div.f-19.f-bold > span, div.seat-fare > div.fare > span").text
                price_numeric = ''.join(filter(str.isdigit, price_text))
                try:
                    star_rating = item.find_element(By.XPATH, ".//div[@class='rating-sec lh-24']").text
                except NoSuchElementException:
                    star_rating = '0' # Nilai default

                try:
                    # Perhatikan selector XPath ini lebih umum dan kuat
                    seat_availability_text = item.find_element(By.XPATH, ".//div[contains(@class, 'seat-left')]").text
                except NoSuchElementException:
                    seat_availability_text = 'N/A' # Nilai default
                bus_detail = {
                    "Route_Name": route,
                    "Route_Date": date_str,
                    "Route_Link": format_url_with_date(url, date_str),
                    "Bus_Name": bus_name,
                    "Bus_Type": bus_type,
                    "Departing_Time": departing_time,
                    "Duration": duration,
                    "Reaching_Time": reaching_time,
                    "Star_Rating": star_rating,
                    "Price": price_numeric,
                    "Seat_Availability": seat_availability_text
                }
                bus_details.append(bus_detail)

            except Exception as e:
                print(f"Gagal memproses satu item bus. Melewati... Error: {e}")
        
        return bus_details
    except Exception as e:
        print(f"Error occurred while accessing {url}: {str(e)}")
        return []

def get_user_input():
    """Get routes and dates from user input"""
    print("\n" + "="*60)
    print("           REDBUS SCRAPER - User Input Mode")
    print("="*60)
    
    # Show available routes
    print("\nAvailable Routes:")
    for idx, route_name in enumerate(AVAILABLE_ROUTES.keys(), 1):
        print(f"  {idx}. {route_name}")
    
    # Get route selection
    selected_routes = {}
    print("\nSelect routes (comma-separated numbers, or 'all' for all routes):")
    print("Example: 1,3 or all")
    route_input = input("Your choice: ").strip()
    
    if route_input.lower() == 'all':
        selected_routes = AVAILABLE_ROUTES.copy()
        print(f"✓ Selected all {len(selected_routes)} routes")
    else:
        try:
            route_indices = [int(x.strip()) for x in route_input.split(',')]
            route_list = list(AVAILABLE_ROUTES.keys())
            for idx in route_indices:
                if 1 <= idx <= len(route_list):
                    route_name = route_list[idx - 1]
                    selected_routes[route_name] = AVAILABLE_ROUTES[route_name]
            print(f"✓ Selected {len(selected_routes)} route(s): {', '.join(selected_routes.keys())}")
        except ValueError:
            print("Invalid input! Using all routes.")
            selected_routes = AVAILABLE_ROUTES.copy()
    
    # Get date range
    print("\n" + "-"*60)
    print("Date Selection (YYYY-MM-DD format):")
    print("  - Enter specific dates (comma-separated): 2025-12-15,2025-12-16")
    print("  - Enter date range: 2025-12-15 to 2025-12-20")
    print("  - Enter single date: 2025-12-15")
    date_input = input("Your choice: ").strip()
    
    selected_dates = []
    if ' to ' in date_input.lower():
        # Date range input
        try:
            start_date, end_date = date_input.lower().split(' to ')
            start = datetime.strptime(start_date.strip(), "%Y-%m-%d")
            end = datetime.strptime(end_date.strip(), "%Y-%m-%d")
            
            current = start
            while current <= end:
                selected_dates.append(current.strftime("%Y-%m-%d"))
                current = datetime.fromtimestamp(current.timestamp() + 86400)  # Add 1 day
            
            print(f"✓ Selected date range: {selected_dates[0]} to {selected_dates[-1]} ({len(selected_dates)} days)")
        except ValueError as e:
            print(f"Invalid date range format! Error: {e}")
            print("Using default: 2025-12-15 to 2025-12-31")
            current = datetime(2025, 12, 15)
            end = datetime(2025, 12, 31)
            while current <= end:
                selected_dates.append(current.strftime("%Y-%m-%d"))
                current = datetime.fromtimestamp(current.timestamp() + 86400)
    elif ',' in date_input:
        # Multiple specific dates
        try:
            date_strs = [d.strip() for d in date_input.split(',')]
            for date_str in date_strs:
                datetime.strptime(date_str, "%Y-%m-%d")  # Validate format
                selected_dates.append(date_str)
            print(f"✓ Selected {len(selected_dates)} date(s): {', '.join(selected_dates)}")
        except ValueError as e:
            print(f"Invalid date format! Error: {e}")
            print("Using default: 2025-12-15 to 2025-12-31")
            current = datetime(2025, 12, 15)
            end = datetime(2025, 12, 31)
            while current <= end:
                selected_dates.append(current.strftime("%Y-%m-%d"))
                current = datetime.fromtimestamp(current.timestamp() + 86400)
    else:
        # Single date
        try:
            datetime.strptime(date_input, "%Y-%m-%d")  # Validate format
            selected_dates = [date_input]
            print(f"✓ Selected date: {date_input}")
        except ValueError as e:
            print(f"Invalid date format! Error: {e}")
            print("Using default: 2025-12-15 to 2025-12-31")
            current = datetime(2025, 12, 15)
            end = datetime(2025, 12, 31)
            while current <= end:
                selected_dates.append(current.strftime("%Y-%m-%d"))
                current = datetime.fromtimestamp(current.timestamp() + 86400)
    
    # Confirmation
    print("\n" + "="*60)
    print("SUMMARY:")
    print(f"  Routes: {len(selected_routes)} - {', '.join(selected_routes.keys())}")
    if len(selected_dates) > 0:
        print(f"  Dates: {len(selected_dates)} - {selected_dates[0]} to {selected_dates[-1]}")
    print(f"  Total tasks: {len(selected_routes) * len(selected_dates)}")
    print("="*60)
    
    confirm = input("\nProceed with scraping? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Scraping cancelled.")
        sys.exit(0)
    
    return selected_routes, selected_dates

def scrape_with_selection(selected_routes, selected_dates):
    """Scrape with user-selected routes and dates"""
    print("\nStarting scraping process...")
    all_bus_details = []
    total_tasks = len(selected_routes) * len(selected_dates)
    current_task = 0
    
    driver = initialize_driver()
    
    try:
        for route_name, url in selected_routes.items():
            for date_str in selected_dates:
                current_task += 1
                print(f"\n[{current_task}/{total_tasks}] Processing: {route_name} - Date: {date_str}")
                print("-" * 60)
                
                try:
                    bus_details = get_bus_detail(driver, url, route_name, date_str)
                    if bus_details:
                        df_bus_details = pd.DataFrame(bus_details)
                        # Use simplified filename with date
                        filename = f'new_data/redbus_{route_name}-{date_str}.csv'
                        df_bus_details.to_csv(filename, index=False, encoding='utf-8')
                        print(f"✓ Saved {len(bus_details)} buses to {filename}")
                        all_bus_details.extend(bus_details)
                    else:
                        print(f"⚠ No data found for {route_name} - {date_str}")
                except Exception as e:
                    print(f"✗ Error: {str(e)}")
    finally:
        driver.quit()
        print("\n" + "="*60)
        print("Scraping completed!")
        print(f"Total buses scraped: {len(all_bus_details)}")
        print("="*60)
    
    return all_bus_details

def scrape_all_pages():
    """Legacy function - scrape all routes and dates (for backward compatibility with web UI)"""
    all_bus_details = []
    for route, url in routes.items():
        driver = initialize_driver()
        for date in dates:
            try:
                # Check if date is already in YYYY-MM-DD format or just a day number
                if '-' in date:
                    # Already in full date format (YYYY-MM-DD)
                    date_str = date
                else:
                    # Legacy format: convert day only to full date format for December 2025
                    date_str = f"2025-12-{date.zfill(2)}"
                
                bus_details = get_bus_detail(driver, url, route, date_str)
                if bus_details:
                    df_bus_details = pd.DataFrame(bus_details)
                    # For legacy compatibility, keep filename with just the date part for display
                    if '-' in date:
                        filename_date = date  # Full date like 2026-12-02
                    else:
                        filename_date = date  # Just day like 15
                    df_bus_details.to_csv(f'new_data/redbus_{route}-{filename_date}.csv', index=False, encoding='utf-8')
                    all_bus_details.extend(bus_details)
            except Exception as e:
                print(f"Error occurred while accessing route {route} - date {date}: {str(e)}")
        driver.quit()
    return all_bus_details

if __name__ == "__main__":
    # Check if running with command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == '--auto':
        print("Running in automatic mode (all routes, all dates)...")
        all_bus_details = scrape_all_pages()
    else:
        # Interactive mode - get user input
        selected_routes, selected_dates = get_user_input()
        all_bus_details = scrape_with_selection(selected_routes, selected_dates)
    
    if all_bus_details:
        print(f"\n✓ Total data successfully scraped: {len(all_bus_details)} buses")
    else:
        print("\n⚠ No data was scraped.")
