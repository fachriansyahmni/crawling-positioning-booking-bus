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
import re
from datetime import datetime

# Available routes (using placeholders for date components)
# Legacy routes - now managed by RouteManager
AVAILABLE_ROUTES = {
    "Jakarta-Semarang": "https://www.redbus.id/tiket-bus/jakarta-ke-semarang?fromCityName=Jakarta&fromCityId=193490&toCityName=Semarang%20(Semua%20Lokasi)&toCityId=193470&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
    "Jakarta-Surabaya": "https://www.redbus.id/tiket-bus/jakarta-ke-surabaya?fromCityName=Jakarta&fromCityId=193490&toCityName=Surabaya%20(Semua%20Lokasi)&toCityId=194354&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
    "Jakarta-Malang": "https://www.redbus.id/tiket-bus/jakarta-ke-malang?fromCityName=Jakarta&fromCityId=193490&toCityName=Malang&toCityId=194349&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
    "Jakarta-Lampung": "https://www.redbus.id/tiket-bus/jakarta-ke-bandar-lampung?fromCityName=Jakarta&fromCityId=193490&toCityName=Bandar%20Lampung&toCityId=194674&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
}

def get_routes_from_manager():
    """Get routes from RouteManager if available, fallback to legacy"""
    try:
        from routes_manager import RouteManager
        from url_crawler_formatter import LegacyCompatibility
        
        routes_manager = RouteManager()
        legacy_compat = LegacyCompatibility(routes_manager)
        
        # Get routes in legacy format
        routes_dict, dates_list = legacy_compat.get_redbus_legacy_format()
        
        print(f"✓ Loaded {len(routes_dict)} routes from RouteManager")
        return routes_dict, dates_list
        
    except ImportError:
        print("⚠ RouteManager not available, using legacy routes")
        return AVAILABLE_ROUTES, ["15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]
    except Exception as e:
        print(f"⚠ Error loading from RouteManager: {e}")
        return AVAILABLE_ROUTES, ["15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]

# Get routes from manager or use legacy
routes, dates = get_routes_from_manager()


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

    # chrome_driver_path = "/usr/bin/chromedriver"
    
    # Get selenium config
    selenium_config = config.get('selenium', {})
    
    # Use parameter if provided, otherwise use config
    if headless is None:
        headless = selenium_config.get('headless', False)  # Default False for Windows
    
    options = Options()
    
    # Conditional headless mode
    if headless:
        options.add_argument('--headless=new')  # New headless mode
        options.add_argument('--disable-gpu')
    
    # Essential Chrome options for Windows
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    # options.add_argument('--start-maximized')
    
    # Fix for "session not created" error on Windows
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--log-level=3')  # Suppress warnings
    
    # Anti-detection settings (works for both headless and visible)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    options.add_argument(f'user-agent={user_agent}')
    options.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Suppress unnecessary logs
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    try:
        # service = Service(chrome_driver_path)
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        
        # Remove webdriver flag
        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
            "userAgent": user_agent.replace('Headless', '')
        })
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        print("✓ Chrome driver initialized successfully")
        return driver
        
    except Exception as e:
        print(f"✗ Error initializing Chrome driver: {e}")
        print("\nTroubleshooting steps:")
        print("1. Make sure Google Chrome is installed")
        print("2. Try closing all Chrome instances")
        print("3. Run: pip install --upgrade selenium webdriver-manager")
        print("4. Restart your computer if issue persists")
        raise

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

def get_bus_detail(driver, url, route, date_str, max_buses=None, max_scroll=None):
    """
    Get bus details for a specific route and date
    Args:
        driver: Selenium WebDriver
        url: URL template
        route: Route name
        date_str: Date in YYYY-MM-DD format
        max_buses: Maximum number of buses to scrape (None = all buses)
        max_scroll: Maximum number of scroll iterations (None = unlimited, scroll until end)
    """
    try:
        formatted_url = format_url_with_date(url, date_str)
        print(f"Accessing URL: {formatted_url}")
        if max_buses:
            print(f"  Max buses limit: {max_buses}")
        if max_scroll:
            print(f"  Max scroll limit: {max_scroll} iterations")
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
        scroll_count = 0

        while True:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(random.uniform(2.5, 4.5))
            new_height = driver.execute_script("return document.body.scrollHeight")
            scroll_count += 1
            
            # Check if max_scroll limit reached
            if max_scroll and scroll_count >= max_scroll:
                print(f"Reached max scroll limit ({max_scroll} scrolls). Stopping scroll.")
                break
            
            # Check if page height unchanged (end of page)
            if new_height == last_height:
                print("Telah mencapai akhir halaman. Semua data telah dimuat.")
                break # Keluar dari loop jika tinggi halaman tidak lagi bertambah
            
            last_height = new_height
        
        if max_scroll and scroll_count < max_scroll:
            print(f"Completed {scroll_count} scroll(s) - reached end of page")
        elif max_scroll:
            print(f"Completed {scroll_count} scroll(s) - stopped at limit")
        else:
            print(f"Completed {scroll_count} scroll(s)")
        # === LOGIKA SCROLL DINAMIS SELESAI ===
        bus_items = driver.find_elements(By.CSS_SELECTOR, bus_item_selector)
        time.sleep(random.uniform(2, 4))
        
        # Apply max_buses limit if specified
        total_buses_found = len(bus_items)
        if max_buses and total_buses_found > max_buses:
            bus_items = bus_items[:max_buses]
            print(f"Found {total_buses_found} buses, limiting to {max_buses} buses")
        else:
            print(f"Menemukan {total_buses_found} bus untuk diproses.")
        
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
                try:
                    # Extract light-g-bar / green-bar information
                    # Prefer the child `.green-bar` if present and try to parse a `w-<num>` class
                    try:
                        # Try to find a child element inside .light-g-bar.
                        # Prefer .green-bar but accept other child elements (e.g. .bar, div, span).
                        child_selector = (
                            ".light-g-bar .green-bar, .light-g-bar .bar, .light-g-bar > div, .light-g-bar span"
                        )
                        light_g_elem = item.find_element(By.CSS_SELECTOR, child_selector)
                        light_g_class = light_g_elem.get_attribute("class") or ""

                        # 1) Try parse a class like w-10
                        m = re.search(r"w-(\d+)", light_g_class)
                        if m:
                            light_g_value = int(m.group(1))
                        else:
                            # 2) Try inline style (e.g. width:10%) and extract first number
                            style = light_g_elem.get_attribute("style") or ""
                            m2 = re.search(r"(\d+)", style)
                            if m2:
                                light_g_value = int(m2.group(1))
                            else:
                                # 3) Try numeric text inside the element
                                text = (light_g_elem.text or "").strip()
                                if text and re.search(r"\d", text):
                                    m3 = re.search(r"(\d+)", text)
                                    light_g_value = int(m3.group(1)) if m3 else text
                                else:
                                    # 4) Fallback to class or raw text, or 'N/A'
                                    light_g_value = text or light_g_class or 'N/A'
                    except NoSuchElementException:
                        # fallback: any content inside .light-g-bar
                        try:
                            lg = item.find_element(By.CSS_SELECTOR, ".light-g-bar")
                            lg_text = (lg.text or lg.get_attribute("innerHTML") or "").strip()
                            m4 = re.search(r"(\d+)", lg_text)
                            light_g_value = int(m4.group(1)) if m4 else (lg_text or 'N/A')
                        except NoSuchElementException:
                            light_g_value = 'N/A'
                except Exception:
                    light_g_value = 'N/A'
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
                    "Light_G_Bar": light_g_value,
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
    
    # Max buses limit
    print("\n" + "-"*60)
    print("Max Buses Limit (optional):")
    max_buses_input = input("Enter max buses per route/date (press Enter for unlimited): ").strip()
    
    max_buses = None
    if max_buses_input:
        try:
            max_buses = int(max_buses_input)
            if max_buses <= 0:
                print("Invalid number. Using unlimited.")
                max_buses = None
            else:
                print(f"✓ Max buses limit set to: {max_buses} per route/date")
        except ValueError:
            print("Invalid input. Using unlimited.")
            max_buses = None
    else:
        print("✓ No limit set - will crawl all buses")
    
    # Max scroll limit
    print("\n" + "-"*60)
    print("Max Scroll Limit (optional - for faster crawling):")
    max_scroll_input = input("Enter max scroll iterations (press Enter for unlimited): ").strip()
    
    max_scroll = None
    if max_scroll_input:
        try:
            max_scroll = int(max_scroll_input)
            if max_scroll <= 0:
                print("Invalid number. Using unlimited.")
                max_scroll = None
            else:
                print(f"✓ Max scroll limit set to: {max_scroll} scroll(s)")
        except ValueError:
            print("Invalid input. Using unlimited.")
            max_scroll = None
    else:
        print("✓ No limit set - will scroll until end of page")
    
    confirm = input("\nProceed with scraping? (y/n): ").strip().lower()
    if confirm != 'y':
        print("Scraping cancelled.")
        sys.exit(0)
    
    return selected_routes, selected_dates, max_buses, max_scroll

def scrape_with_selection(selected_routes, selected_dates, max_buses=None, max_scroll=None):
    """
    Scrape with user-selected routes and dates
    Args:
        selected_routes: Dictionary of route names and URLs
        selected_dates: List of date strings
        max_buses: Maximum buses per route/date (None = unlimited)
        max_scroll: Maximum scroll iterations (None = unlimited)
    """
    print("\nStarting scraping process...")
    if max_buses:
        print(f"Max buses per route/date: {max_buses}")
    if max_scroll:
        print(f"Max scroll iterations: {max_scroll}")
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
                    bus_details = get_bus_detail(driver, url, route_name, date_str, max_buses=max_buses, max_scroll=max_scroll)
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
        selected_routes, selected_dates, max_buses, max_scroll = get_user_input()
        print(f"\nStarting scraping for {len(selected_routes)} routes and {len(selected_dates)} dates...")
        all_bus_details = scrape_with_selection(selected_routes, selected_dates, max_buses=max_buses, max_scroll=max_scroll)
    
    if all_bus_details:
        print(f"\n✓ Total data successfully scraped: {len(all_bus_details)} buses")
    else:
        print("\n⚠ No data was scraped.")

