"""
URL Crawler Formatter - Handles URL formatting and crawling logic
Integrates with RouteManager for unified route management
"""

from routes_manager import RouteManager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
import re
import json
import os


class URLFormatter:
    """URL formatting and validation for different platforms"""
    
    def __init__(self, routes_manager: RouteManager = None):
        self.routes_manager = routes_manager or RouteManager()
    
    def format_redbus_url(self, route_name: str, date_str: str) -> Optional[str]:
        """Format Redbus URL for specific route and date"""
        route = self.routes_manager.get_route_by_name(route_name)
        if not route:
            print(f"Route '{route_name}' not found")
            return None
        
        return self.routes_manager.format_url_for_date(route["id"], "redbus", date_str)
    
    def format_traveloka_url(self, route_name: str, date_str: str) -> Optional[str]:
        """Format Traveloka URL for specific route and date"""
        route = self.routes_manager.get_route_by_name(route_name)
        if not route:
            print(f"Route '{route_name}' not found")
            return None
        
        return self.routes_manager.format_url_for_date(route["id"], "traveloka", date_str)
    
    def get_formatted_urls_batch(self, routes: List[str], dates: List[str], 
                                platform: str) -> List[Dict]:
        """Get formatted URLs for multiple routes and dates"""
        formatted_urls = []
        
        for route_name in routes:
            for date_str in dates:
                if platform == "redbus":
                    url = self.format_redbus_url(route_name, date_str)
                elif platform == "traveloka":
                    url = self.format_traveloka_url(route_name, date_str)
                else:
                    print(f"Unsupported platform: {platform}")
                    continue
                
                if url:
                    formatted_urls.append({
                        "route_name": route_name,
                        "date": date_str,
                        "platform": platform,
                        "url": url
                    })
        
        return formatted_urls
    
    def validate_date_format(self, date_str: str) -> Tuple[bool, str]:
        """Validate date format (YYYY-MM-DD)"""
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
            return True, "Valid date format"
        except ValueError:
            return False, "Invalid date format. Expected YYYY-MM-DD"
    
    def generate_date_range(self, start_date: str, end_date: str) -> List[str]:
        """Generate list of dates between start and end date"""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
            
            dates = []
            current = start
            while current <= end:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
            
            return dates
        except ValueError as e:
            print(f"Date range error: {e}")
            return []


class CrawlTaskGenerator:
    """Generate crawling tasks for different platforms"""
    
    def __init__(self, routes_manager: RouteManager = None):
        self.routes_manager = routes_manager or RouteManager()
        self.url_formatter = URLFormatter(self.routes_manager)
    
    def generate_redbus_tasks(self, route_names: List[str], dates: List[str]) -> List[Dict]:
        """Generate tasks for Redbus crawling"""
        tasks = []
        
        for route_name in route_names:
            route = self.routes_manager.get_route_by_name(route_name)
            if not route:
                print(f"⚠ Route '{route_name}' not found, skipping...")
                continue
            
            url_template = self.routes_manager.get_platform_url(route["id"], "redbus")
            if not url_template:
                print(f"⚠ No Redbus URL found for route '{route_name}', skipping...")
                continue
            
            for date_str in dates:
                is_valid, message = self.url_formatter.validate_date_format(date_str)
                if not is_valid:
                    print(f"⚠ {message} for date '{date_str}', skipping...")
                    continue
                
                formatted_url = self.routes_manager.format_url_for_date(route["id"], "redbus", date_str)
                
                task = {
                    "route_id": route["id"],
                    "route_name": route_name,
                    "date": date_str,
                    "url": formatted_url,
                    "platform": "redbus",
                    "origin": route["origin"],
                    "destination": route["destination"]
                }
                tasks.append(task)
        
        return tasks
    
    def generate_traveloka_tasks(self, route_names: List[str], dates: List[str]) -> List[Dict]:
        """Generate tasks for Traveloka crawling"""
        tasks = []
        
        for route_name in route_names:
            route = self.routes_manager.get_route_by_name(route_name)
            if not route:
                print(f"⚠ Route '{route_name}' not found, skipping...")
                continue
            
            url_template = self.routes_manager.get_platform_url(route["id"], "traveloka")
            if not url_template:
                print(f"⚠ No Traveloka URL found for route '{route_name}', skipping...")
                continue
            
            for date_str in dates:
                is_valid, message = self.url_formatter.validate_date_format(date_str)
                if not is_valid:
                    print(f"⚠ {message} for date '{date_str}', skipping...")
                    continue
                
                formatted_url = self.routes_manager.format_url_for_date(route["id"], "traveloka", date_str)
                
                task = {
                    "route_idx": 0,  # For compatibility with existing Traveloka code
                    "route_id": route["id"],
                    "route_name": route_name,
                    "date": date_str,
                    "url": formatted_url,
                    "platform": "traveloka",
                    "origin": route["origin"],
                    "destination": route["destination"]
                }
                tasks.append(task)
        
        return tasks
    
    def generate_unified_tasks(self, platforms: List[str], route_names: List[str], 
                              dates: List[str]) -> Dict[str, List[Dict]]:
        """Generate tasks for multiple platforms"""
        unified_tasks = {}
        
        if "redbus" in platforms:
            unified_tasks["redbus"] = self.generate_redbus_tasks(route_names, dates)
        
        if "traveloka" in platforms:
            unified_tasks["traveloka"] = self.generate_traveloka_tasks(route_names, dates)
        
        return unified_tasks


class LegacyCompatibility:
    """Provide backward compatibility with existing crawler code"""
    
    def __init__(self, routes_manager: RouteManager = None):
        self.routes_manager = routes_manager or RouteManager()
    
    def get_redbus_legacy_format(self) -> Tuple[Dict, List]:
        """Get Redbus routes in legacy format for existing code"""
        routes_dict = self.routes_manager.get_routes_for_platform("redbus", active_only=True)
        dates_list = ["15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]
        
        return routes_dict, dates_list
    
    def get_traveloka_legacy_format(self) -> Tuple[List[Dict], List[List]]:
        """Get Traveloka routes in legacy format for existing code"""
        routes_dict = self.routes_manager.get_routes_for_platform("traveloka", active_only=True)
        dates_list = ["15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]
        
        # Traveloka expects list of route sets
        traveloka_routes = [routes_dict]
        traveloka_dates = [dates_list]
        
        return traveloka_routes, traveloka_dates
    
    def convert_tasks_to_legacy_redbus(self, tasks: List[Dict]) -> List[Tuple]:
        """Convert modern task format to legacy tuple format for Redbus"""
        legacy_tasks = []
        for task in tasks:
            # Legacy format: (route_name, date, url)
            legacy_task = (task["route_name"], task["date"], task["url"])
            legacy_tasks.append(legacy_task)
        
        return legacy_tasks
    
    def convert_tasks_to_legacy_traveloka(self, tasks: List[Dict]) -> List[Dict]:
        """Convert modern task format to legacy format for Traveloka"""
        legacy_tasks = []
        for task in tasks:
            legacy_task = {
                "route_idx": task.get("route_idx", 0),
                "route": task["route_name"], 
                "url": task["url"],
                "date": task["date"]
            }
            legacy_tasks.append(legacy_task)
        
        return legacy_tasks


class CrawlConfigManager:
    """Manage crawling configurations and presets"""
    
    def __init__(self, config_file='crawl_configs.json'):
        self.config_file = config_file
        self.configs = self._load_configs()
        self.routes_manager = RouteManager()
        self.task_generator = CrawlTaskGenerator(self.routes_manager)
    
    def _load_configs(self) -> Dict:
        """Load crawl configurations"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading configs: {e}")
                return self._get_default_configs()
        else:
            return self._get_default_configs()
    
    def _get_default_configs(self) -> Dict:
        """Get default crawl configurations"""
        return {
            "presets": {
                "daily_crawl": {
                    "name": "Daily Crawl",
                    "description": "Daily crawling for all active routes",
                    "platforms": ["redbus", "traveloka"],
                    "routes": "all_active",
                    "date_range": "today_plus_7",
                    "schedule": "daily_6am"
                },
                "weekend_crawl": {
                    "name": "Weekend Crawl", 
                    "description": "Weekend-focused crawling",
                    "platforms": ["redbus"],
                    "routes": ["Jakarta-Semarang", "Jakarta-Surabaya"],
                    "date_range": "next_weekend",
                    "schedule": "manual"
                },
                "full_month_crawl": {
                    "name": "Full Month Crawl",
                    "description": "Crawl entire month data",
                    "platforms": ["redbus", "traveloka"],
                    "routes": "all_active",
                    "date_range": "current_month",
                    "schedule": "manual"
                }
            },
            "date_presets": {
                "today": {"days": 0},
                "tomorrow": {"days": 1},
                "today_plus_7": {"days": 7},
                "today_plus_30": {"days": 30},
                "next_weekend": {"type": "next_weekend"},
                "current_month": {"type": "current_month"}
            }
        }
    
    def _save_configs(self):
        """Save configurations to file"""
        try:
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.configs, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving configs: {e}")
    
    def get_preset_tasks(self, preset_name: str) -> Dict[str, List[Dict]]:
        """Generate tasks from a preset configuration"""
        preset = self.configs.get("presets", {}).get(preset_name)
        if not preset:
            raise ValueError(f"Preset '{preset_name}' not found")
        
        # Resolve routes
        if preset["routes"] == "all_active":
            routes = [r["name"] for r in self.routes_manager.get_all_routes(active_only=True)]
        else:
            routes = preset["routes"]
        
        # Resolve dates
        dates = self._resolve_date_range(preset["date_range"])
        
        # Generate tasks
        platforms = preset["platforms"]
        return self.task_generator.generate_unified_tasks(platforms, routes, dates)
    
    def _resolve_date_range(self, date_range: str) -> List[str]:
        """Resolve date range from preset"""
        date_preset = self.configs.get("date_presets", {}).get(date_range)
        if not date_preset:
            # Try to parse as literal date range
            return [date_range]
        
        today = datetime.now()
        
        if "days" in date_preset:
            # Generate dates for next N days
            days = date_preset["days"]
            dates = []
            for i in range(days + 1):
                date = today + timedelta(days=i)
                dates.append(date.strftime("%Y-%m-%d"))
            return dates
        
        elif date_preset.get("type") == "next_weekend":
            # Find next weekend dates
            days_until_saturday = (5 - today.weekday()) % 7
            if days_until_saturday == 0 and today.weekday() == 5:
                days_until_saturday = 7  # If today is Saturday, get next Saturday
            
            saturday = today + timedelta(days=days_until_saturday)
            sunday = saturday + timedelta(days=1)
            return [saturday.strftime("%Y-%m-%d"), sunday.strftime("%Y-%m-%d")]
        
        elif date_preset.get("type") == "current_month":
            # Generate all dates for current month
            year, month = today.year, today.month
            
            # Get first day of month
            first_day = datetime(year, month, 1)
            
            # Get last day of month
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)
            last_day = next_month - timedelta(days=1)
            
            dates = []
            current = first_day
            while current <= last_day:
                dates.append(current.strftime("%Y-%m-%d"))
                current += timedelta(days=1)
            
            return dates
        
        return []
    
    def create_custom_preset(self, name: str, description: str, platforms: List[str], 
                           routes: List[str], date_range: str) -> bool:
        """Create a custom crawling preset"""
        preset = {
            "name": name,
            "description": description,
            "platforms": platforms,
            "routes": routes,
            "date_range": date_range,
            "schedule": "manual",
            "created_at": datetime.now().isoformat()
        }
        
        self.configs.setdefault("presets", {})[name] = preset
        self._save_configs()
        print(f"✓ Created preset: {name}")
        return True
    
    def list_presets(self) -> List[Dict]:
        """List all available presets"""
        presets = []
        for name, config in self.configs.get("presets", {}).items():
            presets.append({
                "name": name,
                "description": config.get("description", ""),
                "platforms": config.get("platforms", []),
                "routes": config.get("routes", []),
                "date_range": config.get("date_range", "")
            })
        return presets


# ============ Integration Functions ============

def update_redbus_module():
    """Update redbus.py to use RouteManager"""
    routes_manager = RouteManager()
    legacy_compat = LegacyCompatibility(routes_manager)
    
    # Get routes in legacy format
    routes_dict, dates_list = legacy_compat.get_redbus_legacy_format()
    
    print("Updated routes for redbus.py:")
    print(f"Routes: {list(routes_dict.keys())}")
    print(f"Dates: {dates_list}")
    
    return routes_dict, dates_list

def update_traveloka_module():
    """Update traveloka.py to use RouteManager"""
    routes_manager = RouteManager()
    legacy_compat = LegacyCompatibility(routes_manager)
    
    # Get routes in legacy format
    traveloka_routes, traveloka_dates = legacy_compat.get_traveloka_legacy_format()
    
    print("Updated routes for traveloka.py:")
    print(f"Route sets: {len(traveloka_routes)}")
    print(f"Routes in set 0: {list(traveloka_routes[0].keys()) if traveloka_routes else []}")
    
    return traveloka_routes, traveloka_dates

def generate_web_interface_data():
    """Generate data for web interface"""
    routes_manager = RouteManager()
    
    # Get all active routes
    active_routes = routes_manager.get_all_routes(active_only=True)
    
    # Format for web interface
    web_routes = []
    for route in active_routes:
        web_routes.append({
            "id": route["id"],
            "name": route["name"],
            "origin": route["origin"],
            "destination": route["destination"],
            "platforms": {
                "redbus": routes_manager.get_platform_url(route["id"], "redbus") is not None,
                "traveloka": routes_manager.get_platform_url(route["id"], "traveloka") is not None
            }
        })
    
    return web_routes


# ============ Testing and Demo Functions ============

def test_url_formatting():
    """Test URL formatting functionality"""
    print("\n" + "="*60)
    print("           URL FORMATTING TEST")
    print("="*60)
    
    routes_manager = RouteManager()
    url_formatter = URLFormatter(routes_manager)
    
    # Test routes
    test_routes = ["Jakarta-Semarang", "Jakarta-Surabaya"]
    test_dates = ["2025-12-15", "2025-12-16"]
    
    for route_name in test_routes:
        print(f"\nTesting route: {route_name}")
        print("-" * 40)
        
        for date_str in test_dates:
            print(f"Date: {date_str}")
            
            # Test Redbus
            redbus_url = url_formatter.format_redbus_url(route_name, date_str)
            print(f"  Redbus: {redbus_url}")
            
            # Test Traveloka
            traveloka_url = url_formatter.format_traveloka_url(route_name, date_str) 
            print(f"  Traveloka: {traveloka_url}")

def demo_task_generation():
    """Demonstrate task generation"""
    print("\n" + "="*60)
    print("           TASK GENERATION DEMO")
    print("="*60)
    
    task_generator = CrawlTaskGenerator()
    
    routes = ["Jakarta-Semarang", "Jakarta-Surabaya"] 
    dates = ["2025-12-15", "2025-12-16"]
    platforms = ["redbus", "traveloka"]
    
    unified_tasks = task_generator.generate_unified_tasks(platforms, routes, dates)
    
    for platform, tasks in unified_tasks.items():
        print(f"\n{platform.upper()} Tasks ({len(tasks)}):")
        print("-" * 30)
        for i, task in enumerate(tasks[:3]):  # Show first 3 tasks
            print(f"  {i+1}. {task['route_name']} - {task['date']}")
            print(f"      URL: {task['url'][:80]}...")


if __name__ == "__main__":
    # Run tests
    test_url_formatting()
    demo_task_generation()
    
    # Show web interface data
    web_data = generate_web_interface_data()
    print(f"\n✓ Generated web interface data for {len(web_data)} routes")