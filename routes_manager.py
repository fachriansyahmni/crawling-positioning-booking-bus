"""
Routes Manager - Master unified routes system with CRUD operations
Handles route management for both Traveloka and Redbus platforms
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import re

class RouteManager:
    """Centralized route management system"""
    
    def __init__(self, config_file='routes_config.json'):
        self.config_file = config_file
        self.routes_data = self._load_routes_config()
        self._ensure_default_routes()
    
    def _load_routes_config(self) -> Dict:
        """Load routes configuration from file"""
        if os.path.exists(self.config_file):
            try:
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading routes config: {e}")
                return self._get_default_config()
        else:
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        """Get default routes configuration"""
        return {
            "version": "1.0",
            "last_updated": datetime.now().isoformat(),
            "platforms": {
                "traveloka": {
                    "base_url": "https://www.traveloka.com",
                    "url_format": "traveloka_format",
                    "date_format": "%Y-%m-%d",
                    "routes": {}
                },
                "redbus": {
                    "base_url": "https://www.redbus.id",
                    "url_format": "redbus_format", 
                    "date_format": "[[DAY]]-[[MONTH]]-[[YEAR]]",
                    "routes": {}
                }
            },
            "master_routes": [],
            "route_mappings": {}
        }
    
    def _ensure_default_routes(self):
        """Ensure default routes exist"""
        default_routes = [
            {
                "id": "jkt_smg",
                "name": "Jakarta-Semarang",
                "origin": "Jakarta",
                "destination": "Semarang",
                "category": "intercity",
                "active": True
            },
            {
                "id": "jkt_sby", 
                "name": "Jakarta-Surabaya",
                "origin": "Jakarta",
                "destination": "Surabaya", 
                "category": "intercity",
                "active": True
            },
            {
                "id": "jkt_mlg",
                "name": "Jakarta-Malang",
                "origin": "Jakarta",
                "destination": "Malang",
                "category": "intercity", 
                "active": True
            },
            {
                "id": "jkt_lpg",
                "name": "Jakarta-Lampung",
                "origin": "Jakarta",
                "destination": "Lampung",
                "category": "intercity",
                "active": True
            }
        ]
        
        # Add default routes if master_routes is empty
        if not self.routes_data.get("master_routes"):
            self.routes_data["master_routes"] = default_routes
            
            # Add default platform mappings
            self._add_default_redbus_mappings()
            self._add_default_traveloka_mappings()
            
            self._save_routes_config()
    
    def _add_default_redbus_mappings(self):
        """Add default Redbus URL mappings"""
        redbus_urls = {
            "jkt_smg": "https://www.redbus.id/tiket-bus/jakarta-ke-semarang?fromCityName=Jakarta&fromCityId=193490&toCityName=Semarang%20(Semua%20Lokasi)&toCityId=193470&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
            "jkt_sby": "https://www.redbus.id/tiket-bus/jakarta-ke-surabaya?fromCityName=Jakarta&fromCityId=193490&toCityName=Surabaya%20(Semua%20Lokasi)&toCityId=194354&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
            "jkt_mlg": "https://www.redbus.id/tiket-bus/jakarta-ke-malang?fromCityName=Jakarta&fromCityId=193490&toCityName=Malang&toCityId=194349&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN",
            "jkt_lpg": "https://www.redbus.id/tiket-bus/jakarta-ke-bandar-lampung?fromCityName=Jakarta&fromCityId=193490&toCityName=Bandar%20Lampung&toCityId=194674&onward=[[DAY]]-[[MONTH]]-[[YEAR]]&busType=Any&srcCountry=IDN&destCountry=IDN"
        }
        
        self.routes_data["platforms"]["redbus"]["routes"] = redbus_urls
    
    def _add_default_traveloka_mappings(self):
        """Add default Traveloka URL mappings (placeholder)"""
        traveloka_urls = {
            "jkt_smg": "https://www.traveloka.com/id-id/bus-travel/search?o=JAKARTA&d=SEMARANG&dt=[[DATE]]",
            "jkt_sby": "https://www.traveloka.com/id-id/bus-travel/search?o=JAKARTA&d=SURABAYA&dt=[[DATE]]", 
            "jkt_mlg": "https://www.traveloka.com/id-id/bus-travel/search?o=JAKARTA&d=MALANG&dt=[[DATE]]",
            "jkt_lpg": "https://www.traveloka.com/id-id/bus-travel/search?o=JAKARTA&d=LAMPUNG&dt=[[DATE]]"
        }
        
        self.routes_data["platforms"]["traveloka"]["routes"] = traveloka_urls
    
    def _save_routes_config(self):
        """Save routes configuration to file"""
        try:
            self.routes_data["last_updated"] = datetime.now().isoformat()
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.routes_data, f, indent=2, ensure_ascii=False)
            print(f"✓ Routes configuration saved to {self.config_file}")
        except Exception as e:
            print(f"Error saving routes config: {e}")
    
    # ============ CRUD Operations for Master Routes ============
    
    def add_route(self, name: str, origin: str, destination: str, 
                  category: str = "intercity", active: bool = True) -> str:
        """
        Add a new master route
        
        Returns:
            route_id: Generated route ID
        """
        # Generate route ID from name
        route_id = self._generate_route_id(name)
        
        # Check if route already exists
        if self.get_route_by_id(route_id):
            raise ValueError(f"Route with ID '{route_id}' already exists")
        
        new_route = {
            "id": route_id,
            "name": name,
            "origin": origin,
            "destination": destination,
            "category": category,
            "active": active,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        }
        
        self.routes_data["master_routes"].append(new_route)
        self._save_routes_config()
        
        print(f"✓ Added route: {name} (ID: {route_id})")
        return route_id
    
    def get_all_routes(self, active_only: bool = False) -> List[Dict]:
        """Get all master routes"""
        routes = self.routes_data.get("master_routes", [])
        if active_only:
            routes = [r for r in routes if r.get("active", True)]
        return routes
    
    def get_route_by_id(self, route_id: str) -> Optional[Dict]:
        """Get route by ID"""
        for route in self.routes_data.get("master_routes", []):
            if route["id"] == route_id:
                return route
        return None
    
    def get_route_by_name(self, name: str) -> Optional[Dict]:
        """Get route by name"""
        for route in self.routes_data.get("master_routes", []):
            if route["name"] == name:
                return route
        return None
    
    def update_route(self, route_id: str, **kwargs) -> bool:
        """Update route by ID"""
        for i, route in enumerate(self.routes_data.get("master_routes", [])):
            if route["id"] == route_id:
                # Update allowed fields
                allowed_fields = ["name", "origin", "destination", "category", "active"]
                for field, value in kwargs.items():
                    if field in allowed_fields:
                        route[field] = value
                
                route["updated_at"] = datetime.now().isoformat()
                self.routes_data["master_routes"][i] = route
                self._save_routes_config()
                print(f"✓ Updated route: {route_id}")
                return True
        
        print(f"Route with ID '{route_id}' not found")
        return False
    
    def delete_route(self, route_id: str) -> bool:
        """Delete route by ID (soft delete - set active=False)"""
        return self.update_route(route_id, active=False)
    
    def hard_delete_route(self, route_id: str) -> bool:
        """Permanently delete route and all platform mappings"""
        # Remove from master routes
        original_length = len(self.routes_data.get("master_routes", []))
        self.routes_data["master_routes"] = [
            r for r in self.routes_data.get("master_routes", []) 
            if r["id"] != route_id
        ]
        
        # Remove platform mappings
        for platform_name in self.routes_data.get("platforms", {}):
            platform_routes = self.routes_data["platforms"][platform_name].get("routes", {})
            if route_id in platform_routes:
                del platform_routes[route_id]
        
        if len(self.routes_data["master_routes"]) < original_length:
            self._save_routes_config()
            print(f"✓ Permanently deleted route: {route_id}")
            return True
        
        print(f"Route with ID '{route_id}' not found")
        return False
    
    # ============ Platform URL Management ============
    
    def add_platform_url(self, route_id: str, platform: str, url: str) -> bool:
        """Add URL mapping for a specific platform"""
        if not self.get_route_by_id(route_id):
            print(f"Route ID '{route_id}' does not exist")
            return False
        
        if platform not in self.routes_data.get("platforms", {}):
            print(f"Platform '{platform}' not supported")
            return False
        
        self.routes_data["platforms"][platform]["routes"][route_id] = url
        self._save_routes_config()
        print(f"✓ Added {platform} URL for route {route_id}")
        return True
    
    def get_platform_url(self, route_id: str, platform: str) -> Optional[str]:
        """Get URL for specific platform and route"""
        platform_data = self.routes_data.get("platforms", {}).get(platform, {})
        return platform_data.get("routes", {}).get(route_id)
    
    def get_all_platform_urls(self, route_id: str) -> Dict[str, str]:
        """Get all platform URLs for a route"""
        urls = {}
        for platform_name, platform_data in self.routes_data.get("platforms", {}).items():
            url = platform_data.get("routes", {}).get(route_id)
            if url:
                urls[platform_name] = url
        return urls
    
    def update_platform_url(self, route_id: str, platform: str, url: str) -> bool:
        """Update URL for specific platform and route"""
        return self.add_platform_url(route_id, platform, url)
    
    def delete_platform_url(self, route_id: str, platform: str) -> bool:
        """Remove URL mapping for specific platform and route"""
        platform_routes = self.routes_data.get("platforms", {}).get(platform, {}).get("routes", {})
        if route_id in platform_routes:
            del platform_routes[route_id]
            self._save_routes_config()
            print(f"✓ Removed {platform} URL for route {route_id}")
            return True
        
        print(f"URL mapping not found for {platform}:{route_id}")
        return False
    
    # ============ URL Formatting Functions ============
    
    def format_url_for_date(self, route_id: str, platform: str, date_str: str) -> Optional[str]:
        """Format platform URL with specific date"""
        url_template = self.get_platform_url(route_id, platform)
        if not url_template:
            return None
        
        platform_data = self.routes_data.get("platforms", {}).get(platform, {})
        date_format = platform_data.get("date_format", "%Y-%m-%d")
        
        if platform == "redbus":
            return self._format_redbus_url(url_template, date_str)
        elif platform == "traveloka":
            return self._format_traveloka_url(url_template, date_str)
        else:
            # Generic formatting
            return url_template.replace("[[DATE]]", date_str)
    
    def _format_redbus_url(self, url_template: str, date_str: str) -> str:
        """Format Redbus URL with date placeholders"""
        try:
            from datetime import datetime
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day = str(date_obj.day).zfill(2)
            month = date_obj.strftime("%b")  # Short month name (Jan, Feb, etc.)
            year = str(date_obj.year)
            
            formatted_url = url_template.replace("[[DAY]]", day)
            formatted_url = formatted_url.replace("[[MONTH]]", month)
            formatted_url = formatted_url.replace("[[YEAR]]", year)
            
            return formatted_url
        except ValueError:
            print(f"Invalid date format: {date_str}. Expected YYYY-MM-DD")
            return url_template
    
    def _format_traveloka_url(self, url_template: str, date_str: str) -> str:
        """Format Traveloka URL with date"""
        # Traveloka typically uses YYYY-MM-DD format
        return url_template.replace("[[DATE]]", date_str)
    
    # ============ Utility Functions ============
    
    def _generate_route_id(self, name: str) -> str:
        """Generate route ID from route name"""
        # Remove special characters and convert to lowercase
        route_id = re.sub(r'[^a-zA-Z0-9\-]', '_', name.lower())
        route_id = re.sub(r'_+', '_', route_id).strip('_')
        
        # Shorten common patterns
        route_id = route_id.replace('jakarta', 'jkt')
        route_id = route_id.replace('surabaya', 'sby') 
        route_id = route_id.replace('semarang', 'smg')
        route_id = route_id.replace('malang', 'mlg')
        route_id = route_id.replace('lampung', 'lpg')
        route_id = route_id.replace('bandung', 'bdg')
        route_id = route_id.replace('yogyakarta', 'yka')
        route_id = route_id.replace('denpasar', 'dps')
        
        return route_id
    
    def get_routes_for_platform(self, platform: str, active_only: bool = True) -> Dict[str, str]:
        """Get all routes available for specific platform"""
        routes = {}
        platform_routes = self.routes_data.get("platforms", {}).get(platform, {}).get("routes", {})
        
        for route_id, url in platform_routes.items():
            route_info = self.get_route_by_id(route_id)
            if route_info and (not active_only or route_info.get("active", True)):
                routes[route_info["name"]] = url
        
        return routes
    
    def get_legacy_format(self, platform: str) -> Dict:
        """Get routes in legacy format for backward compatibility"""
        if platform == "redbus":
            return self.get_routes_for_platform("redbus")
        elif platform == "traveloka":
            # For Traveloka, return in the expected format
            routes = self.get_routes_for_platform("traveloka")
            return {0: routes}  # Traveloka expects route set format
        
        return {}
    
    def validate_url_format(self, platform: str, url: str) -> Tuple[bool, str]:
        """Validate URL format for specific platform"""
        if platform == "redbus":
            required_placeholders = ["[[DAY]]", "[[MONTH]]", "[[YEAR]]"]
            missing = [p for p in required_placeholders if p not in url]
            if missing:
                return False, f"Missing placeholders: {', '.join(missing)}"
        elif platform == "traveloka":
            if "[[DATE]]" not in url:
                return False, "Missing [[DATE]] placeholder"
        
        # Basic URL validation
        if not url.startswith(("http://", "https://")):
            return False, "URL must start with http:// or https://"
        
        return True, "Valid URL format"
    
    def export_routes(self, filename: str = None) -> str:
        """Export routes configuration to JSON file"""
        if filename is None:
            filename = f"routes_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.routes_data, f, indent=2, ensure_ascii=False)
            print(f"✓ Routes exported to {filename}")
            return filename
        except Exception as e:
            print(f"Error exporting routes: {e}")
            return ""
    
    def import_routes(self, filename: str, merge: bool = True) -> bool:
        """Import routes configuration from JSON file"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                imported_data = json.load(f)
            
            if merge:
                # Merge with existing data
                existing_route_ids = {r["id"] for r in self.routes_data.get("master_routes", [])}
                imported_routes = imported_data.get("master_routes", [])
                
                new_routes = [r for r in imported_routes if r["id"] not in existing_route_ids]
                self.routes_data["master_routes"].extend(new_routes)
                
                # Merge platform URLs
                for platform, platform_data in imported_data.get("platforms", {}).items():
                    if platform in self.routes_data.get("platforms", {}):
                        imported_urls = platform_data.get("routes", {})
                        self.routes_data["platforms"][platform]["routes"].update(imported_urls)
            else:
                # Replace all data
                self.routes_data = imported_data
            
            self._save_routes_config()
            print(f"✓ Routes imported from {filename}")
            return True
        except Exception as e:
            print(f"Error importing routes: {e}")
            return False


# ============ CLI Interface ============

def print_routes_table(routes: List[Dict]):
    """Print routes in table format"""
    if not routes:
        print("No routes found.")
        return
    
    print("\n" + "="*80)
    print(f"{'ID':<15} {'Name':<25} {'Origin':<15} {'Destination':<15} {'Active':<8}")
    print("-"*80)
    
    for route in routes:
        active_status = "✓" if route.get("active", True) else "✗"
        print(f"{route['id']:<15} {route['name']:<25} {route['origin']:<15} {route['destination']:<15} {active_status:<8}")
    
    print("="*80)

def interactive_cli():
    """Interactive command line interface"""
    manager = RouteManager()
    
    while True:
        print("\n" + "="*60)
        print("           ROUTES MANAGER - Interactive CLI")
        print("="*60)
        print("1. List all routes")
        print("2. Add new route") 
        print("3. Update route")
        print("4. Delete route")
        print("5. Manage platform URLs")
        print("6. Test URL formatting")
        print("7. Export/Import routes")
        print("0. Exit")
        print("-"*60)
        
        try:
            choice = input("Select option (0-7): ").strip()
            
            if choice == "0":
                print("Goodbye!")
                break
            elif choice == "1":
                active_only = input("Show active routes only? (y/n): ").strip().lower() == 'y'
                routes = manager.get_all_routes(active_only=active_only)
                print_routes_table(routes)
            elif choice == "2":
                name = input("Route name: ").strip()
                origin = input("Origin city: ").strip()
                destination = input("Destination city: ").strip()
                category = input("Category (default: intercity): ").strip() or "intercity"
                route_id = manager.add_route(name, origin, destination, category)
                print(f"Route created with ID: {route_id}")
            elif choice == "3":
                route_id = input("Route ID to update: ").strip()
                route = manager.get_route_by_id(route_id)
                if not route:
                    print("Route not found!")
                    continue
                
                print(f"Current route: {route}")
                updates = {}
                
                new_name = input(f"New name (current: {route['name']}): ").strip()
                if new_name: updates["name"] = new_name
                
                new_origin = input(f"New origin (current: {route['origin']}): ").strip()
                if new_origin: updates["origin"] = new_origin
                
                new_dest = input(f"New destination (current: {route['destination']}): ").strip()
                if new_dest: updates["destination"] = new_dest
                
                new_active = input(f"Active (current: {route['active']}) (y/n): ").strip()
                if new_active: updates["active"] = new_active.lower() == 'y'
                
                if updates:
                    manager.update_route(route_id, **updates)
                else:
                    print("No updates provided.")
            elif choice == "4":
                route_id = input("Route ID to delete: ").strip()
                hard_delete = input("Permanent delete? (y/n): ").strip().lower() == 'y'
                
                if hard_delete:
                    confirm = input("Are you sure? This cannot be undone! (yes/no): ").strip()
                    if confirm == "yes":
                        manager.hard_delete_route(route_id)
                    else:
                        print("Deletion cancelled.")
                else:
                    manager.delete_route(route_id)
            elif choice == "5":
                print("\nPlatform URL Management:")
                print("1. View URLs for route")
                print("2. Add/Update URL")
                print("3. Delete URL")
                
                sub_choice = input("Select option (1-3): ").strip()
                
                if sub_choice == "1":
                    route_id = input("Route ID: ").strip()
                    urls = manager.get_all_platform_urls(route_id)
                    if urls:
                        for platform, url in urls.items():
                            print(f"{platform}: {url}")
                    else:
                        print("No URLs found for this route.")
                elif sub_choice == "2":
                    route_id = input("Route ID: ").strip()
                    platform = input("Platform (redbus/traveloka): ").strip().lower()
                    url = input("URL template: ").strip()
                    
                    is_valid, message = manager.validate_url_format(platform, url)
                    if is_valid:
                        manager.add_platform_url(route_id, platform, url)
                    else:
                        print(f"Invalid URL: {message}")
                elif sub_choice == "3":
                    route_id = input("Route ID: ").strip()
                    platform = input("Platform (redbus/traveloka): ").strip().lower()
                    manager.delete_platform_url(route_id, platform)
            elif choice == "6":
                route_id = input("Route ID: ").strip()
                platform = input("Platform (redbus/traveloka): ").strip().lower()
                date_str = input("Date (YYYY-MM-DD): ").strip()
                
                formatted_url = manager.format_url_for_date(route_id, platform, date_str)
                if formatted_url:
                    print(f"Formatted URL: {formatted_url}")
                else:
                    print("Could not format URL. Check route ID and platform.")
            elif choice == "7":
                print("\nExport/Import:")
                print("1. Export routes")
                print("2. Import routes")
                
                sub_choice = input("Select option (1-2): ").strip()
                
                if sub_choice == "1":
                    filename = input("Export filename (optional): ").strip()
                    exported_file = manager.export_routes(filename if filename else None)
                    if exported_file:
                        print(f"Exported to: {exported_file}")
                elif sub_choice == "2":
                    filename = input("Import filename: ").strip()
                    merge = input("Merge with existing routes? (y/n): ").strip().lower() == 'y'
                    manager.import_routes(filename, merge=merge)
            else:
                print("Invalid option. Please try again.")
        
        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    # Run interactive CLI
    interactive_cli()