from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import threading
import json
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import glob
import queue
import re
from typing import Dict, List, Optional, Tuple

# Import crawler functions
from redbus import initialize_driver as init_redbus_driver, get_bus_detail as get_redbus_data
from redbus import routes as redbus_routes, dates as redbus_dates

# Import database module
from database import BusDatabase

# Import API V2 endpoints
from api_v2_endpoints import register_api_v2_routes


# ============ UNIFIED ROUTES MANAGEMENT SYSTEM ============

class RouteManager:
    """Centralized route management system integrated into unified crawler"""
    
    def __init__(self, config_file='routes_config.json', use_database=True):
        self.config_file = config_file
        self.use_database = use_database
        self.routes_data = self._load_routes_config()
        
        # Initialize database connection if enabled
        self.db = None
        if self.use_database:
            try:
                self.db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
                print("✓ RouteManager connected to database")
            except Exception as e:
                print(f"⚠ RouteManager database connection failed: {e}")
                print("✓ Falling back to JSON file storage")
                self.use_database = False
        
        if not self.use_database:
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
    
    def _save_routes_config(self):
        """Save routes configuration to file"""
        try:
            self.routes_data["last_updated"] = datetime.now().isoformat()
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.routes_data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving routes config: {e}")
    
    # ============ CRUD Operations for Master Routes ============
    
    def add_route(self, name: str, origin: str, destination: str, 
                  category: str = "intercity", active: bool = True) -> str:
        """Add a new master route to database or JSON"""
        route_id = self._generate_route_id(name)
        
        if self.get_route_by_id(route_id):
            raise ValueError(f"Route with ID '{route_id}' already exists")
        
        if self.use_database and self.db:
            try:
                # Insert into database
                sql = """
                    INSERT INTO routes (id, name, origin, destination, category, active, created_at, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
                """
                if self.db.db_type == 'sqlite':
                    sql = """
                        INSERT INTO routes (id, name, origin, destination, category, active, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    """
                
                self.db.cursor.execute(sql, (route_id, name, origin, destination, category, active))
                self.db.conn.commit()
                return route_id
            except Exception as e:
                print(f"Database error in add_route: {e}")
                self.db.conn.rollback()
                # Fall back to JSON
                pass
        
        # JSON fallback or default
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
        return route_id
    
    def get_all_routes(self, active_only: bool = False) -> List[Dict]:
        """Get all master routes from database or JSON file"""
        if self.use_database and self.db:
            try:
                where_clause = "WHERE active = 1" if active_only else ""
                sql = f"SELECT id, name, origin, destination, category, active, redbus_url, created_at, updated_at FROM routes {where_clause} ORDER BY name"
                self.db.cursor.execute(sql)
                rows = self.db.cursor.fetchall()
                
                routes = []
                for row in rows:
                    if isinstance(row, dict):
                        # MySQL returns dict
                        route = {
                            'id': row['id'],
                            'name': row['name'],
                            'origin': row['origin'],
                            'destination': row['destination'],
                            'category': row['category'],
                            'active': bool(row['active']),
                            'platforms': {
                                'redbus': row.get('redbus_url') is not None and row.get('redbus_url') != '',
                            },
                            'created_at': str(row['created_at']) if row.get('created_at') else None,
                            'updated_at': str(row['updated_at']) if row.get('updated_at') else None
                        }
                    else:
                        # SQLite/PostgreSQL returns tuple
                        route = {
                            'id': row[0],
                            'name': row[1],
                            'origin': row[2],
                            'destination': row[3],
                            'category': row[4],
                            'active': bool(row[5]),
                            'platforms': {
                                'redbus': row[6] is not None and row[6] != '',
                            },
                            'created_at': str(row[8]) if row[8] else None,
                            'updated_at': str(row[9]) if row[9] else None
                        }
                    routes.append(route)
                
                return routes
            except Exception as e:
                print(f"Database error in get_all_routes: {e}")
                # Fall back to JSON
                routes = self.routes_data.get("master_routes", [])
                if active_only:
                    routes = [r for r in routes if r.get("active", True)]
                # Add platform info for JSON fallback
                for route in routes:
                    if 'platforms' not in route:
                        route['platforms'] = {
                            'redbus': self.get_platform_url(route['id'], 'redbus') is not None,
                        }
                return routes
        else:
            routes = self.routes_data.get("master_routes", [])
            if active_only:
                routes = [r for r in routes if r.get("active", True)]
            # Add platform info for JSON
            for route in routes:
                if 'platforms' not in route:
                    route['platforms'] = {
                        'redbus': self.get_platform_url(route['id'], 'redbus') is not None,
                    }
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
                allowed_fields = ["name", "origin", "destination", "category", "active"]
                for field, value in kwargs.items():
                    if field in allowed_fields:
                        route[field] = value
                
                route["updated_at"] = datetime.now().isoformat()
                self.routes_data["master_routes"][i] = route
                self._save_routes_config()
                return True
        return False
    
    def delete_route(self, route_id: str) -> bool:
        """Delete route by ID (soft delete)"""
        return self.update_route(route_id, active=False)
    
    # ============ Platform URL Management ============
    
    def add_platform_url(self, route_id: str, platform: str, url: str) -> bool:
        """Add URL mapping for a specific platform"""
        if not self.get_route_by_id(route_id):
            return False
        
        if platform not in self.routes_data.get("platforms", {}):
            return False
        
        self.routes_data["platforms"][platform]["routes"][route_id] = url
        self._save_routes_config()
        return True
    
    def get_platform_url(self, route_id: str, platform: str) -> Optional[str]:
        """Get URL for specific platform and route from database or JSON"""
        if self.use_database and self.db:
            try:
                column_name = f"{platform}_url"
                sql = f"SELECT {column_name} FROM routes WHERE id = %s"
                if self.db.db_type == 'sqlite':
                    sql = f"SELECT {column_name} FROM routes WHERE id = ?"
                
                self.db.cursor.execute(sql, (route_id,))
                result = self.db.cursor.fetchone()
                
                if result:
                    return result[0] if isinstance(result, tuple) else result[column_name]
                return None
            except Exception as e:
                print(f"Database error in get_platform_url: {e}")
                # Fall back to JSON
                platform_data = self.routes_data.get("platforms", {}).get(platform, {})
                return platform_data.get("routes", {}).get(route_id)
        else:
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
    
    def format_url_for_date(self, route_id: str, platform: str, date_str: str) -> Optional[str]:
        """Format platform URL with specific date"""
        url_template = self.get_platform_url(route_id, platform)
        if not url_template:
            return None
        
        if platform == "redbus":
            return self._format_redbus_url(url_template, date_str)
        else:
            return url_template.replace("[[DATE]]", date_str)
    
    def _format_redbus_url(self, url_template: str, date_str: str) -> str:
        """Format Redbus URL with date placeholders"""
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d")
            day = str(date_obj.day).zfill(2)
            month = date_obj.strftime("%b")  # Short month name
            year = str(date_obj.year)
            
            formatted_url = url_template.replace("[[DAY]]", day)
            formatted_url = formatted_url.replace("[[MONTH]]", month)
            formatted_url = formatted_url.replace("[[YEAR]]", year)
            
            return formatted_url
        except ValueError:
            return url_template
        
    def get_routes_for_platform(self, platform: str, active_only: bool = True) -> Dict[str, str]:
        """Get all routes available for specific platform"""
        routes = {}
        platform_routes = self.routes_data.get("platforms", {}).get(platform, {}).get("routes", {})
        
        for route_id, url in platform_routes.items():
            route_info = self.get_route_by_id(route_id)
            if route_info and (not active_only or route_info.get("active", True)):
                routes[route_info["name"]] = url
        
        return routes
    
    def _generate_route_id(self, name: str) -> str:
        """Generate route ID from route name"""
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
        
        return route_id


class URLFormatter:
    """URL formatting and validation for different platforms"""
    
    def __init__(self, routes_manager: RouteManager):
        self.routes_manager = routes_manager
    
    def format_redbus_url(self, route_name: str, date_str: str) -> Optional[str]:
        """Format Redbus URL for specific route and date"""
        route = self.routes_manager.get_route_by_name(route_name)
        if not route:
            return None
        
        return self.routes_manager.format_url_for_date(route["id"], "redbus", date_str)

class CrawlTaskGenerator:
    """Generate crawling tasks for different platforms"""
    
    def __init__(self, routes_manager: RouteManager):
        self.routes_manager = routes_manager
        self.url_formatter = URLFormatter(routes_manager)
    
    def generate_redbus_tasks(self, route_names: List[str], dates: List[str]) -> List[Dict]:
        """Generate tasks for Redbus crawling"""
        tasks = []
        
        for route_name in route_names:
            route = self.routes_manager.get_route_by_name(route_name)
            if not route:
                continue
            
            url_template = self.routes_manager.get_platform_url(route["id"], "redbus")
            if not url_template:
                continue
            
            for date_str in dates:
                formatted_url = self.routes_manager.format_url_for_date(route["id"], "redbus", date_str)
                
                task = {
                    "route": route_name,
                    "url": formatted_url,
                    "date": date_str
                }
                tasks.append(task)
        
        return tasks
    
# Load configuration
def load_config():
    config_file = 'config.json'
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            return json.load(f)
    return {
        'server': {'host': '0.0.0.0', 'debug': False, 'unified_port': 5002},
        'crawler': {'default_workers': 3, 'max_workers': 5}
    }

def load_db_config():
    """Load database configuration"""
    config_file = 'config_db.json'
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
            return config.get('database', {})
    return {'type': 'sqlite', 'database': 'data/bus_data.db'}

config = load_config()
db_config = load_db_config()

# Initialize unified route management system
route_manager = RouteManager()
task_generator = CrawlTaskGenerator(route_manager)

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'unified_crawler_secret!')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global variables for crawling state
crawling_state = {
    'redbus': {
        'is_running': False,
        'progress': 0,
        'total_tasks': 0,
        'current_tasks': [],
        'completed_tasks': 0,
        'logs': [],
        'stats': {
            'total_scraped': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
    }
}

# Global variables for ML training state
training_state = {
    'is_running': False,
    'progress': 0,
    'current_step': '',
    'logs': [],
    'results': None,
    'start_time': None,
    'end_time': None
}

# Thread references
crawl_threads = {'redbus': None}
training_thread = None

def log_message(platform, message, level='info'):
    """Add a log message for specific platform"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {
        'timestamp': timestamp,
        'level': level,
        'message': message,
        'platform': platform
    }
    
    # Route to appropriate logs storage
    if platform == 'training':
        training_state['logs'].append(log_entry)
        if len(training_state['logs']) > 100:
            training_state['logs'] = training_state['logs'][-100:]
    else:
        crawling_state[platform]['logs'].append(log_entry)
        if len(crawling_state[platform]['logs']) > 100:
            crawling_state[platform]['logs'] = crawling_state[platform]['logs'][-100:]
    
    socketio.emit('log_update', log_entry)

def update_progress(platform):
    """Update progress for specific platform"""
    completed = crawling_state[platform]['completed_tasks']
    total = crawling_state[platform]['total_tasks']
    crawling_state[platform]['progress'] = int((completed / total) * 100) if total > 0 else 0
    
    socketio.emit('progress_update', {
        'platform': platform,
        'progress': crawling_state[platform]['progress'],
        'completed': completed,
        'total': total,
        'current_task': crawling_state[platform].get('current_task', ''),
        'current_tasks': crawling_state[platform].get('current_tasks', [])
    })

# Redbus normal sequential crawling
def redbus_worker(tasks, max_buses=None, max_scroll=None, filter_buses=None):
    """
    Sequential worker function for Redbus crawling using RouteManager
    Args:
        tasks: List of crawling tasks
        max_buses: Maximum buses to scrape per task (None = unlimited)
        max_scroll: Maximum scroll iterations per task (None = unlimited)
        filter_buses: List of bus company names to filter (None = all buses)
    """
    driver = None
    db = None
    
    try:
        crawling_state['redbus']['is_running'] = True
        crawling_state['redbus']['stats']['start_time'] = datetime.now().isoformat()
        crawling_state['redbus']['completed_tasks'] = 0
        crawling_state['redbus']['current_tasks'] = []
        
        log_message('redbus', f'Starting sequential crawling...', 'info')
        log_message('redbus', f'Total tasks: {len(tasks)}', 'info')
        if max_buses:
            log_message('redbus', f'Max buses limit: {max_buses} per task', 'info')
        if max_scroll:
            log_message('redbus', f'Max scroll limit: {max_scroll} iterations per task', 'info')
        if filter_buses:
            log_message('redbus', f'Filtering bus companies: {", ".join(filter_buses)}', 'info')
        
        # Initialize database connection
        try:
            db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
            log_message('redbus', f'✓ Connected to {db_config.get("type", "sqlite").upper()} database', 'success')
        except Exception as db_error:
            log_message('redbus', f'⚠ Database connection failed: {str(db_error)}', 'warning')
            log_message('redbus', 'Continuing without database insertion', 'warning')
            db = None
        
        driver = init_redbus_driver()
        
        for idx, task_info in enumerate(tasks):
            if not crawling_state['redbus']['is_running']:
                log_message('redbus', 'Crawling stopped by user', 'warning')
                break
            
            # Handle new task format from RouteManager
            route_name = task_info.get('route', task_info.get('route_name', ''))
            url = task_info['url']
            date = task_info['date']
            task_name = f"{route_name} - Date: {date}"
            
            try:
                crawling_state['redbus']['current_tasks'] = [{'task': task_name}]
                crawling_state['redbus']['current_task'] = task_name
                socketio.emit('task_start', {
                    'platform': 'redbus',
                    'task': task_name
                })
                
                log_message('redbus', f'Starting {task_name}...', 'info')
                
                bus_details = get_redbus_data(driver, url, route_name, date, max_buses=max_buses, max_scroll=max_scroll, filter_buses=filter_buses)
                
                if bus_details:
                    # Save to CSV file
                    df = pd.DataFrame(bus_details)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f'new_data/redbus_{route_name}-{date}_{timestamp}.csv'
                    df.to_csv(filename, index=False, encoding='utf-8')
                    
                    # Insert to database
                    if db:
                        try:
                            db_stats = db.insert_bulk_data(bus_details, platform='redbus')
                            log_message('redbus', 
                                      f'💾 Database: {db_stats["inserted"]} records inserted', 
                                      'info')
                        except Exception as db_error:
                            log_message('redbus', f'⚠ Database insert error: {str(db_error)}', 'warning')
                    
                    crawling_state['redbus']['stats']['successful'] += 1
                    crawling_state['redbus']['stats']['total_scraped'] += len(bus_details)
                    log_message('redbus', f'✓ Scraped {len(bus_details)} buses - Saved to {filename}', 'success')
                else:
                    crawling_state['redbus']['stats']['failed'] += 1
                    log_message('redbus', f'⚠ No data for {task_name}', 'warning')
                
            except Exception as e:
                crawling_state['redbus']['stats']['failed'] += 1
                log_message('redbus', f'✗ Error on {task_name}: {str(e)}', 'error')
            
            finally:
                crawling_state['redbus']['completed_tasks'] += 1
                update_progress('redbus')
                crawling_state['redbus']['current_tasks'] = []
            
            # Small delay between tasks
            if idx < len(tasks) - 1:
                time.sleep(1)
        
        log_message('redbus', 'All tasks completed!', 'success')
        
    except Exception as e:
        log_message('redbus', f'Fatal error: {str(e)}', 'error')
    
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        
        if db:
            try:
                db.close()
                log_message('redbus', 'Database connection closed', 'info')
            except:
                pass
        
        crawling_state['redbus']['is_running'] = False
        crawling_state['redbus']['stats']['end_time'] = datetime.now().isoformat()
        crawling_state['redbus']['current_tasks'] = []
        update_progress('redbus')

# ML Training worker
def train_model_worker(days_back=90):
    """Worker function for ML model training"""
    try:
        training_state['is_running'] = True
        training_state['progress'] = 0
        training_state['current_step'] = 'Initializing...'
        training_state['start_time'] = datetime.now().isoformat()
        training_state['logs'] = []
        
        log_message('training', f'Starting model training with {days_back} days of data...', 'info')
        
        # Import ML training module
        from train_bus_prediction_model import BusPredictionModel
        
        training_state['progress'] = 10
        training_state['current_step'] = 'Loading data from database...'
        socketio.emit('training_progress', {
            'progress': training_state['progress'],
            'step': training_state['current_step']
        })
        
        log_message('training', 'Connecting to database...', 'info')
        
        # Initialize database
        db = BusDatabase(db_type=db_config.get('type', 'mysql'), db_config=db_config)
        
        # Load data
        log_message('training', f'Loading data from last {days_back} days...', 'info')
        data = db.get_training_data(days_back=days_back)
        
        if data.empty:
            log_message('training', '⚠ No data found in database for training', 'error')
            training_state['results'] = {'error': 'No data available'}
            return
        
        log_message('training', f'✓ Loaded {len(data)} records', 'success')
        
        training_state['progress'] = 30
        training_state['current_step'] = 'Preprocessing data...'
        socketio.emit('training_progress', {
            'progress': training_state['progress'],
            'step': training_state['current_step']
        })
        
        # Initialize model
        log_message('training', 'Initializing prediction model...', 'info')
        model = BusPredictionModel()
        
        training_state['progress'] = 40
        training_state['current_step'] = 'Training models...'
        socketio.emit('training_progress', {
            'progress': training_state['progress'],
            'step': training_state['current_step']
        })
        
        log_message('training', 'Training Random Forest models...', 'info')
        
        # Train model
        results = model.train(data)
        
        training_state['progress'] = 80
        training_state['current_step'] = 'Saving models...'
        socketio.emit('training_progress', {
            'progress': training_state['progress'],
            'step': training_state['current_step']
        })
        
        log_message('training', 'Saving trained models...', 'info')
        model.save_model()
        
        training_state['progress'] = 100
        training_state['current_step'] = 'Complete'
        socketio.emit('training_progress', {
            'progress': training_state['progress'],
            'step': training_state['current_step']
        })
        
        # Store results
        training_state['results'] = {
            'metrics': results,
            'data_points': len(data),
            'days_back': days_back,
            'timestamp': datetime.now().isoformat()
        }
        
        log_message('training', '✓ Training completed successfully!', 'success')
        log_message('training', f'MAE: {results["mae"]:.2f}', 'info')
        log_message('training', f'RMSE: {results["rmse"]:.2f}', 'info')
        log_message('training', f'R² Score: {results["r2"]:.4f}', 'info')
        
        db.close()
        
    except Exception as e:
        log_message('training', f'✗ Training error: {str(e)}', 'error')
        training_state['results'] = {'error': str(e)}
    
    finally:
        training_state['is_running'] = False
        training_state['end_time'] = datetime.now().isoformat()

@app.route('/')
def index():
    """Main unified interface"""
    return render_template('unified.html')

@app.route('/api/status')
def get_status():
    """Get status of both crawlers"""
    return jsonify(crawling_state)

@app.route('/api/routes/<platform>')
def get_routes(platform):
    """Get routes for specific platform using RouteManager"""
    if platform == 'redbus':
        # Get routes from RouteManager
        routes_dict = route_manager.get_routes_for_platform("redbus", active_only=True)
        dates = ["15","16","17","18","19","20","21","22","23","24","25","26","27","28","29","30","31"]
        
        return jsonify({
            'routes': list(routes_dict.keys()),
            'dates': dates
        })

    elif platform == 'all':
        # Return all routes managed by RouteManager
        all_routes = route_manager.get_all_routes(active_only=True)
        return jsonify([{
            'id': route['id'],
            'name': route['name'],
            'origin': route['origin'],
            'destination': route['destination'],
            'platforms': route_manager.get_all_platform_urls(route['id'])
        } for route in all_routes])
    else:
        return jsonify({'error': 'Invalid platform'}), 400

@app.route('/api/start/<platform>', methods=['POST'])
def start_crawling(platform):
    """Start crawling for specific platform using RouteManager"""
    if platform not in ['redbus']:
        return jsonify({'error': 'Invalid platform'}), 400
    
    if crawling_state[platform]['is_running']:
        return jsonify({'error': f'{platform.capitalize()} crawler already running'}), 400
    
    data = request.json
    print(f"Received start request for {platform} with data: {data}")
    
    # Reset stats
    crawling_state[platform]['stats'] = {
        'total_scraped': 0,
        'successful': 0,
        'failed': 0,
        'start_time': None,
        'end_time': None
    }
    crawling_state[platform]['logs'] = []
    crawling_state[platform]['progress'] = 0
    crawling_state[platform]['completed_tasks'] = 0
    
    os.makedirs('new_data', exist_ok=True)
    
    if platform == 'redbus':
        selected_routes = data.get('routes', [])
        selected_dates = data.get('dates', [])
        max_buses = data.get('max_buses', None)  # Get max_buses parameter
        max_scroll = data.get('max_scroll', None)  # Get max_scroll parameter
        bus_names = data.get('bus_names', [])  # Get bus names filter
        
        # Convert max_buses to int if provided
        if max_buses is not None:
            try:
                max_buses = int(max_buses)
                if max_buses <= 0:
                    max_buses = None
            except (ValueError, TypeError):
                max_buses = None
        
        # Convert max_scroll to int if provided
        if max_scroll is not None:
            try:
                max_scroll = int(max_scroll)
                if max_scroll <= 0:
                    max_scroll = None
            except (ValueError, TypeError):
                max_scroll = None
        
        # Convert dates to full format
        dates = []
        for date in selected_dates:
            if date.isdigit() and len(date) <= 2:
                date_full = f"2025-12-{date.zfill(2)}"
            else:
                date_full = date
            dates.append(date_full)
        
        # Generate tasks using RouteManager
        tasks = task_generator.generate_redbus_tasks(selected_routes, dates)
        
        crawling_state['redbus']['total_tasks'] = len(tasks)
        crawling_state['redbus']['max_buses'] = max_buses  # Store in state
        crawling_state['redbus']['max_scroll'] = max_scroll  # Store in state
        crawling_state['redbus']['filter_buses'] = bus_names  # Store in state
        
        thread = threading.Thread(
            target=redbus_worker, 
            args=(tasks, max_buses, max_scroll, bus_names if bus_names else None)
        )
        thread.start()
        crawl_threads['redbus'] = thread
        
        response_msg = f'Redbus crawling started with {len(tasks)} tasks'
        if max_buses:
            response_msg += f' (max {max_buses} buses per task)'
        if max_scroll:
            response_msg += f' (max {max_scroll} scrolls per task)'
        if bus_names:
            response_msg += f' (filtering: {", ".join(bus_names)})'
        
        return jsonify({
            'message': response_msg,
            'total_tasks': len(tasks),
            'max_buses': max_buses,
            'max_scroll': max_scroll,
            'filter_buses': bus_names
        })

@app.route('/api/stop/<platform>', methods=['POST'])
def stop_crawling(platform):
    """Stop crawling for specific platform"""
    if platform not in ['redbus']:
        return jsonify({'error': 'Invalid platform'}), 400
    
    if not crawling_state[platform]['is_running']:
        return jsonify({'error': f'{platform.capitalize()} crawler not running'}), 400
    
    crawling_state[platform]['is_running'] = False
    log_message(platform, 'Stop requested by user', 'warning')
    
    return jsonify({'message': f'{platform.capitalize()} will stop after current task'})

@app.route('/api/data/<platform>')
def get_scraped_data(platform):
    """Get list of scraped data files for platform"""
    if platform not in ['redbus', 'all']:
        return jsonify({'error': 'Invalid platform'}), 400
    
    data_files = []
    
    if os.path.exists('new_data'):
        pattern = f'new_data/{platform}_*.csv' if platform != 'all' else 'new_data/*.csv'
        for filename in glob.glob(pattern):
            try:
                df = pd.read_csv(filename)
                file_stat = os.stat(filename)
                
                # Determine platform from filename
                file_platform = 'traveloka' if 'traveloka' in filename else 'redbus'
                
                data_files.append({
                    'filename': os.path.basename(filename),
                    'path': filename,
                    'platform': file_platform,
                    'size': file_stat.st_size,
                    'rows': len(df),
                    'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                })
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    
    data_files.sort(key=lambda x: x['modified'], reverse=True)
    return jsonify(data_files)


@app.route('/api/data/db')
def query_database_data():
    """Query bus data stored in the database with filters.

    Query parameters:
      - platform (optional)
      - route_name (optional)
      - date (optional, YYYY-MM-DD)
      - bus_name (optional)
      - limit (optional)
    """
    try:
        platform = request.args.get('platform')
        route_name = request.args.get('route_name')
        date = request.args.get('date')
        bus_name = request.args.get('bus_name')
        limit = request.args.get('limit')

        db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)

        # Use query_data to fetch rows (limit default handled by function)
        qlimit = int(limit) if limit and limit.isdigit() else None

        # If qlimit is None, pass a large number to avoid implicit LIMIT in query_data (it accepts limit param)
        df = db.query_data(platform=platform, route_name=route_name, route_date=date, limit=qlimit or 1000000)

        # If bus_name filter provided, apply on DataFrame
        if bus_name and not df.empty:
            if 'bus_name' in df.columns:
                df = df[df['bus_name'] == bus_name]

        # Convert DataFrame to list of dicts safely
        result = []
        if not df.empty:
            for _, row in df.iterrows():
                record = {}
                for col in df.columns:
                    val = row[col]
                    # Convert datetimes
                    if hasattr(val, 'isoformat'):
                        record[col] = val.isoformat()
                    else:
                        try:
                            # numpy types
                            record[col] = val.item() if hasattr(val, 'item') else val
                        except Exception:
                            record[col] = val
                result.append(record)

        db.close()
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data/preview/<filename>')
def get_file_data(filename):
    """Get preview of a specific data file"""
    filepath = os.path.join('new_data', filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        df = pd.read_csv(filepath)
        
        stats = {
            'unique_buses': df['Bus_Name'].nunique() if 'Bus_Name' in df.columns else 0,
            'unique_types': df['Bus_Type'].nunique() if 'Bus_Type' in df.columns else 0,
        }
        
        if 'Price' in df.columns:
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            stats['avg_price'] = float(df['Price'].mean())
            stats['min_price'] = float(df['Price'].min())
            stats['max_price'] = float(df['Price'].max())
        
        return jsonify({
            'filename': filename,
            'rows': len(df),
            'columns': df.columns.tolist(),
            'preview': df.head(20).to_dict('records'),
            'stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics')
def get_analytics():
    """Generate analytics summary for a specific route and date"""
    try:
        platform = request.args.get('platform', 'traveloka')
        route = request.args.get('route', '')
        date = request.args.get('date', '')
        
        if not route or not date:
            return jsonify({'error': 'Route and date are required'}), 400
        
        # Find all files matching the criteria (multiple crawls per day)
        pattern = f'new_data/{platform}_{route}-{date}_*.csv'
        files = glob.glob(pattern)
        
        if not files:
            return jsonify({'error': 'No data found for the specified criteria'}), 404
        
        # Sort files by timestamp to get crawl order
        files.sort()
        
        analytics = {
            'platform': platform,
            'route': route,
            'date': date,
            'total_crawls': len(files),
            'crawl_sessions': [],
            'summary': {
                'bus_companies': {},  # Company -> total count
                'bus_types_by_company': {},  # Company -> {Type -> count}
                'total_unique_buses': 0,
                'total_unique_types': 0,
                'crawl_times': []
            }
        }
        
        all_bus_names = set()
        all_bus_types = set()
        
        # Process each file (crawl session)
        for idx, file in enumerate(files):
            df = pd.read_csv(file)
            
            # Extract timestamp from filename
            filename = os.path.basename(file)
            timestamp_str = filename.split('_')[-1].replace('.csv', '')
            crawl_time = f"{timestamp_str[:2]}:{timestamp_str[2:4]}:{timestamp_str[4:6]}"
            
            session_data = {
                'crawl_number': idx + 1,
                'filename': filename,
                'crawl_time': crawl_time,
                'total_buses': len(df),
                'companies': {},
                'bus_types': {}
            }
            
            # Normalize column names
            df.columns = df.columns.str.strip()
            
            # Count by company
            if 'Bus_Name' in df.columns or 'Bus Name' in df.columns:
                bus_name_col = 'Bus_Name' if 'Bus_Name' in df.columns else 'Bus Name'
                company_counts = df[bus_name_col].value_counts().to_dict()
                session_data['companies'] = company_counts
                
                # Update summary
                for company, count in company_counts.items():
                    all_bus_names.add(company)
                    analytics['summary']['bus_companies'][company] = \
                        analytics['summary']['bus_companies'].get(company, 0) + count
            
            # Count by bus type
            if 'Bus_Type' in df.columns or 'Bus Type' in df.columns:
                bus_type_col = 'Bus_Type' if 'Bus_Type' in df.columns else 'Bus Type'
                type_counts = df[bus_type_col].value_counts().to_dict()
                session_data['bus_types'] = type_counts
                
                # Update summary by company and type
                if bus_name_col in df.columns:
                    for _, row in df.iterrows():
                        company = str(row[bus_name_col])
                        bus_type = str(row[bus_type_col])
                        all_bus_types.add(bus_type)
                        
                        if company not in analytics['summary']['bus_types_by_company']:
                            analytics['summary']['bus_types_by_company'][company] = {}
                        
                        analytics['summary']['bus_types_by_company'][company][bus_type] = \
                            analytics['summary']['bus_types_by_company'][company].get(bus_type, 0) + 1
            
            analytics['crawl_sessions'].append(session_data)
            analytics['summary']['crawl_times'].append(crawl_time)
        
        analytics['summary']['total_unique_buses'] = len(all_bus_names)
        analytics['summary']['total_unique_types'] = len(all_bus_types)
        
        return jsonify(analytics)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============ ROUTES MANAGEMENT API ============

@app.route('/routes')
def routes_management():
    """Routes management web interface"""
    return render_template('routes_management.html')

@app.route('/api/routes-manager/routes')
def api_get_all_routes():
    """Get all master routes"""
    active_only = request.args.get('active_only', 'false').lower() == 'true'
    routes = route_manager.get_all_routes(active_only=active_only)
    return jsonify(routes)

@app.route('/api/routes-manager/routes', methods=['POST'])
def api_add_route():
    """Add a new route"""
    try:
        data = request.json
        route_id = route_manager.add_route(
            name=data['name'],
            origin=data['origin'],
            destination=data['destination'],
            category=data.get('category', 'intercity'),
            active=data.get('active', True)
        )
        return jsonify({'success': True, 'route_id': route_id})
    except ValueError as e:
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/routes/<route_id>', methods=['PUT'])
def api_update_route(route_id):
    """Update a route"""
    try:
        data = request.json
        success = route_manager.update_route(route_id, **data)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Route not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/routes/<route_id>', methods=['DELETE'])
def api_delete_route(route_id):
    """Delete a route"""
    try:
        hard_delete = request.args.get('hard', 'false').lower() == 'true'
        if hard_delete:
            success = route_manager.hard_delete_route(route_id)
        else:
            success = route_manager.delete_route(route_id)
        
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Route not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/routes/<route_id>/urls')
def api_get_route_urls(route_id):
    """Get all platform URLs for a route"""
    urls = route_manager.get_all_platform_urls(route_id)
    return jsonify(urls)

@app.route('/api/routes-manager/routes/<route_id>/urls/<platform>', methods=['POST'])
def api_add_route_url(route_id, platform):
    """Add URL for specific platform"""
    try:
        data = request.json
        url = data['url']
        success = route_manager.add_platform_url(route_id, platform, url)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'Failed to add URL'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/routes/<route_id>/urls/<platform>', methods=['DELETE'])
def api_delete_route_url(route_id, platform):
    """Delete URL for specific platform"""
    try:
        success = route_manager.delete_platform_url(route_id, platform)
        if success:
            return jsonify({'success': True})
        else:
            return jsonify({'error': 'URL not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/test-url')
def api_test_url():
    """Test URL formatting"""
    try:
        route_id = request.args.get('route_id')
        platform = request.args.get('platform')
        date_str = request.args.get('date')
        
        if not all([route_id, platform, date_str]):
            return jsonify({'error': 'Missing required parameters'}), 400
        
        formatted_url = route_manager.format_url_for_date(route_id, platform, date_str)
        if formatted_url:
            return jsonify({'formatted_url': formatted_url})
        else:
            return jsonify({'error': 'Could not format URL'}), 400
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/generate-tasks', methods=['POST'])
def api_generate_tasks():
    """Generate crawling tasks"""
    try:
        data = request.json
        platform = data['platform']
        routes = data['routes']
        dates = data['dates']
        
        if platform == 'redbus':
            tasks = task_generator.generate_redbus_tasks(routes, dates)
        else:
            return jsonify({'error': 'Invalid platform'}), 400
        
        return jsonify({'tasks': tasks, 'count': len(tasks)})
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/export')
def api_export_routes():
    """Export routes configuration"""
    try:
        filename = route_manager.export_routes()
        if filename and os.path.exists(filename):
            return send_file(filename, as_attachment=True)
        else:
            return jsonify({'error': 'Export failed'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/routes-manager/import', methods=['POST'])
def api_import_routes():
    """Import routes configuration"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Save uploaded file temporarily
        temp_filename = f"temp_import_{int(time.time())}.json"
        file.save(temp_filename)
        
        try:
            # Import the routes
            merge = request.form.get('merge', 'true').lower() == 'true'
            success = route_manager.import_routes(temp_filename, merge=merge)
            
            if success:
                return jsonify({'success': True})
            else:
                return jsonify({'error': 'Import failed'}), 400
        finally:
            # Cleanup temp file
            if os.path.exists(temp_filename):
                os.remove(temp_filename)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    """Download a data file"""
    filepath = os.path.join('new_data', filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(filepath, as_attachment=True)

# ============ ML Training & Prediction API ============

@app.route('/api/train/start', methods=['POST'])
def start_training():
    """Start ML model training"""
    global training_thread
    
    if training_state['is_running']:
        return jsonify({'error': 'Training already in progress'}), 400
    
    data = request.json
    days_back = data.get('days_back', 90)
    
    # Validate days_back
    if not isinstance(days_back, int) or days_back < 7 or days_back > 365:
        return jsonify({'error': 'days_back must be between 7 and 365'}), 400
    
    # Reset state
    training_state['logs'] = []
    training_state['results'] = None
    training_state['progress'] = 0
    
    # Start training in background thread
    training_thread = threading.Thread(target=train_model_worker, args=(days_back,))
    training_thread.start()
    
    return jsonify({
        'message': 'Training started',
        'days_back': days_back
    })

@app.route('/api/train/status')
def get_training_status():
    """Get current training status"""
    return jsonify({
        'is_running': training_state['is_running'],
        'progress': training_state['progress'],
        'current_step': training_state['current_step'],
        'results': training_state['results'],
        'start_time': training_state['start_time'],
        'end_time': training_state['end_time']
    })

@app.route('/api/train/results')
def get_training_results():
    """Get training results"""
    if training_state['results'] is None:
        return jsonify({'error': 'No training results available'}), 404
    
    return jsonify(training_state['results'])

@app.route('/api/routes/available')
def get_available_routes():
    """Get available routes from database"""
    try:
        db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
        
        # Query distinct routes
        sql = "SELECT DISTINCT route_name FROM bus_data ORDER BY route_name"
        db.cursor.execute(sql)
        rows = db.cursor.fetchall()
        
        routes = [row[0] if isinstance(row, tuple) else row['route_name'] for row in rows]
        
        db.close()
        return jsonify(routes)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/predict', methods=['POST'])
def make_predictions():
    """Generate predictions"""
    try:
        data = request.json
        days = data.get('days', 7)  # Number of days to predict
        route_filter = data.get('route', None)  # Optional route filter
        start_date = data.get('start_date', None)
        end_date = data.get('end_date', None)

        if start_date and end_date:
            log_message('training', f'Generating predictions for range {start_date} to {end_date}...', 'info')
        else:
            log_message('training', f'Generating predictions for next {days} days...', 'info')

        if route_filter:
            log_message('training', f'Route filter: {route_filter}', 'info')
        
        # Import prediction module
        from predict_bus_availability import BusPredictor
        
        # Initialize predictor
        predictor = BusPredictor(db_config=db_config)
        
        # Generate predictions: date range if provided, otherwise custom days
        if start_date and end_date:
            predictions_df = predictor.predict_date_range(start_date, end_date, route_filter=route_filter)
        else:
            predictions_df = predictor.predict_custom_days(days=days, route_filter=route_filter)
        
        if predictions_df.empty:
            log_message('training', '⚠ No predictions generated', 'warning')
            return jsonify({'error': 'No predictions generated'}), 500
        
        # Determine period name for saving
        if start_date and end_date:
            period = 'custom_range'
        else:
            if days <= 7:
                period = 'next_week'
            elif days <= 30:
                period = 'next_month'
            elif days <= 365:
                period = 'next_year'
            else:
                period = 'custom'
        
        # Save to database
        session_id = predictor.save_predictions(predictions_df, period=period)
        
        log_message('training', f'✓ Generated {len(predictions_df)} predictions', 'success')
        log_message('training', f'Session ID: {session_id}', 'info')
        
        # Convert to dict for JSON response
        predictions_list = predictions_df.to_dict('records')
        
        return jsonify({
            'session_id': session_id,
            'days': days,
            'route': route_filter,
            'total_predictions': len(predictions_df),
            'predictions': predictions_list[:100]  # Limit response size
        })
    
    except Exception as e:
        log_message('training', f'✗ Prediction error: {str(e)}', 'error')
        return jsonify({'error': str(e)}), 500

@app.route('/api/predictions/history')
def get_predictions_history():
    """Get prediction history"""
    try:
        db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
        sessions_df = db.get_prediction_sessions()
        db.close()
        
        # Convert DataFrame to list of dicts
        if sessions_df.empty:
            return jsonify([])
        
        sessions_list = sessions_df.to_dict('records')
        
        # Convert datetime objects to strings
        for session in sessions_list:
            if 'created_at' in session and session['created_at']:
                if hasattr(session['created_at'], 'isoformat'):
                    session['created_at'] = session['created_at'].isoformat()
                else:
                    session['created_at'] = str(session['created_at'])
            if 'prediction_start_date' in session and session['prediction_start_date']:
                if hasattr(session['prediction_start_date'], 'isoformat'):
                    session['prediction_start_date'] = session['prediction_start_date'].isoformat()
                else:
                    session['prediction_start_date'] = str(session['prediction_start_date'])
            if 'prediction_end_date' in session and session['prediction_end_date']:
                if hasattr(session['prediction_end_date'], 'isoformat'):
                    session['prediction_end_date'] = session['prediction_end_date'].isoformat()
                else:
                    session['prediction_end_date'] = str(session['prediction_end_date'])
        
        return jsonify(sessions_list)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/predictions/session/<session_id>')
def get_prediction_session(session_id):
    """Get predictions for a specific session"""
    try:
        db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
        predictions_df = db.get_predictions(session_id=int(session_id))
        db.close()
        
        if predictions_df.empty:
            return jsonify({'error': 'Session not found'}), 404
        
        # Convert DataFrame to list of dicts
        predictions_list = predictions_df.to_dict('records')
        
        # Convert datetime objects to strings
        for pred in predictions_list:
            if 'prediction_date' in pred and pred['prediction_date']:
                if hasattr(pred['prediction_date'], 'isoformat'):
                    pred['prediction_date'] = pred['prediction_date'].isoformat()
                else:
                    pred['prediction_date'] = str(pred['prediction_date'])
        
        return jsonify(predictions_list)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/database/stats')
def get_database_stats():
    """Get database statistics"""
    try:
        db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
        stats = db.get_statistics()
        db.close()
        
        return jsonify(stats)
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    emit('connected', {'message': 'Connected to Unified Crawler'})
    # Send current logs
    for log in crawling_state['redbus']['logs']:
        emit('log_update', log)
    for log in training_state['logs']:
        emit('log_update', log)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    # Register API V2 endpoints
    register_api_v2_routes(
        app=app,
        crawling_state=crawling_state,
        task_generator=task_generator,
        crawl_threads=crawl_threads,
        redbus_worker=redbus_worker,
        log_message=log_message,
        db_config=db_config
    )
    
    # Get configuration
    host = config.get('server', {}).get('host', '0.0.0.0')
    port = config.get('server', {}).get('unified_port', 5002)
    debug = config.get('server', {}).get('debug', False)
    
    print(f"Starting Unified Crawler on {host}:{port}")
    print(f"Debug mode: {debug}")
    print(f"API V2 endpoints registered:")
    print(f"  - POST /api/v2/crawl/start")
    print(f"  - POST /api/v2/crawl/stop")
    print(f"  - GET  /api/v2/crawl/status")
    print(f"  - GET  /api/v2/data")
    print(f"  - GET  /api/v2/data/summary")
    print(f"  - GET  /api/v2/data/export")
    
    # Ensure data directories exist
    os.makedirs('new_data', exist_ok=True)
    os.makedirs('data/redbus', exist_ok=True)
    
    socketio.run(app, debug=debug, host=host, port=port)

