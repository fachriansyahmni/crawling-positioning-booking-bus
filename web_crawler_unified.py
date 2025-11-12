from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import threading
import json
import os
import pandas as pd
from datetime import datetime
import time
import glob
import queue

# Import crawler functions
from traveloka import initialize_driver as init_traveloka_driver, get_bus_detail as get_traveloka_data
from traveloka import routes as traveloka_routes, dates as traveloka_dates
from redbus import initialize_driver as init_redbus_driver, get_bus_detail as get_redbus_data
from redbus import routes as redbus_routes, dates as redbus_dates

# Import database module
from database import BusDatabase

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

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'unified_crawler_secret!')
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global variables for crawling state
crawling_state = {
    'traveloka': {
        'is_running': False,
        'progress': 0,
        'total_tasks': 0,
        'current_task': '',
        'completed_tasks': 0,
        'logs': [],
        'stats': {
            'total_scraped': 0,
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None
        }
    },
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
crawl_threads = {'traveloka': None, 'redbus': None}
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

# Traveloka crawler worker
def traveloka_worker(tasks):
    """Worker function for Traveloka crawling"""
    driver = None
    db = None
    
    try:
        crawling_state['traveloka']['is_running'] = True
        crawling_state['traveloka']['stats']['start_time'] = datetime.now().isoformat()
        crawling_state['traveloka']['completed_tasks'] = 0
        
        log_message('traveloka', 'Starting Traveloka crawling...', 'info')
        log_message('traveloka', f'Total tasks: {len(tasks)}', 'info')
        
        # Initialize database connection
        try:
            db = BusDatabase(db_type=db_config.get('type', 'sqlite'), db_config=db_config)
            log_message('traveloka', f'✓ Connected to {db_config.get("type", "sqlite").upper()} database', 'success')
        except Exception as db_error:
            log_message('traveloka', f'⚠ Database connection failed: {str(db_error)}', 'warning')
            log_message('traveloka', 'Continuing without database insertion', 'warning')
            db = None
        
        driver = init_traveloka_driver()
        
        for idx, task in enumerate(tasks):
            if not crawling_state['traveloka']['is_running']:
                log_message('traveloka', 'Stopped by user', 'warning')
                break
            
            route_idx = task['route_idx']
            route_name = task['route']
            url = task['url']
            date = task['date']
            
            task_name = f"{route_name} - Date: {date}"
            crawling_state['traveloka']['current_task'] = task_name
            update_progress('traveloka')
            
            log_message('traveloka', f'Scraping {task_name}...', 'info')
            
            try:
                bus_details = get_traveloka_data(driver, url, route_name, date)
                
                if bus_details:
                    # Save to CSV file
                    df = pd.DataFrame(bus_details)
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    filename = f'new_data/traveloka_{route_name}-{date}_{timestamp}.csv'
                    df.to_csv(filename, index=False, encoding='utf-8')
                    
                    # Insert to database
                    if db:
                        try:
                            db_stats = db.insert_bulk_data(bus_details, platform='traveloka')
                            log_message('traveloka', 
                                      f'💾 Database: {db_stats["inserted"]} records inserted', 
                                      'info')
                        except Exception as db_error:
                            log_message('traveloka', f'⚠ Database insert error: {str(db_error)}', 'warning')
                    
                    crawling_state['traveloka']['stats']['successful'] += 1
                    crawling_state['traveloka']['stats']['total_scraped'] += len(bus_details)
                    log_message('traveloka', f'✓ Scraped {len(bus_details)} buses for {task_name}', 'success')
                else:
                    crawling_state['traveloka']['stats']['failed'] += 1
                    log_message('traveloka', f'⚠ No data for {task_name}', 'warning')
            
            except Exception as e:
                crawling_state['traveloka']['stats']['failed'] += 1
                log_message('traveloka', f'✗ Error: {str(e)}', 'error')
            
            crawling_state['traveloka']['completed_tasks'] += 1
            update_progress('traveloka')
            time.sleep(1)
        
        log_message('traveloka', 'Crawling completed!', 'success')
        
    except Exception as e:
        log_message('traveloka', f'Fatal error: {str(e)}', 'error')
    
    finally:
        if driver:
            try:
                driver.quit()
                log_message('traveloka', 'Browser closed', 'info')
            except:
                pass
        
        if db:
            try:
                db.close()
                log_message('traveloka', 'Database connection closed', 'info')
            except:
                pass
        
        crawling_state['traveloka']['is_running'] = False
        crawling_state['traveloka']['stats']['end_time'] = datetime.now().isoformat()
        update_progress('traveloka')

# Redbus normal sequential crawling
def redbus_worker(tasks):
    """Sequential worker function for Redbus crawling (no multi-worker)"""
    driver = None
    db = None
    
    try:
        crawling_state['redbus']['is_running'] = True
        crawling_state['redbus']['stats']['start_time'] = datetime.now().isoformat()
        crawling_state['redbus']['completed_tasks'] = 0
        crawling_state['redbus']['current_tasks'] = []
        
        log_message('redbus', f'Starting sequential crawling...', 'info')
        log_message('redbus', f'Total tasks: {len(tasks)}', 'info')
        
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
            
            route_name = task_info['route']
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
                
                bus_details = get_redbus_data(driver, url, route_name, date)
                
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
    """Get routes for specific platform"""
    if platform == 'traveloka':
        route_info = []
        for idx, route_set in enumerate(traveloka_routes):
            route_info.append({
                'index': idx,
                'routes': list(route_set.keys()),
                'dates': traveloka_dates[idx]
            })
        return jsonify(route_info)
    elif platform == 'redbus':
        return jsonify({
            'routes': list(redbus_routes.keys()),
            'dates': redbus_dates
        })
    else:
        return jsonify({'error': 'Invalid platform'}), 400

@app.route('/api/start/<platform>', methods=['POST'])
def start_crawling(platform):
    """Start crawling for specific platform"""
    if platform not in ['traveloka', 'redbus']:
        return jsonify({'error': 'Invalid platform'}), 400
    
    if crawling_state[platform]['is_running']:
        return jsonify({'error': f'{platform.capitalize()} crawler already running'}), 400
    
    data = request.json
    
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
    
    if platform == 'traveloka':
        selected_routes = data.get('routes', {})
        selected_dates = data.get('dates', {})
        
        tasks = []
        for route_idx_str in selected_routes:
            route_idx = int(route_idx_str)
            route_set = traveloka_routes[route_idx]
            date_list = traveloka_dates[route_idx]
            
            for route_name in selected_routes[route_idx_str]:
                if route_name in route_set:
                    url = route_set[route_name]
                    for date in selected_dates.get(route_idx_str, []):
                        # Convert day-only format to full date if needed
                        if date.isdigit() and len(date) <= 2:
                            # Convert to full date format (December 2025 for backward compatibility)
                            date_full = f"2025-12-{date.zfill(2)}"
                        else:
                            # Already in YYYY-MM-DD format
                            date_full = date
                        
                        if date in date_list or date_full.split('-')[2] in date_list:
                            tasks.append({
                                'route_idx': route_idx,
                                'route': route_name,
                                'url': url,
                                'date': date_full
                            })
        
        crawling_state['traveloka']['total_tasks'] = len(tasks)
        
        thread = threading.Thread(target=traveloka_worker, args=(tasks,))
        thread.start()
        crawl_threads['traveloka'] = thread
        
        return jsonify({'message': 'Traveloka crawling started', 'total_tasks': len(tasks)})
    
    elif platform == 'redbus':
        selected_routes = data.get('routes', [])
        selected_dates = data.get('dates', [])
        
        tasks = []
        for route_name in selected_routes:
            if route_name in redbus_routes:
                url = redbus_routes[route_name]
                for date in selected_dates:
                    # Convert day-only format to full date if needed
                    # Check if date is just a number (legacy format)
                    if date.isdigit() and len(date) <= 2:
                        # Convert to full date format (December 2025 for backward compatibility)
                        date_full = f"2025-12-{date.zfill(2)}"
                    else:
                        # Already in YYYY-MM-DD format
                        date_full = date
                    
                    tasks.append({
                        'route': route_name,
                        'url': url,
                        'date': date_full
                    })
        
        crawling_state['redbus']['total_tasks'] = len(tasks)
        
        thread = threading.Thread(target=redbus_worker, args=(tasks,))
        thread.start()
        crawl_threads['redbus'] = thread
        
        return jsonify({'message': 'Redbus crawling started', 'total_tasks': len(tasks)})

@app.route('/api/stop/<platform>', methods=['POST'])
def stop_crawling(platform):
    """Stop crawling for specific platform"""
    if platform not in ['traveloka', 'redbus']:
        return jsonify({'error': 'Invalid platform'}), 400
    
    if not crawling_state[platform]['is_running']:
        return jsonify({'error': f'{platform.capitalize()} crawler not running'}), 400
    
    crawling_state[platform]['is_running'] = False
    log_message(platform, 'Stop requested by user', 'warning')
    
    return jsonify({'message': f'{platform.capitalize()} will stop after current task'})

@app.route('/api/data/<platform>')
def get_scraped_data(platform):
    """Get list of scraped data files for platform"""
    if platform not in ['traveloka', 'redbus', 'all']:
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

@app.route('/api/compare')
def compare_data():
    """Compare data from both platforms"""
    try:
        # Get all files
        traveloka_files = glob.glob('new_data/traveloka_*.csv')
        redbus_files = glob.glob('new_data/redbus_*.csv')
        
        comparison = {
            'traveloka': {
                'total_files': len(traveloka_files),
                'total_records': 0,
                'routes': {},
                'avg_price': 0,
                'date_coverage': set()
            },
            'redbus': {
                'total_files': len(redbus_files),
                'total_records': 0,
                'routes': {},
                'avg_price': 0,
                'date_coverage': set()
            },
            'comparison': []
        }
        
        # Process Traveloka files
        all_prices_t = []
        for file in traveloka_files:
            df = pd.read_csv(file)
            comparison['traveloka']['total_records'] += len(df)
            
            if 'Route_Name' in df.columns and len(df) > 0:
                route = df['Route_Name'].iloc[0]
                comparison['traveloka']['routes'][route] = \
                    comparison['traveloka']['routes'].get(route, 0) + len(df)
            
            if 'Route_Date' in df.columns:
                comparison['traveloka']['date_coverage'].update(df['Route_Date'].unique())
            
            if 'Price' in df.columns:
                prices = pd.to_numeric(df['Price'], errors='coerce').dropna()
                all_prices_t.extend(prices.tolist())
        
        if all_prices_t:
            comparison['traveloka']['avg_price'] = sum(all_prices_t) / len(all_prices_t)
        
        # Process Redbus files
        all_prices_r = []
        for file in redbus_files:
            df = pd.read_csv(file)
            comparison['redbus']['total_records'] += len(df)
            
            if 'Route_Name' in df.columns and len(df) > 0:
                route = df['Route_Name'].iloc[0]
                comparison['redbus']['routes'][route] = \
                    comparison['redbus']['routes'].get(route, 0) + len(df)
            
            if 'Route_Date' in df.columns:
                comparison['redbus']['date_coverage'].update(df['Route_Date'].unique())
            
            if 'Price' in df.columns:
                prices = pd.to_numeric(df['Price'], errors='coerce').dropna()
                all_prices_r.extend(prices.tolist())
        
        if all_prices_r:
            comparison['redbus']['avg_price'] = sum(all_prices_r) / len(all_prices_r)
        
        # Convert sets to lists for JSON
        comparison['traveloka']['date_coverage'] = sorted(list(comparison['traveloka']['date_coverage']))
        comparison['redbus']['date_coverage'] = sorted(list(comparison['redbus']['date_coverage']))
        
        # Route-by-route comparison
        all_routes = set(comparison['traveloka']['routes'].keys()) | \
                     set(comparison['redbus']['routes'].keys())
        
        for route in all_routes:
            comparison['comparison'].append({
                'route': route,
                'traveloka_records': comparison['traveloka']['routes'].get(route, 0),
                'redbus_records': comparison['redbus']['routes'].get(route, 0)
            })
        
        return jsonify(comparison)
    
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
        
        log_message('training', f'Generating predictions for next {days} days...', 'info')
        if route_filter:
            log_message('training', f'Route filter: {route_filter}', 'info')
        
        # Import prediction module
        from predict_bus_availability import BusPredictor
        
        # Initialize predictor
        predictor = BusPredictor(db_config=db_config)
        
        # Generate predictions with custom days
        predictions_df = predictor.predict_custom_days(days=days, route_filter=route_filter)
        
        if predictions_df.empty:
            log_message('training', '⚠ No predictions generated', 'warning')
            return jsonify({'error': 'No predictions generated'}), 500
        
        # Determine period name
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
    for log in crawling_state['traveloka']['logs']:
        emit('log_update', log)
    for log in crawling_state['redbus']['logs']:
        emit('log_update', log)
    for log in training_state['logs']:
        emit('log_update', log)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    # Get configuration
    host = config.get('server', {}).get('host', '0.0.0.0')
    port = config.get('server', {}).get('unified_port', 5002)
    debug = config.get('server', {}).get('debug', False)
    
    print(f"Starting Unified Crawler on {host}:{port}")
    print(f"Debug mode: {debug}")
    
    # Ensure data directories exist
    os.makedirs('new_data', exist_ok=True)
    os.makedirs('data/traveloka', exist_ok=True)
    os.makedirs('data/redbus', exist_ok=True)
    
    socketio.run(app, debug=debug, host=host, port=port)

