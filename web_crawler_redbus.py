from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import threading
import json
import os
import pandas as pd
from datetime import datetime
import time
from redbus import initialize_driver, get_bus_detail, routes, dates as redbus_dates
import glob
import queue
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret_redbus!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables for crawling state
crawling_state = {
    'is_running': False,
    'progress': 0,
    'total_tasks': 0,
    'current_tasks': [],  # List of currently running tasks
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

# Queue for managing crawl tasks
task_queue = queue.Queue()
active_workers = []
executor = None

def log_message(message, level='info'):
    """Add a log message and emit to all connected clients"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {
        'timestamp': timestamp,
        'level': level,
        'message': message
    }
    crawling_state['logs'].append(log_entry)
    # Keep only last 150 logs
    if len(crawling_state['logs']) > 150:
        crawling_state['logs'] = crawling_state['logs'][-150:]
    
    socketio.emit('log_update', log_entry)

def update_progress():
    """Update progress and emit to clients"""
    completed = crawling_state['completed_tasks']
    total = crawling_state['total_tasks']
    crawling_state['progress'] = int((completed / total) * 100) if total > 0 else 0
    
    socketio.emit('progress_update', {
        'progress': crawling_state['progress'],
        'completed': completed,
        'total': total,
        'current_tasks': crawling_state['current_tasks']
    })

def crawl_single_task(task_info, worker_id):
    """Worker function for a single crawl task"""
    route_name = task_info['route']
    url = task_info['url']
    date = task_info['date']
    task_name = f"{route_name} - Date: {date}"
    
    driver = None
    try:
        # Update current tasks
        crawling_state['current_tasks'].append({
            'worker_id': worker_id,
            'task': task_name
        })
        socketio.emit('task_start', {
            'worker_id': worker_id,
            'task': task_name
        })
        
        log_message(f'[Worker {worker_id}] Starting {task_name}...', 'info')
        
        # Initialize driver
        driver = initialize_driver()
        
        # Scrape data
        bus_details = get_bus_detail(driver, url, route_name, date)
        
        if bus_details:
            df = pd.DataFrame(bus_details)
            # Add full timestamp with date to filename
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'new_data/redbus_{route_name}-{date}_{timestamp}_worker{worker_id}.csv'
            df.to_csv(filename, index=False, encoding='utf-8')
            
            crawling_state['stats']['successful'] += 1
            crawling_state['stats']['total_scraped'] += len(bus_details)
            log_message(f'[Worker {worker_id}] ✓ Successfully scraped {len(bus_details)} buses for {task_name}', 'success')
        else:
            crawling_state['stats']['failed'] += 1
            log_message(f'[Worker {worker_id}] ⚠ No data found for {task_name}', 'warning')
        
        return {'success': True, 'worker_id': worker_id, 'task': task_name}
    
    except Exception as e:
        crawling_state['stats']['failed'] += 1
        log_message(f'[Worker {worker_id}] ✗ Error scraping {task_name}: {str(e)}', 'error')
        return {'success': False, 'worker_id': worker_id, 'task': task_name, 'error': str(e)}
    
    finally:
        if driver:
            try:
                driver.quit()
            except:
                pass
        
        # Remove from current tasks
        crawling_state['current_tasks'] = [
            t for t in crawling_state['current_tasks'] 
            if t['worker_id'] != worker_id
        ]
        
        # Update progress
        crawling_state['completed_tasks'] += 1
        update_progress()
        
        socketio.emit('task_complete', {
            'worker_id': worker_id,
            'task': task_name
        })

def crawl_worker(tasks, max_workers):
    """Main worker function that manages multiple concurrent crawlers"""
    global executor
    
    try:
        crawling_state['is_running'] = True
        crawling_state['stats']['start_time'] = datetime.now().isoformat()
        crawling_state['completed_tasks'] = 0
        crawling_state['current_tasks'] = []
        
        log_message(f'Starting crawling with {max_workers} concurrent workers...', 'info')
        log_message(f'Total tasks to process: {len(tasks)}', 'info')
        
        # Create thread pool
        executor = ThreadPoolExecutor(max_workers=max_workers)
        
        # Submit all tasks
        futures = []
        for idx, task in enumerate(tasks):
            worker_id = (idx % max_workers) + 1
            future = executor.submit(crawl_single_task, task, worker_id)
            futures.append(future)
            time.sleep(0.5)  # Small delay between task submissions
        
        # Wait for all tasks to complete
        for future in as_completed(futures):
            try:
                result = future.result()
                if result['success']:
                    log_message(f"[Worker {result['worker_id']}] Task completed: {result['task']}", 'success')
            except Exception as e:
                log_message(f"Task failed with exception: {str(e)}", 'error')
        
        log_message('All crawling tasks completed!', 'success')
        
    except Exception as e:
        log_message(f'Fatal error in crawling process: {str(e)}', 'error')
    
    finally:
        if executor:
            executor.shutdown(wait=True)
        
        crawling_state['is_running'] = False
        crawling_state['stats']['end_time'] = datetime.now().isoformat()
        crawling_state['current_tasks'] = []
        update_progress()

@app.route('/')
def index():
    """Main page"""
    return render_template('redbus.html')

@app.route('/api/status')
def get_status():
    """Get current crawling status"""
    return jsonify(crawling_state)

@app.route('/api/routes')
def get_routes():
    """Get available routes and dates"""
    route_info = {
        'routes': list(routes.keys()),
        'dates': redbus_dates
    }
    return jsonify(route_info)

@app.route('/api/start', methods=['POST'])
def start_crawling():
    """Start the crawling process"""
    global crawl_thread
    
    if crawling_state['is_running']:
        return jsonify({'error': 'Crawling is already running'}), 400
    
    data = request.json
    selected_routes = data.get('routes', [])
    selected_dates = data.get('dates', [])
    max_workers = data.get('max_workers', 3)  # Default 3 concurrent workers
    runs_per_task = data.get('runs_per_task', 1)  # How many times to run each route-date combo
    
    if not selected_routes or not selected_dates:
        return jsonify({'error': 'Please select at least one route and one date'}), 400
    
    # Reset stats
    crawling_state['stats'] = {
        'total_scraped': 0,
        'successful': 0,
        'failed': 0,
        'start_time': None,
        'end_time': None
    }
    crawling_state['logs'] = []
    crawling_state['progress'] = 0
    crawling_state['completed_tasks'] = 0
    
    # Create new_data directory if it doesn't exist
    os.makedirs('new_data', exist_ok=True)
    
    # Build task list
    tasks = []
    for route_name in selected_routes:
        if route_name in routes:
            url = routes[route_name]
            for date in selected_dates:
                # Convert day-only format to full date if needed
                if date.isdigit() and len(date) <= 2:
                    date_full = f"2025-12-{date.zfill(2)}"
                else:
                    date_full = date
                
                # Repeat task based on runs_per_task
                for run in range(runs_per_task):
                    tasks.append({
                        'route': route_name,
                        'url': url,
                        'date': date_full,
                        'run': run + 1
                    })
    
    crawling_state['total_tasks'] = len(tasks)
    
    log_message(f'Configured {len(tasks)} tasks with {max_workers} concurrent workers', 'info')
    
    # Start crawling in a new thread
    crawl_thread = threading.Thread(
        target=crawl_worker,
        args=(tasks, max_workers)
    )
    crawl_thread.start()
    
    return jsonify({'message': 'Crawling started successfully', 'total_tasks': len(tasks)})

@app.route('/api/stop', methods=['POST'])
def stop_crawling():
    """Stop the crawling process"""
    global executor
    
    if not crawling_state['is_running']:
        return jsonify({'error': 'No crawling process is running'}), 400
    
    crawling_state['is_running'] = False
    log_message('Stop requested by user - waiting for current tasks to complete...', 'warning')
    
    # Shutdown executor
    if executor:
        executor.shutdown(wait=False)
    
    return jsonify({'message': 'Crawling will stop after current tasks complete'})

@app.route('/api/data')
def get_scraped_data():
    """Get list of scraped data files"""
    data_files = []
    
    if os.path.exists('new_data'):
        for filename in glob.glob('new_data/redbus_*.csv'):
            try:
                df = pd.read_csv(filename)
                file_stat = os.stat(filename)
                data_files.append({
                    'filename': os.path.basename(filename),
                    'path': filename,
                    'size': file_stat.st_size,
                    'rows': len(df),
                    'modified': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
                })
            except Exception as e:
                print(f"Error reading {filename}: {e}")
    
    # Sort by modified time (newest first)
    data_files.sort(key=lambda x: x['modified'], reverse=True)
    return jsonify(data_files)

@app.route('/api/data/<filename>')
def get_file_data(filename):
    """Get preview of a specific data file"""
    filepath = os.path.join('new_data', filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        df = pd.read_csv(filepath)
        
        # Calculate statistics
        stats = {
            'unique_buses': df['Bus_Name'].nunique() if 'Bus_Name' in df.columns else 0,
            'unique_types': df['Bus_Type'].nunique() if 'Bus_Type' in df.columns else 0,
        }
        
        if 'Price' in df.columns:
            df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
            stats['avg_price'] = float(df['Price'].mean())
            stats['min_price'] = float(df['Price'].min())
            stats['max_price'] = float(df['Price'].max())
        else:
            stats['avg_price'] = 0
            stats['min_price'] = 0
            stats['max_price'] = 0
        
        return jsonify({
            'filename': filename,
            'rows': len(df),
            'columns': df.columns.tolist(),
            'preview': df.head(10).to_dict('records'),
            'stats': stats
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/download/<filename>')
def download_file(filename):
    """Download a data file"""
    filepath = os.path.join('new_data', filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(filepath, as_attachment=True)

@app.route('/api/stats')
def get_statistics():
    """Get overall statistics"""
    stats = {
        'total_files': 0,
        'total_records': 0,
        'total_size': 0,
        'routes': {},
        'date_range': {'min': None, 'max': None}
    }
    
    if os.path.exists('new_data'):
        for filename in glob.glob('new_data/redbus_*.csv'):
            try:
                df = pd.read_csv(filename)
                file_stat = os.stat(filename)
                
                stats['total_files'] += 1
                stats['total_records'] += len(df)
                stats['total_size'] += file_stat.st_size
                
                if 'Route_Name' in df.columns:
                    route_name = df['Route_Name'].iloc[0] if len(df) > 0 else 'Unknown'
                    if route_name not in stats['routes']:
                        stats['routes'][route_name] = 0
                    stats['routes'][route_name] += len(df)
                
            except Exception as e:
                print(f"Error processing {filename}: {e}")
    
    return jsonify(stats)

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    emit('connected', {'message': 'Connected to Redbus Crawler'})
    # Send current logs to newly connected client
    for log in crawling_state['logs']:
        emit('log_update', log)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5001)
