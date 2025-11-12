from flask import Flask, render_template, request, jsonify, send_file
from flask_socketio import SocketIO, emit
import threading
import json
import os
import pandas as pd
from datetime import datetime
import time
from traveloka import initialize_driver, get_bus_detail, routes, dates
import glob

app = Flask(__name__)
app.config['SECRET_KEY'] = 'secret!'
socketio = SocketIO(app, cors_allowed_origins="*")

# Global variables for crawling state
crawling_state = {
    'is_running': False,
    'progress': 0,
    'total_tasks': 0,
    'current_task': '',
    'logs': [],
    'stats': {
        'total_scraped': 0,
        'successful': 0,
        'failed': 0,
        'start_time': None,
        'end_time': None
    }
}

crawl_thread = None
driver = None

def log_message(message, level='info'):
    """Add a log message and emit to all connected clients"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {
        'timestamp': timestamp,
        'level': level,
        'message': message
    }
    crawling_state['logs'].append(log_entry)
    # Keep only last 100 logs
    if len(crawling_state['logs']) > 100:
        crawling_state['logs'] = crawling_state['logs'][-100:]
    
    socketio.emit('log_update', log_entry)

def update_progress(current, total, task_name):
    """Update progress and emit to clients"""
    crawling_state['progress'] = int((current / total) * 100) if total > 0 else 0
    crawling_state['current_task'] = task_name
    crawling_state['total_tasks'] = total
    
    socketio.emit('progress_update', {
        'progress': crawling_state['progress'],
        'current': current,
        'total': total,
        'task': task_name
    })

def crawl_worker(selected_routes, selected_dates_indices):
    """Worker function for crawling in a separate thread"""
    global driver, crawling_state
    
    try:
        crawling_state['is_running'] = True
        crawling_state['stats']['start_time'] = datetime.now().isoformat()
        log_message('Starting crawling process...', 'info')
        
        # Calculate total tasks
        total_tasks = 0
        for route_idx in selected_routes:
            route_set = routes[route_idx]
            date_list = dates[route_idx]
            for route_name in route_set.keys():
                if route_name in selected_routes[route_idx]:
                    for date in date_list:
                        if date in selected_dates_indices[route_idx]:
                            total_tasks += 1
        
        current_task = 0
        
        # Initialize driver
        log_message('Initializing Chrome driver...', 'info')
        driver = initialize_driver()
        
        # Iterate through routes and dates
        for route_idx in selected_routes:
            route_set = routes[route_idx]
            date_list = dates[route_idx]
            
            for route_name, url in route_set.items():
                if route_name not in selected_routes[route_idx]:
                    continue
                    
                for date in date_list:
                    # Convert day-only format to full date if needed
                    if date.isdigit() and len(date) <= 2:
                        date_full = f"2025-12-{date.zfill(2)}"
                    else:
                        date_full = date
                    
                    if date not in selected_dates_indices[route_idx]:
                        continue
                
                    if not crawling_state['is_running']:
                        log_message('Crawling stopped by user', 'warning')
                        break
                    
                    current_task += 1
                    task_name = f"{route_name} - Date: {date_full}"
                    update_progress(current_task, total_tasks, task_name)
                    log_message(f'Scraping {task_name}...', 'info')
                    
                    try:
                        bus_details = get_bus_detail(driver, url, route_name, date_full)
                        
                        if bus_details:
                            df = pd.DataFrame(bus_details)
                            # Add timestamp to filename
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            filename = f'new_data/traveloka_{route_name}-{date_full}_{timestamp}.csv'
                            df.to_csv(filename, index=False, encoding='utf-8')
                            
                            crawling_state['stats']['successful'] += 1
                            crawling_state['stats']['total_scraped'] += len(bus_details)
                            log_message(f'Successfully scraped {len(bus_details)} buses for {task_name}', 'success')
                        else:
                            crawling_state['stats']['failed'] += 1
                            log_message(f'No data found for {task_name}', 'warning')
                    
                    except Exception as e:
                        crawling_state['stats']['failed'] += 1
                        log_message(f'Error scraping {task_name}: {str(e)}', 'error')
                    
                    time.sleep(1)  # Brief pause between requests
        
        log_message('Crawling process completed!', 'success')
        
    except Exception as e:
        log_message(f'Fatal error in crawling process: {str(e)}', 'error')
    
    finally:
        if driver:
            driver.quit()
            log_message('Chrome driver closed', 'info')
        
        crawling_state['is_running'] = False
        crawling_state['stats']['end_time'] = datetime.now().isoformat()
        update_progress(100, 100, 'Completed')

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    """Get current crawling status"""
    return jsonify(crawling_state)

@app.route('/api/routes')
def get_routes():
    """Get available routes and dates"""
    route_info = []
    for idx, route_set in enumerate(routes):
        route_info.append({
            'index': idx,
            'routes': list(route_set.keys()),
            'dates': dates[idx]
        })
    return jsonify(route_info)

@app.route('/api/start', methods=['POST'])
def start_crawling():
    """Start the crawling process"""
    global crawl_thread
    
    if crawling_state['is_running']:
        return jsonify({'error': 'Crawling is already running'}), 400
    
    data = request.json
    selected_routes = data.get('routes', {})
    selected_dates = data.get('dates', {})
    
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
    
    # Create new_data directory if it doesn't exist
    os.makedirs('new_data', exist_ok=True)
    
    # Start crawling in a new thread
    crawl_thread = threading.Thread(
        target=crawl_worker,
        args=(selected_routes, selected_dates)
    )
    crawl_thread.start()
    
    return jsonify({'message': 'Crawling started successfully'})

@app.route('/api/stop', methods=['POST'])
def stop_crawling():
    """Stop the crawling process"""
    if not crawling_state['is_running']:
        return jsonify({'error': 'No crawling process is running'}), 400
    
    crawling_state['is_running'] = False
    log_message('Stop requested by user', 'warning')
    
    return jsonify({'message': 'Crawling will stop after current task'})

@app.route('/api/data')
def get_scraped_data():
    """Get list of scraped data files"""
    data_files = []
    
    if os.path.exists('new_data'):
        for filename in glob.glob('new_data/*.csv'):
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
    
    return jsonify(data_files)

@app.route('/api/data/<filename>')
def get_file_data(filename):
    """Get preview of a specific data file"""
    filepath = os.path.join('new_data', filename)
    
    if not os.path.exists(filepath):
        return jsonify({'error': 'File not found'}), 404
    
    try:
        df = pd.read_csv(filepath)
        return jsonify({
            'filename': filename,
            'rows': len(df),
            'columns': df.columns.tolist(),
            'preview': df.head(10).to_dict('records'),
            'stats': {
                'unique_buses': df['Bus_Name'].nunique() if 'Bus_Name' in df.columns else 0,
                'avg_price': df['Price'].mean() if 'Price' in df.columns else 0,
                'min_price': df['Price'].min() if 'Price' in df.columns else 0,
                'max_price': df['Price'].max() if 'Price' in df.columns else 0,
            }
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
        for filename in glob.glob('new_data/*.csv'):
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
    emit('connected', {'message': 'Connected to server'})
    # Send current logs to newly connected client
    for log in crawling_state['logs']:
        emit('log_update', log)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
