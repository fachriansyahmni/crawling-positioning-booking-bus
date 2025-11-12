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
import joblib
from database import BusDatabase

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

# Global variables for training state
training_state = {
    'is_running': False,
    'progress': 0,
    'current_step': '',
    'logs': [],
    'results': None
}

crawl_thread = None
train_thread = None
driver = None

def log_message(message, level='info', category='crawler'):
    """Add a log message and emit to all connected clients"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_entry = {
        'timestamp': timestamp,
        'level': level,
        'message': message,
        'category': category
    }
    
    if category == 'crawler':
        crawling_state['logs'].append(log_entry)
        if len(crawling_state['logs']) > 100:
            crawling_state['logs'] = crawling_state['logs'][-100:]
    else:
        training_state['logs'].append(log_entry)
        if len(training_state['logs']) > 100:
            training_state['logs'] = training_state['logs'][-100:]
    
    socketio.emit('log_update', log_entry)

def update_progress(current, total, task_name, category='crawler'):
    """Update progress and emit to clients"""
    progress = int((current / total) * 100) if total > 0 else 0
    
    if category == 'crawler':
        crawling_state['progress'] = progress
        crawling_state['current_task'] = task_name
        crawling_state['total_tasks'] = total
    else:
        training_state['progress'] = progress
        training_state['current_step'] = task_name
    
    socketio.emit('progress_update', {
        'progress': progress,
        'current': current,
        'total': total,
        'task': task_name,
        'category': category
    })

def crawl_worker(selected_routes, selected_dates_indices):
    """Worker function for crawling in a separate thread"""
    global driver, crawling_state
    
    try:
        crawling_state['is_running'] = True
        crawling_state['stats']['start_time'] = datetime.now().isoformat()
        log_message('Starting crawling process...', 'info', 'crawler')
        
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
        log_message('Initializing Chrome driver...', 'info', 'crawler')
        driver = initialize_driver()
        
        # Iterate through routes and dates
        for route_idx in selected_routes:
            route_set = routes[route_idx]
            date_list = dates[route_idx]
            
            for route_name, url in route_set.items():
                if route_name not in selected_routes[route_idx]:
                    continue
                    
                for date in date_list:
                    if date.isdigit() and len(date) <= 2:
                        date_full = f"2025-12-{date.zfill(2)}"
                    else:
                        date_full = date
                    
                    if date not in selected_dates_indices[route_idx]:
                        continue
                
                    if not crawling_state['is_running']:
                        log_message('Crawling stopped by user', 'warning', 'crawler')
                        break
                    
                    current_task += 1
                    task_name = f"{route_name} - Date: {date_full}"
                    update_progress(current_task, total_tasks, task_name, 'crawler')
                    log_message(f'Scraping {task_name}...', 'info', 'crawler')
                    
                    try:
                        bus_details = get_bus_detail(driver, url, route_name, date_full)
                        
                        if bus_details:
                            df = pd.DataFrame(bus_details)
                            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                            filename = f'new_data/traveloka_{route_name}-{date_full}_{timestamp}.csv'
                            df.to_csv(filename, index=False, encoding='utf-8')
                            
                            crawling_state['stats']['successful'] += 1
                            crawling_state['stats']['total_scraped'] += len(bus_details)
                            log_message(f'Successfully scraped {len(bus_details)} buses for {task_name}', 'success', 'crawler')
                        else:
                            crawling_state['stats']['failed'] += 1
                            log_message(f'No data found for {task_name}', 'warning', 'crawler')
                    
                    except Exception as e:
                        crawling_state['stats']['failed'] += 1
                        log_message(f'Error scraping {task_name}: {str(e)}', 'error', 'crawler')
                    
                    time.sleep(1)
        
        log_message('Crawling process completed!', 'success', 'crawler')
        
    except Exception as e:
        log_message(f'Fatal error in crawling process: {str(e)}', 'error', 'crawler')
    
    finally:
        if driver:
            driver.quit()
            log_message('Chrome driver closed', 'info', 'crawler')
        
        crawling_state['is_running'] = False
        crawling_state['stats']['end_time'] = datetime.now().isoformat()
        update_progress(100, 100, 'Completed', 'crawler')

def train_model_worker(days_back=90):
    """Worker function for model training"""
    global training_state
    
    try:
        training_state['is_running'] = True
        training_state['logs'] = []
        training_state['results'] = None
        
        log_message('Starting model training...', 'info', 'training')
        update_progress(10, 100, 'Connecting to database', 'training')
        
        # Import training module
        from train_bus_prediction_model import BusPredictionModel
        from database import BusDatabase, load_db_config
        
        # Connect to database
        log_message('Connecting to database...', 'info', 'training')
        db_config = load_db_config()
        db = BusDatabase(db_type=db_config['type'], db_config=db_config)
        
        update_progress(20, 100, f'Loading data (last {days_back} days)', 'training')
        
        # Initialize model
        model = BusPredictionModel()
        
        # Load data
        log_message(f'Loading data from database (last {days_back} days)...', 'info', 'training')
        df_raw = model.load_data_from_database(db, days_back=days_back)
        
        if df_raw.empty:
            log_message('No data available for training!', 'error', 'training')
            training_state['is_running'] = False
            return
        
        log_message(f'Loaded {len(df_raw)} records', 'success', 'training')
        update_progress(40, 100, 'Preparing features', 'training')
        
        # Prepare features
        log_message('Preparing features...', 'info', 'training')
        df_features = model.prepare_features(df_raw)
        
        # Aggregate daily data
        log_message('Aggregating daily data...', 'info', 'training')
        df_agg = model.aggregate_daily_data(df_features)
        log_message(f'Aggregated to {len(df_agg)} daily records', 'success', 'training')
        
        update_progress(60, 100, 'Creating training features', 'training')
        
        # Create training features
        log_message('Creating feature matrix...', 'info', 'training')
        X, y, df_with_features = model.create_features_for_training(df_agg)
        
        if X is None:
            log_message('Failed to create feature matrix!', 'error', 'training')
            training_state['is_running'] = False
            db.close()
            return
        
        update_progress(70, 100, 'Training models', 'training')
        
        # Train models
        log_message('Training Random Forest models...', 'info', 'training')
        training_results = model.train_models(X, y)
        
        update_progress(90, 100, 'Saving model', 'training')
        
        # Save model
        log_message('Saving model...', 'info', 'training')
        model.save_model('models/bus_prediction_model.pkl')
        
        # Prepare results
        results = {
            'training_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'days_back': days_back,
            'total_records': len(df_raw),
            'daily_records': len(df_agg),
            'features_count': X.shape[1] if X is not None else 0,
            'models': {}
        }
        
        for target, metrics in training_results.items():
            results['models'][target] = {
                'mae': float(metrics['mae']),
                'rmse': float(metrics['rmse']),
                'r2': float(metrics['r2']),
                'n_samples': int(metrics['n_samples'])
            }
        
        training_state['results'] = results
        
        log_message('Model training completed successfully!', 'success', 'training')
        log_message(f'Total buses - MAE: {results["models"]["total"]["mae"]:.4f}, R²: {results["models"]["total"]["r2"]:.4f}', 'info', 'training')
        log_message(f'VIP buses - MAE: {results["models"]["vip"]["mae"]:.4f}, R²: {results["models"]["vip"]["r2"]:.4f}', 'info', 'training')
        log_message(f'Executive buses - MAE: {results["models"]["executive"]["mae"]:.4f}, R²: {results["models"]["executive"]["r2"]:.4f}', 'info', 'training')
        
        update_progress(100, 100, 'Training completed', 'training')
        
        db.close()
        
    except Exception as e:
        log_message(f'Fatal error in training: {str(e)}', 'error', 'training')
        import traceback
        log_message(traceback.format_exc(), 'error', 'training')
    
    finally:
        training_state['is_running'] = False

# ===== ROUTES =====

@app.route('/')
def index():
    """Main page"""
    return render_template('ml_ui.html')

@app.route('/api/status')
def get_status():
    """Get current crawling status"""
    return jsonify(crawling_state)

@app.route('/api/training/status')
def get_training_status():
    """Get current training status"""
    return jsonify(training_state)

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
    
    os.makedirs('new_data', exist_ok=True)
    
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
    log_message('Stop requested by user', 'warning', 'crawler')
    
    return jsonify({'message': 'Crawling will stop after current task'})

@app.route('/api/train/start', methods=['POST'])
def start_training():
    """Start model training"""
    global train_thread
    
    if training_state['is_running']:
        return jsonify({'error': 'Training is already running'}), 400
    
    data = request.json
    days_back = data.get('days_back', 90)
    
    # Validate days_back
    if not isinstance(days_back, int) or days_back < 1:
        return jsonify({'error': 'Invalid days_back parameter'}), 400
    
    os.makedirs('models', exist_ok=True)
    
    train_thread = threading.Thread(
        target=train_model_worker,
        args=(days_back,)
    )
    train_thread.start()
    
    return jsonify({'message': 'Training started successfully'})

@app.route('/api/train/results')
def get_training_results():
    """Get training results"""
    if training_state['results']:
        return jsonify(training_state['results'])
    
    # Try to load from saved model
    model_path = 'models/bus_prediction_model.pkl'
    if os.path.exists(model_path):
        try:
            model_data = joblib.load(model_path)
            if 'training_stats' in model_data:
                results = {
                    'training_date': model_data.get('training_date', 'Unknown'),
                    'days_back': model_data.get('training_days', 'Unknown'),
                    'models': {}
                }
                
                for target, metrics in model_data['training_stats'].items():
                    results['models'][target] = {
                        'mae': float(metrics.get('mae', 0)),
                        'rmse': float(metrics.get('rmse', 0)),
                        'r2': float(metrics.get('r2', 0)),
                        'n_samples': int(metrics.get('n_samples', 0))
                    }
                
                return jsonify(results)
        except Exception as e:
            return jsonify({'error': f'Error loading model: {str(e)}'}), 500
    
    return jsonify({'message': 'No training results available'}), 404

@app.route('/api/predict', methods=['POST'])
def make_prediction():
    """Make predictions using trained model"""
    try:
        data = request.json
        days = data.get('days', 7)
        route = data.get('route', None)
        start_date = data.get('start_date', None)  # NEW: Support for date range
        end_date = data.get('end_date', None)      # NEW: Support for date range
        
        # Check if model exists
        model_path = 'models/bus_prediction_model.pkl'
        if not os.path.exists(model_path):
            return jsonify({'error': 'No trained model found. Please train a model first.'}), 404
        
        # Import prediction module
        from predict_bus_availability import BusPredictor
        from database import load_db_config
        
        # Create predictor
        db_config = load_db_config()
        predictor = BusPredictor(db_config=db_config)
        
        # Make predictions based on input type
        if start_date and end_date:
            # Date range prediction (route dates)
            predictions_df = predictor.predict_date_range(start_date, end_date, route_filter=route)
        else:
            # Custom days prediction (from tomorrow)
            predictions_df = predictor.predict_custom_days(days=days, route_filter=route)
        
        # Save to database
        period_name = f"{days}_days" if not (start_date and end_date) else "custom_range"
        session_id = predictor.save_predictions(predictions_df, period=period_name)
        
        # Convert to JSON-serializable format
        result = {
            'session_id': session_id,
            'total_predictions': len(predictions_df),
            'predictions': predictions_df.to_dict('records')
        }
        
        return jsonify(result)
        
    except Exception as e:
        import traceback
        return jsonify({'error': f'Prediction error: {str(e)}', 'traceback': traceback.format_exc()}), 500

@app.route('/api/predictions/history')
def get_prediction_history():
    """Get prediction history"""
    try:
        from database import load_db_config
        db_config = load_db_config()
        db = BusDatabase(db_type=db_config['type'], db_config=db_config)
        sessions = db.get_prediction_sessions(limit=20)
        db.close()
        
        # Convert DataFrame to dict if needed
        if hasattr(sessions, 'to_dict'):
            sessions = sessions.to_dict('records')
        
        return jsonify(sessions)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/predictions/<int:session_id>')
def get_prediction_details(session_id):
    """Get details of a specific prediction session"""
    try:
        from database import load_db_config
        db_config = load_db_config()
        db = BusDatabase(db_type=db_config['type'], db_config=db_config)
        predictions = db.get_predictions(session_id=session_id)
        db.close()
        
        # Convert DataFrame to dict if needed
        if hasattr(predictions, 'to_dict'):
            predictions = predictions.to_dict('records')
        
        return jsonify(predictions)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

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

@app.route('/api/stats')
def get_statistics():
    """Get overall statistics"""
    try:
        db = BusDatabase()
        
        # Get database stats
        db.cursor.execute("SELECT COUNT(*) FROM bus_data")
        total_records = db.cursor.fetchone()[0]
        
        db.cursor.execute("SELECT COUNT(DISTINCT platform) FROM bus_data")
        platforms_count = db.cursor.fetchone()[0]
        
        db.cursor.execute("SELECT COUNT(DISTINCT bus_name) FROM bus_data")
        companies_count = db.cursor.fetchone()[0]
        
        db.cursor.execute("SELECT COUNT(*) FROM prediction_sessions")
        prediction_sessions = db.cursor.fetchone()[0]
        
        db.close()
        
        stats = {
            'total_records': total_records,
            'platforms_count': platforms_count,
            'companies_count': companies_count,
            'prediction_sessions': prediction_sessions
        }
        
        return jsonify(stats)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    emit('connected', {'message': 'Connected to server'})
    # Send current logs
    for log in crawling_state['logs']:
        emit('log_update', log)
    for log in training_state['logs']:
        emit('log_update', log)

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection"""
    print('Client disconnected')

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
