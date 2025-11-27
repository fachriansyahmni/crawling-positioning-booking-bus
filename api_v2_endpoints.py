"""
Enhanced API V2 Endpoints for Crawling System
Provides advanced filtering, sorting, and data management
"""

from flask import request, jsonify, send_file
from datetime import datetime
import pandas as pd
import os
from database import BusDatabase


def register_api_v2_routes(app, crawling_state, task_generator, crawl_threads, redbus_worker, log_message, db_config):
    """Register all API V2 routes to the Flask app"""
    
    # ============ CRAWLING CONTROL API ============
    
    @app.route('/api/v2/crawl/start', methods=['POST'])
    def api_v2_start_crawling():
        """
        Start crawling with enhanced control
        
        Request body:
        {
            "platform": "redbus",
            "routes": ["Jakarta-Semarang", "Jakarta-Surabaya"],
            "dates": ["2025-11-28", "2025-11-29"],
            "max_buses": 50,  // Optional: limit buses per task
            "max_scroll": 5   // Optional: limit scroll iterations per task
        }
        
        Response:
        {
            "message": "redbus crawling started",
            "platform": "redbus",
            "total_tasks": 4,
            "routes": ["Jakarta-Semarang"],
            "dates": ["2025-11-28"],
            "max_buses": 50,
            "max_scroll": 5,
            "status": "started"
        }
        """
        try:
            data = request.json
            platform = data.get('platform', 'redbus')
            
            if platform not in ['redbus']:
                return jsonify({'error': f'Unsupported platform: {platform}'}), 400
            
            if crawling_state[platform]['is_running']:
                return jsonify({
                    'error': f'{platform} crawler is already running',
                    'status': 'running'
                }), 409
            
            routes = data.get('routes', [])
            dates = data.get('dates', [])
            max_buses = data.get('max_buses', None)
            max_scroll = data.get('max_scroll', None)
            
            # Validate max_buses
            if max_buses is not None:
                try:
                    max_buses = int(max_buses)
                    if max_buses <= 0:
                        max_buses = None
                except (ValueError, TypeError):
                    max_buses = None
            
            # Validate max_scroll
            if max_scroll is not None:
                try:
                    max_scroll = int(max_scroll)
                    if max_scroll <= 0:
                        max_scroll = None
                except (ValueError, TypeError):
                    max_scroll = None
            
            if not routes or not dates:
                return jsonify({'error': 'Routes and dates are required'}), 400
            
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
            crawling_state[platform]['max_buses'] = max_buses
            crawling_state[platform]['max_scroll'] = max_scroll
            
            os.makedirs('new_data', exist_ok=True)
            
            # Generate tasks
            tasks = task_generator.generate_redbus_tasks(routes, dates)
            
            if not tasks:
                return jsonify({'error': 'No valid tasks generated'}), 400
            
            crawling_state[platform]['total_tasks'] = len(tasks)
            
            # Start crawling thread
            import threading
            crawl_threads[platform] = threading.Thread(target=redbus_worker, args=(tasks, max_buses, max_scroll))
            crawl_threads[platform].start()
            
            response_data = {
                'message': f'{platform} crawling started',
                'platform': platform,
                'total_tasks': len(tasks),
                'routes': routes,
                'dates': dates,
                'status': 'started'
            }
            
            if max_buses:
                response_data['max_buses'] = max_buses
                response_data['message'] += f' (max {max_buses} buses per task)'
            
            if max_scroll:
                response_data['max_scroll'] = max_scroll
                response_data['message'] += f' (max {max_scroll} scrolls per task)'
            
            return jsonify(response_data), 200
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/v2/crawl/stop', methods=['POST'])
    def api_v2_stop_crawling():
        """
        Stop crawling
        
        Request body:
        {
            "platform": "redbus"
        }
        """
        try:
            data = request.json
            platform = data.get('platform', 'redbus')
            
            if platform not in ['redbus']:
                return jsonify({'error': f'Unsupported platform: {platform}'}), 400
            
            if not crawling_state[platform]['is_running']:
                return jsonify({
                    'error': f'{platform} crawler is not running',
                    'status': 'stopped'
                }), 400
            
            crawling_state[platform]['is_running'] = False
            log_message(platform, 'Stop requested via API', 'warning')
            
            return jsonify({
                'message': f'{platform} crawler will stop after current task',
                'platform': platform,
                'status': 'stopping'
            }), 200
            
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    @app.route('/api/v2/crawl/status', methods=['GET'])
    def api_v2_crawl_status():
        """
        Get crawling status
        
        Query parameters:
        - platform: redbus, all (default: all)
        """
        try:
            platform = request.args.get('platform', 'all')
            
            if platform == 'all':
                return jsonify({
                    'platforms': {
                        'redbus': {
                            'is_running': crawling_state['redbus']['is_running'],
                            'progress': crawling_state['redbus']['progress'],
                            'completed_tasks': crawling_state['redbus']['completed_tasks'],
                            'total_tasks': crawling_state['redbus']['total_tasks'],
                            'current_tasks': crawling_state['redbus']['current_tasks'],
                            'stats': crawling_state['redbus']['stats']
                        }
                    },
                    'timestamp': datetime.now().isoformat()
                }), 200
            
            elif platform in ['redbus']:
                return jsonify({
                    'platform': platform,
                    'is_running': crawling_state[platform]['is_running'],
                    'progress': crawling_state[platform]['progress'],
                    'completed_tasks': crawling_state[platform]['completed_tasks'],
                    'total_tasks': crawling_state[platform]['total_tasks'],
                    'current_tasks': crawling_state[platform]['current_tasks'],
                    'stats': crawling_state[platform]['stats'],
                    'logs': crawling_state[platform]['logs'][-10:],
                    'timestamp': datetime.now().isoformat()
                }), 200
            
            else:
                return jsonify({'error': f'Unknown platform: {platform}'}), 400
                
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    
    # ============ DATA QUERY API WITH FILTERING & SORTING ============
    
    @app.route('/api/v2/data', methods=['GET'])
    def api_v2_get_data():
        """
        Get crawled data with advanced filtering and sorting
        
        Query parameters for filtering:
        - platform: redbus
        - route_name: Jakarta-Semarang
        - route_date: 2025-11-27
        - bus_name: Sinar (partial match)
        - bus_type: Executive (partial match)
        - price_min: 100000
        - price_max: 500000
        - crawl_timestamp_from: 2025-11-27 00:00:00
        - crawl_timestamp_to: 2025-11-27 23:59:59
        - crawl_sequence: 1
        - light_g_bar: value
        - star_rating_min: 3.5
        - star_rating_max: 5.0
        
        Query parameters for sorting:
        - sort_by: column name (default: crawl_timestamp)
        - sort_order: asc or desc (default: desc)
        
        Query parameters for pagination:
        - page: 1 (default)
        - per_page: 100 (default, max: 1000)
        """
        try:
            # Pagination
            page = int(request.args.get('page', 1))
            per_page = min(int(request.args.get('per_page', 100)), 1000)
            
            # Sorting
            sort_by = request.args.get('sort_by', 'crawl_timestamp')
            sort_order = request.args.get('sort_order', 'desc').upper()
            
            if sort_order not in ['ASC', 'DESC']:
                sort_order = 'DESC'
            
            allowed_columns = [
                'id', 'platform', 'route_name', 'route_date', 'bus_name', 'bus_type',
                'departing_time', 'reaching_time', 'duration', 'star_rating', 'price',
                'seat_availability', 'light_g_bar', 'crawl_timestamp', 'crawl_sequence'
            ]
            
            if sort_by not in allowed_columns:
                sort_by = 'crawl_timestamp'
            
            # Filters
            platform = request.args.get('platform')
            route_name = request.args.get('route_name')
            route_date = request.args.get('route_date')
            bus_name = request.args.get('bus_name')
            bus_type = request.args.get('bus_type')
            price_min = request.args.get('price_min')
            price_max = request.args.get('price_max')
            crawl_timestamp_from = request.args.get('crawl_timestamp_from')
            crawl_timestamp_to = request.args.get('crawl_timestamp_to')
            crawl_sequence = request.args.get('crawl_sequence')
            light_g_bar = request.args.get('light_g_bar')
            star_rating_min = request.args.get('star_rating_min')
            star_rating_max = request.args.get('star_rating_max')
            
            # Database
            db = BusDatabase(db_type=db_config.get('type', 'mysql'), db_config=db_config)
            
            # Build WHERE clauses
            where_clauses = []
            params = []
            
            if platform:
                where_clauses.append("platform = ?")
                params.append(platform)
            if route_name:
                where_clauses.append("route_name = ?")
                params.append(route_name)
            if route_date:
                where_clauses.append("route_date = ?")
                params.append(route_date)
            if bus_name:
                where_clauses.append("bus_name LIKE ?")
                params.append(f'%{bus_name}%')
            if bus_type:
                where_clauses.append("bus_type LIKE ?")
                params.append(f'%{bus_type}%')
            if price_min:
                where_clauses.append("price >= ?")
                params.append(int(price_min))
            if price_max:
                where_clauses.append("price <= ?")
                params.append(int(price_max))
            if crawl_timestamp_from:
                where_clauses.append("crawl_timestamp >= ?")
                params.append(crawl_timestamp_from)
            if crawl_timestamp_to:
                where_clauses.append("crawl_timestamp <= ?")
                params.append(crawl_timestamp_to)
            if crawl_sequence:
                where_clauses.append("crawl_sequence = ?")
                params.append(int(crawl_sequence))
            if light_g_bar:
                where_clauses.append("light_g_bar = ?")
                params.append(light_g_bar)
            if star_rating_min:
                where_clauses.append("star_rating >= ?")
                params.append(float(star_rating_min))
            if star_rating_max:
                where_clauses.append("star_rating <= ?")
                params.append(float(star_rating_max))
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            # Count total
            count_sql = f"SELECT COUNT(*) as total FROM bus_data WHERE {where_sql}"
            if db.db_type in ['mysql', 'postgresql']:
                count_sql = count_sql.replace('?', '%s')
            
            db.cursor.execute(count_sql, tuple(params))
            result = db.cursor.fetchone()
            total_records = result[0] if isinstance(result, tuple) else result.get('total', 0)
            
            # Pagination calculation
            total_pages = (total_records + per_page - 1) // per_page
            offset = (page - 1) * per_page
            
            # Main query
            sql = f"""
                SELECT * FROM bus_data 
                WHERE {where_sql} 
                ORDER BY {sort_by} {sort_order}
                LIMIT ? OFFSET ?
            """
            
            query_params = params + [per_page, offset]
            
            if db.db_type in ['mysql', 'postgresql']:
                sql = sql.replace('?', '%s')
            
            df = pd.read_sql_query(sql, db.conn, params=query_params)
            db.close()
            
            # Convert to JSON
            data = df.to_dict('records')
            
            for record in data:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
            
            return jsonify({
                'success': True,
                'data': data,
                'pagination': {
                    'page': page,
                    'per_page': per_page,
                    'total_records': total_records,
                    'total_pages': total_pages,
                    'has_next': page < total_pages,
                    'has_prev': page > 1
                },
                'filters': {
                    'platform': platform,
                    'route_name': route_name,
                    'route_date': route_date,
                    'bus_name': bus_name,
                    'bus_type': bus_type,
                    'price_range': f"{price_min or 'any'} - {price_max or 'any'}",
                    'crawl_sequence': crawl_sequence,
                    'light_g_bar': light_g_bar,
                    'star_rating_range': f"{star_rating_min or 'any'} - {star_rating_max or 'any'}"
                },
                'sorting': {
                    'sort_by': sort_by,
                    'sort_order': sort_order
                },
                'timestamp': datetime.now().isoformat()
            }), 200
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    @app.route('/api/v2/data/summary', methods=['GET'])
    def api_v2_data_summary():
        """
        Get summary statistics with optional filtering
        
        Query parameters:
        - platform, route_name, route_date, crawl_sequence (optional)
        """
        try:
            platform = request.args.get('platform')
            route_name = request.args.get('route_name')
            route_date = request.args.get('route_date')
            crawl_sequence = request.args.get('crawl_sequence')
            
            db = BusDatabase(db_type=db_config.get('type', 'mysql'), db_config=db_config)
            
            where_clauses = []
            params = []
            
            if platform:
                where_clauses.append("platform = ?")
                params.append(platform)
            if route_name:
                where_clauses.append("route_name = ?")
                params.append(route_name)
            if route_date:
                where_clauses.append("route_date = ?")
                params.append(route_date)
            if crawl_sequence:
                where_clauses.append("crawl_sequence = ?")
                params.append(int(crawl_sequence))
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            sql = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT route_name) as total_routes,
                    COUNT(DISTINCT bus_name) as total_buses,
                    COUNT(DISTINCT route_date) as total_dates,
                    COUNT(DISTINCT DATE(crawl_timestamp)) as total_crawl_days,
                    AVG(price) as avg_price,
                    MIN(price) as min_price,
                    MAX(price) as max_price,
                    AVG(star_rating) as avg_rating,
                    MIN(star_rating) as min_rating,
                    MAX(star_rating) as max_rating,
                    MAX(crawl_sequence) as max_sequence
                FROM bus_data
                WHERE {where_sql}
            """
            
            if db.db_type in ['mysql', 'postgresql']:
                sql = sql.replace('?', '%s')
            
            db.cursor.execute(sql, tuple(params))
            result = db.cursor.fetchone()
            
            if db.db_type == 'mysql' and isinstance(result, dict):
                stats = result
            else:
                stats = {
                    'total_records': result[0] or 0,
                    'total_routes': result[1] or 0,
                    'total_buses': result[2] or 0,
                    'total_dates': result[3] or 0,
                    'total_crawl_days': result[4] or 0,
                    'avg_price': float(result[5]) if result[5] else 0,
                    'min_price': result[6] or 0,
                    'max_price': result[7] or 0,
                    'avg_rating': float(result[8]) if result[8] else 0,
                    'min_rating': float(result[9]) if result[9] else 0,
                    'max_rating': float(result[10]) if result[10] else 0,
                    'max_sequence': result[11] or 0
                }
            
            db.close()
            
            return jsonify({
                'success': True,
                'summary': stats,
                'filters': {
                    'platform': platform,
                    'route_name': route_name,
                    'route_date': route_date,
                    'crawl_sequence': crawl_sequence
                },
                'timestamp': datetime.now().isoformat()
            }), 200
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
    
    
    @app.route('/api/v2/data/export', methods=['GET'])
    def api_v2_export_data():
        """
        Export data to CSV with filtering (same filters as /api/v2/data)
        """
        try:
            platform = request.args.get('platform')
            route_name = request.args.get('route_name')
            route_date = request.args.get('route_date')
            bus_name = request.args.get('bus_name')
            bus_type = request.args.get('bus_type')
            price_min = request.args.get('price_min')
            price_max = request.args.get('price_max')
            crawl_timestamp_from = request.args.get('crawl_timestamp_from')
            crawl_timestamp_to = request.args.get('crawl_timestamp_to')
            crawl_sequence = request.args.get('crawl_sequence')
            light_g_bar = request.args.get('light_g_bar')
            star_rating_min = request.args.get('star_rating_min')
            star_rating_max = request.args.get('star_rating_max')
            
            db = BusDatabase(db_type=db_config.get('type', 'mysql'), db_config=db_config)
            
            where_clauses = []
            params = []
            
            if platform:
                where_clauses.append("platform = ?")
                params.append(platform)
            if route_name:
                where_clauses.append("route_name = ?")
                params.append(route_name)
            if route_date:
                where_clauses.append("route_date = ?")
                params.append(route_date)
            if bus_name:
                where_clauses.append("bus_name LIKE ?")
                params.append(f'%{bus_name}%')
            if bus_type:
                where_clauses.append("bus_type LIKE ?")
                params.append(f'%{bus_type}%')
            if price_min:
                where_clauses.append("price >= ?")
                params.append(int(price_min))
            if price_max:
                where_clauses.append("price <= ?")
                params.append(int(price_max))
            if crawl_timestamp_from:
                where_clauses.append("crawl_timestamp >= ?")
                params.append(crawl_timestamp_from)
            if crawl_timestamp_to:
                where_clauses.append("crawl_timestamp <= ?")
                params.append(crawl_timestamp_to)
            if crawl_sequence:
                where_clauses.append("crawl_sequence = ?")
                params.append(int(crawl_sequence))
            if light_g_bar:
                where_clauses.append("light_g_bar = ?")
                params.append(light_g_bar)
            if star_rating_min:
                where_clauses.append("star_rating >= ?")
                params.append(float(star_rating_min))
            if star_rating_max:
                where_clauses.append("star_rating <= ?")
                params.append(float(star_rating_max))
            
            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
            
            sql = f"SELECT * FROM bus_data WHERE {where_sql} ORDER BY crawl_timestamp DESC"
            
            if db.db_type in ['mysql', 'postgresql']:
                sql = sql.replace('?', '%s')
            
            df = pd.read_sql_query(sql, db.conn, params=tuple(params))
            db.close()
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'export_{platform or "all"}_{timestamp}.csv'
            filepath = os.path.join('new_data', filename)
            
            df.to_csv(filepath, index=False, encoding='utf-8')
            
            return send_file(filepath, as_attachment=True, download_name=filename)
            
        except Exception as e:
            return jsonify({'success': False, 'error': str(e)}), 500
