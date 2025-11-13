"""
Web interface for RouteManager - Add routes management to the unified crawler
"""

from flask import Flask, render_template, request, jsonify
from routes_manager import RouteManager
from url_crawler_formatter import CrawlTaskGenerator, CrawlConfigManager, URLFormatter
from datetime import datetime, timedelta
import json


def add_routes_management_endpoints(app, routes_manager=None):
    """Add route management endpoints to existing Flask app"""
    
    if routes_manager is None:
        routes_manager = RouteManager()
    
    task_generator = CrawlTaskGenerator(routes_manager)
    config_manager = CrawlConfigManager()
    url_formatter = URLFormatter(routes_manager)
    
    # ============ Routes Management API ============
    
    @app.route('/api/routes/master')
    def get_master_routes():
        """Get all master routes"""
        try:
            active_only = request.args.get('active_only', 'true').lower() == 'true'
            routes = routes_manager.get_all_routes(active_only=active_only)
            
            # Add platform availability info
            for route in routes:
                route['platforms'] = {
                    'redbus': routes_manager.get_platform_url(route['id'], 'redbus') is not None,
                    'traveloka': routes_manager.get_platform_url(route['id'], 'traveloka') is not None
                }
            
            return jsonify(routes)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/master', methods=['POST'])
    def add_master_route():
        """Add new master route"""
        try:
            data = request.json
            required_fields = ['name', 'origin', 'destination']
            
            for field in required_fields:
                if not data.get(field):
                    return jsonify({'error': f'Field {field} is required'}), 400
            
            route_id = routes_manager.add_route(
                name=data['name'],
                origin=data['origin'],
                destination=data['destination'],
                category=data.get('category', 'intercity'),
                active=data.get('active', True)
            )
            
            return jsonify({
                'success': True,
                'route_id': route_id,
                'message': f'Route {data["name"]} added successfully'
            })
        
        except ValueError as e:
            return jsonify({'error': str(e)}), 400
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/master/<route_id>', methods=['PUT'])
    def update_master_route(route_id):
        """Update master route"""
        try:
            data = request.json
            allowed_fields = ['name', 'origin', 'destination', 'category', 'active']
            
            updates = {k: v for k, v in data.items() if k in allowed_fields}
            
            if not updates:
                return jsonify({'error': 'No valid fields to update'}), 400
            
            success = routes_manager.update_route(route_id, **updates)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f'Route {route_id} updated successfully'
                })
            else:
                return jsonify({'error': 'Route not found'}), 404
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/master/<route_id>', methods=['DELETE'])
    def delete_master_route(route_id):
        """Delete master route (soft delete)"""
        try:
            hard_delete = request.args.get('hard', 'false').lower() == 'true'
            
            if hard_delete:
                success = routes_manager.hard_delete_route(route_id)
                message = 'Route permanently deleted'
            else:
                success = routes_manager.delete_route(route_id)
                message = 'Route deactivated'
            
            if success:
                return jsonify({
                    'success': True,
                    'message': message
                })
            else:
                return jsonify({'error': 'Route not found'}), 404
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # ============ Platform URLs Management ============
    
    @app.route('/api/routes/<route_id>/urls')
    def get_route_urls(route_id):
        """Get all platform URLs for a route"""
        try:
            urls = routes_manager.get_all_platform_urls(route_id)
            return jsonify(urls)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/<route_id>/urls/<platform>', methods=['POST', 'PUT'])
    def update_route_url(route_id, platform):
        """Add or update platform URL for route"""
        try:
            data = request.json
            url = data.get('url', '').strip()
            
            if not url:
                return jsonify({'error': 'URL is required'}), 400
            
            # Validate URL format for platform
            is_valid, message = routes_manager.validate_url_format(platform, url)
            if not is_valid:
                return jsonify({'error': f'Invalid URL format: {message}'}), 400
            
            success = routes_manager.add_platform_url(route_id, platform, url)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f'{platform.capitalize()} URL updated for route {route_id}'
                })
            else:
                return jsonify({'error': 'Failed to update URL'}), 500
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/<route_id>/urls/<platform>', methods=['DELETE'])
    def delete_route_url(route_id, platform):
        """Delete platform URL for route"""
        try:
            success = routes_manager.delete_platform_url(route_id, platform)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f'{platform.capitalize()} URL removed for route {route_id}'
                })
            else:
                return jsonify({'error': 'URL mapping not found'}), 404
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # ============ URL Formatting and Testing ============
    
    @app.route('/api/routes/format-url')
    def format_route_url():
        """Format URL for specific route, platform and date"""
        try:
            route_id = request.args.get('route_id')
            platform = request.args.get('platform')
            date_str = request.args.get('date')
            
            if not all([route_id, platform, date_str]):
                return jsonify({'error': 'route_id, platform, and date are required'}), 400
            
            # Validate date format
            try:
                datetime.strptime(date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({'error': 'Invalid date format. Expected YYYY-MM-DD'}), 400
            
            formatted_url = routes_manager.format_url_for_date(route_id, platform, date_str)
            
            if formatted_url:
                return jsonify({
                    'success': True,
                    'formatted_url': formatted_url,
                    'route_id': route_id,
                    'platform': platform,
                    'date': date_str
                })
            else:
                return jsonify({'error': 'Could not format URL. Check route_id and platform.'}), 404
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # ============ Task Generation ============
    
    @app.route('/api/routes/generate-tasks', methods=['POST'])
    def generate_crawl_tasks():
        """Generate crawling tasks for selected routes and dates"""
        try:
            data = request.json
            platforms = data.get('platforms', [])
            route_names = data.get('routes', [])
            
            # Handle date input
            dates = []
            if 'dates' in data:
                dates = data['dates']
            elif 'date_range' in data:
                start_date = data['date_range'].get('start')
                end_date = data['date_range'].get('end')
                if start_date and end_date:
                    dates = url_formatter.generate_date_range(start_date, end_date)
            
            if not dates:
                return jsonify({'error': 'No dates specified'}), 400
            
            # Generate tasks
            unified_tasks = task_generator.generate_unified_tasks(platforms, route_names, dates)
            
            # Count total tasks
            total_tasks = sum(len(tasks) for tasks in unified_tasks.values())
            
            return jsonify({
                'success': True,
                'tasks': unified_tasks,
                'summary': {
                    'total_tasks': total_tasks,
                    'platforms': list(unified_tasks.keys()),
                    'routes': len(route_names),
                    'dates': len(dates),
                    'date_range': f"{dates[0]} to {dates[-1]}" if len(dates) > 1 else dates[0] if dates else ""
                }
            })
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # ============ Configuration Presets ============
    
    @app.route('/api/routes/presets')
    def get_crawl_presets():
        """Get available crawling presets"""
        try:
            presets = config_manager.list_presets()
            return jsonify(presets)
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/presets/<preset_name>/tasks')
    def get_preset_tasks(preset_name):
        """Generate tasks from preset configuration"""
        try:
            tasks = config_manager.get_preset_tasks(preset_name)
            total_tasks = sum(len(task_list) for task_list in tasks.values())
            
            return jsonify({
                'success': True,
                'preset': preset_name,
                'tasks': tasks,
                'total_tasks': total_tasks
            })
        
        except ValueError as e:
            return jsonify({'error': str(e)}), 404
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/presets', methods=['POST'])
    def create_crawl_preset():
        """Create new crawling preset"""
        try:
            data = request.json
            required_fields = ['name', 'description', 'platforms', 'routes', 'date_range']
            
            for field in required_fields:
                if field not in data:
                    return jsonify({'error': f'Field {field} is required'}), 400
            
            success = config_manager.create_custom_preset(
                name=data['name'],
                description=data['description'],
                platforms=data['platforms'],
                routes=data['routes'],
                date_range=data['date_range']
            )
            
            if success:
                return jsonify({
                    'success': True,
                    'message': f'Preset {data["name"]} created successfully'
                })
            else:
                return jsonify({'error': 'Failed to create preset'}), 500
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # ============ Export/Import ============
    
    @app.route('/api/routes/export')
    def export_routes_config():
        """Export routes configuration"""
        try:
            filename = routes_manager.export_routes()
            return jsonify({
                'success': True,
                'filename': filename,
                'message': 'Routes configuration exported successfully'
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    @app.route('/api/routes/import', methods=['POST'])
    def import_routes_config():
        """Import routes configuration"""
        try:
            data = request.json
            filename = data.get('filename')
            merge = data.get('merge', True)
            
            if not filename:
                return jsonify({'error': 'Filename is required'}), 400
            
            success = routes_manager.import_routes(filename, merge=merge)
            
            if success:
                return jsonify({
                    'success': True,
                    'message': 'Routes configuration imported successfully'
                })
            else:
                return jsonify({'error': 'Failed to import configuration'}), 500
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500
    
    # ============ Legacy Compatibility ============
    
    @app.route('/api/routes/legacy/<platform>')
    def get_legacy_routes(platform):
        """Get routes in legacy format for backward compatibility"""
        try:
            from url_crawler_formatter import LegacyCompatibility
            
            legacy_compat = LegacyCompatibility(routes_manager)
            
            if platform == 'redbus':
                routes_dict, dates_list = legacy_compat.get_redbus_legacy_format()
                return jsonify({
                    'routes': routes_dict,
                    'dates': dates_list
                })
            elif platform == 'traveloka':
                traveloka_routes, traveloka_dates = legacy_compat.get_traveloka_legacy_format()
                return jsonify({
                    'routes': traveloka_routes,
                    'dates': traveloka_dates
                })
            else:
                return jsonify({'error': 'Unsupported platform'}), 400
        
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    return app


# ============ Standalone Route Management App ============

def create_routes_management_app():
    """Create standalone Flask app for route management"""
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'routes_manager_secret'
    
    # Add route management endpoints
    add_routes_management_endpoints(app)
    
    @app.route('/')
    def index():
        """Main route management interface"""
        return render_template('routes_management.html')
    
    @app.route('/health')
    def health_check():
        """Health check endpoint"""
        return jsonify({
            'status': 'ok',
            'service': 'routes_management',
            'timestamp': datetime.now().isoformat()
        })
    
    return app


if __name__ == "__main__":
    # Run standalone route management app
    app = create_routes_management_app()
    app.run(debug=True, host='0.0.0.0', port=5003)