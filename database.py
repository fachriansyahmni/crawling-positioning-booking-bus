"""
Database module for storing bus crawling data
Supports SQLite (local) and MySQL/PostgreSQL (production)
"""

import sqlite3
import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd


class BusDatabase:
    """Database handler for bus crawling data"""
    
    def __init__(self, db_type='sqlite', db_config=None):
        """
        Initialize database connection
        
        Args:
            db_type: 'sqlite', 'mysql', or 'postgresql'
            db_config: Database configuration dict (optional for sqlite)
                      For MySQL/PostgreSQL: {'host', 'port', 'database', 'user', 'password'}
        """
        self.db_type = db_type
        self.db_config = db_config or {}
        self.conn = None
        self.cursor = None
        
        self._connect()
        self._create_tables()
    
    def _connect(self):
        """Establish database connection"""
        if self.db_type == 'sqlite':
            # Default SQLite database
            db_path = self.db_config.get('database', 'data/bus_data.db')
            os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else 'data', exist_ok=True)
            self.conn = sqlite3.connect(db_path)
            self.cursor = self.conn.cursor()
            print(f"✓ Connected to SQLite database: {db_path}")
            
        elif self.db_type == 'mysql':
            try:
                import mysql.connector
                from mysql.connector import Error as MySQLError
                
                print(f"Connecting to MySQL at {self.db_config.get('host')}:{self.db_config.get('port', 3306)}...")
                
                self.conn = mysql.connector.connect(
                    host=self.db_config.get('host', 'localhost'),
                    port=self.db_config.get('port', 3306),
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    connection_timeout=10,  # 10 seconds timeout
                    connect_timeout=10,     # Alternative timeout parameter
                    use_pure=True,          # Use pure Python implementation (fixes hanging issue)
                    autocommit=False
                )
                self.cursor = self.conn.cursor(dictionary=True)
                print(f"✓ Connected to MySQL database: {self.db_config['database']}")
            except ImportError:
                raise Exception("MySQL connector not installed. Run: pip install mysql-connector-python")
            except MySQLError as err:
                error_msg = str(err)
                if err.errno == 2003:
                    raise Exception(f"Cannot connect to MySQL server at {self.db_config.get('host')}:{self.db_config.get('port', 3306)} - Server may be down or unreachable")
                elif err.errno == 1045:
                    raise Exception(f"Access denied for user '{self.db_config['user']}' - Check username and password")
                elif err.errno == 1049:
                    raise Exception(f"Unknown database '{self.db_config['database']}' - Database doesn't exist")
                else:
                    raise Exception(f"MySQL Error ({err.errno}): {error_msg}")
                
        elif self.db_type == 'postgresql':
            try:
                import psycopg2
                from psycopg2.extras import RealDictCursor
                self.conn = psycopg2.connect(
                    host=self.db_config.get('host', 'localhost'),
                    port=self.db_config.get('port', 5432),
                    database=self.db_config['database'],
                    user=self.db_config['user'],
                    password=self.db_config['password']
                )
                self.cursor = self.conn.cursor(cursor_factory=RealDictCursor)
                print(f"✓ Connected to PostgreSQL database: {self.db_config['database']}")
            except ImportError:
                raise Exception("PostgreSQL connector not installed. Run: pip install psycopg2-binary")
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        
        # Main bus data table
        if self.db_type == 'sqlite':
            bus_data_sql = """
            CREATE TABLE IF NOT EXISTS bus_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                route_date DATE NOT NULL,
                route_link TEXT,
                bus_name VARCHAR(200) NOT NULL,
                bus_type VARCHAR(100),
                departing_time VARCHAR(20),
                duration VARCHAR(50),
                reaching_time VARCHAR(20),
                star_rating DECIMAL(3,2),
                price INTEGER,
                seat_availability VARCHAR(50),
                crawl_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                data_hash VARCHAR(64)
            )
            """
            
            # Crawl sessions table
            crawl_sessions_sql = """
            CREATE TABLE IF NOT EXISTS crawl_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                route_date DATE NOT NULL,
                start_time DATETIME NOT NULL,
                end_time DATETIME,
                total_records INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'running',
                error_message TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
        elif self.db_type == 'mysql':
            bus_data_sql = """
            CREATE TABLE IF NOT EXISTS bus_data (
                id INT AUTO_INCREMENT PRIMARY KEY,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                route_date DATE NOT NULL,
                route_link TEXT,
                bus_name VARCHAR(200) NOT NULL,
                bus_type VARCHAR(100),
                departing_time VARCHAR(20),
                duration VARCHAR(50),
                reaching_time VARCHAR(20),
                star_rating DECIMAL(3,2),
                price INTEGER,
                seat_availability VARCHAR(50),
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_hash VARCHAR(64)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            crawl_sessions_sql = """
            CREATE TABLE IF NOT EXISTS crawl_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                route_date DATE NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NULL,
                total_records INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'running',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        
        else:  # PostgreSQL
            bus_data_sql = """
            CREATE TABLE IF NOT EXISTS bus_data (
                id SERIAL PRIMARY KEY,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                route_date DATE NOT NULL,
                route_link TEXT,
                bus_name VARCHAR(200) NOT NULL,
                bus_type VARCHAR(100),
                departing_time VARCHAR(20),
                duration VARCHAR(50),
                reaching_time VARCHAR(20),
                star_rating DECIMAL(3,2),
                price INTEGER,
                seat_availability VARCHAR(50),
                crawl_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                data_hash VARCHAR(64)
            )
            """
            
            crawl_sessions_sql = """
            CREATE TABLE IF NOT EXISTS crawl_sessions (
                id SERIAL PRIMARY KEY,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                route_date DATE NOT NULL,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP,
                total_records INTEGER DEFAULT 0,
                status VARCHAR(50) DEFAULT 'running',
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        
        # Create indexes for faster queries
        if self.db_type == 'sqlite':
            indexes_sql = [
                "CREATE INDEX IF NOT EXISTS idx_platform ON bus_data(platform)",
                "CREATE INDEX IF NOT EXISTS idx_route_date ON bus_data(route_name, route_date)",
                "CREATE INDEX IF NOT EXISTS idx_crawl_timestamp ON bus_data(crawl_timestamp)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_platform ON crawl_sessions(platform)",
                "CREATE INDEX IF NOT EXISTS idx_sessions_route ON crawl_sessions(route_name, route_date)"
            ]
        else:  # MySQL/PostgreSQL - use different syntax
            indexes_sql = [
                "CREATE INDEX idx_platform ON bus_data(platform)",
                "CREATE INDEX idx_route_date ON bus_data(route_name, route_date)",
                "CREATE INDEX idx_crawl_timestamp ON bus_data(crawl_timestamp)",
                "CREATE INDEX idx_sessions_platform ON crawl_sessions(platform)",
                "CREATE INDEX idx_sessions_route ON crawl_sessions(route_name, route_date)"
            ]
        
        # Prediction tables
        if self.db_type == 'sqlite':
            prediction_sessions_sql = """
            CREATE TABLE IF NOT EXISTS prediction_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prediction_period VARCHAR(50) NOT NULL,
                prediction_start_date DATE NOT NULL,
                prediction_end_date DATE NOT NULL,
                model_version VARCHAR(100),
                training_data_days INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            predictions_sql = """
            CREATE TABLE IF NOT EXISTS predictions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER NOT NULL,
                prediction_date DATE NOT NULL,
                day_of_week INTEGER NOT NULL,
                is_weekend INTEGER NOT NULL,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                bus_name VARCHAR(200) NOT NULL,
                predicted_total INTEGER NOT NULL,
                predicted_vip INTEGER NOT NULL,
                predicted_executive INTEGER NOT NULL,
                predicted_other INTEGER NOT NULL,
                predicted_departing_time VARCHAR(20),
                predicted_reaching_time VARCHAR(20),
                predicted_price INTEGER,
                actual_total INTEGER,
                actual_vip INTEGER,
                actual_executive INTEGER,
                actual_other INTEGER,
                accuracy_score DECIMAL(5,2),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES prediction_sessions(id)
            )
            """
        elif self.db_type == 'mysql':
            prediction_sessions_sql = """
            CREATE TABLE IF NOT EXISTS prediction_sessions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                prediction_period VARCHAR(50) NOT NULL,
                prediction_start_date DATE NOT NULL,
                prediction_end_date DATE NOT NULL,
                model_version VARCHAR(100),
                training_data_days INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
            
            predictions_sql = """
            CREATE TABLE IF NOT EXISTS predictions (
                id INT AUTO_INCREMENT PRIMARY KEY,
                session_id INT NOT NULL,
                prediction_date DATE NOT NULL,
                day_of_week INTEGER NOT NULL,
                is_weekend INTEGER NOT NULL,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                bus_name VARCHAR(200) NOT NULL,
                predicted_total INTEGER NOT NULL,
                predicted_vip INTEGER NOT NULL,
                predicted_executive INTEGER NOT NULL,
                predicted_other INTEGER NOT NULL,
                predicted_departing_time VARCHAR(20),
                predicted_reaching_time VARCHAR(20),
                predicted_price INTEGER,
                actual_total INTEGER,
                actual_vip INTEGER,
                actual_executive INTEGER,
                actual_other INTEGER,
                accuracy_score DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES prediction_sessions(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        else:  # PostgreSQL
            prediction_sessions_sql = """
            CREATE TABLE IF NOT EXISTS prediction_sessions (
                id SERIAL PRIMARY KEY,
                prediction_period VARCHAR(50) NOT NULL,
                prediction_start_date DATE NOT NULL,
                prediction_end_date DATE NOT NULL,
                model_version VARCHAR(100),
                training_data_days INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            predictions_sql = """
            CREATE TABLE IF NOT EXISTS predictions (
                id SERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL,
                prediction_date DATE NOT NULL,
                day_of_week INTEGER NOT NULL,
                is_weekend INTEGER NOT NULL,
                platform VARCHAR(50) NOT NULL,
                route_name VARCHAR(100) NOT NULL,
                bus_name VARCHAR(200) NOT NULL,
                predicted_total INTEGER NOT NULL,
                predicted_vip INTEGER NOT NULL,
                predicted_executive INTEGER NOT NULL,
                predicted_other INTEGER NOT NULL,
                predicted_departing_time VARCHAR(20),
                predicted_reaching_time VARCHAR(20),
                predicted_price INTEGER,
                actual_total INTEGER,
                actual_vip INTEGER,
                actual_executive INTEGER,
                actual_other INTEGER,
                accuracy_score DECIMAL(5,2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (session_id) REFERENCES prediction_sessions(id)
            )
            """
        
        # Prediction indexes
        if self.db_type == 'sqlite':
            prediction_indexes = [
                "CREATE INDEX IF NOT EXISTS idx_pred_session ON predictions(session_id)",
                "CREATE INDEX IF NOT EXISTS idx_pred_date ON predictions(prediction_date)",
                "CREATE INDEX IF NOT EXISTS idx_pred_company ON predictions(bus_name)",
                "CREATE INDEX IF NOT EXISTS idx_pred_route ON predictions(route_name)"
            ]
        else:
            prediction_indexes = [
                "CREATE INDEX idx_pred_session ON predictions(session_id)",
                "CREATE INDEX idx_pred_date ON predictions(prediction_date)",
                "CREATE INDEX idx_pred_company ON predictions(bus_name)",
                "CREATE INDEX idx_pred_route ON predictions(route_name)"
            ]
        
        try:
            self.cursor.execute(bus_data_sql)
            self.cursor.execute(crawl_sessions_sql)
            self.cursor.execute(prediction_sessions_sql)
            self.cursor.execute(predictions_sql)
            self.conn.commit()
            
            # Create indexes, ignore if they already exist
            for index_sql in indexes_sql + prediction_indexes:
                try:
                    self.cursor.execute(index_sql)
                    self.conn.commit()
                except Exception as idx_error:
                    # Index might already exist, that's ok
                    if 'already exists' in str(idx_error).lower() or 'duplicate' in str(idx_error).lower():
                        pass  # Ignore duplicate index errors
                    else:
                        print(f"Warning: Could not create index: {idx_error}")
                    self.conn.rollback()
            
            print("✓ Database tables created successfully")
        except Exception as e:
            print(f"Error creating tables: {e}")
            self.conn.rollback()
            raise
    
    def _generate_hash(self, bus_data: Dict) -> str:
        """Generate unique hash for bus data to prevent duplicates"""
        import hashlib
        
        # Create unique string from key fields
        unique_str = f"{bus_data.get('platform', '')}|" \
                    f"{bus_data.get('route_name', '')}|" \
                    f"{bus_data.get('route_date', '')}|" \
                    f"{bus_data.get('bus_name', '')}|" \
                    f"{bus_data.get('departing_time', '')}|" \
                    f"{bus_data.get('price', '')}"
        
        return hashlib.sha256(unique_str.encode()).hexdigest()
    
    def insert_bus_data(self, data: Dict, platform: str) -> bool:
        """
        Insert single bus data record
        
        Args:
            data: Dictionary containing bus data
            platform: 'traveloka' or 'redbus'
        
        Returns:
            True if inserted successfully
        """
        # Normalize field names (handle both formats)
        normalized_data = {
            'platform': platform.lower(),
            'route_name': data.get('Route_Name', data.get('route_name', '')),
            'route_date': data.get('Route_Date', data.get('route_date', '')),
            'route_link': data.get('Route_Link', data.get('route_link', '')),
            'bus_name': data.get('Bus_Name', data.get('bus_name', '')),
            'bus_type': data.get('Bus_Type', data.get('bus_type', '')),
            'departing_time': data.get('Departing_Time', data.get('departing_time', '')),
            'duration': data.get('Duration', data.get('duration', '')),
            'reaching_time': data.get('Reaching_Time', data.get('reaching_time', '')),
            'star_rating': data.get('Star_Rating', data.get('star_rating')),
            'price': data.get('Price', data.get('price')),
            'seat_availability': data.get('Seat_Availability', data.get('seat_availability', ''))
        }
        
        # Generate hash for reference (not for duplicate prevention)
        data_hash = self._generate_hash(normalized_data)
        normalized_data['data_hash'] = data_hash
        
        # Convert empty strings to None for numeric fields
        if normalized_data['star_rating'] == '' or normalized_data['star_rating'] is None:
            normalized_data['star_rating'] = None
        if normalized_data['price'] == '' or normalized_data['price'] is None:
            normalized_data['price'] = None
        
        sql = """
        INSERT INTO bus_data (
            platform, route_name, route_date, route_link, bus_name, bus_type,
            departing_time, duration, reaching_time, star_rating, price,
            seat_availability, data_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            self.cursor.execute(sql, (
                normalized_data['platform'],
                normalized_data['route_name'],
                normalized_data['route_date'],
                normalized_data['route_link'],
                normalized_data['bus_name'],
                normalized_data['bus_type'],
                normalized_data['departing_time'],
                normalized_data['duration'],
                normalized_data['reaching_time'],
                normalized_data['star_rating'],
                normalized_data['price'],
                normalized_data['seat_availability'],
                normalized_data['data_hash']
            ))
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error inserting data: {e}")
            self.conn.rollback()
            raise
    
    def insert_bulk_data(self, data_list: List[Dict], platform: str) -> Dict:
        """
        Insert multiple bus data records
        
        Args:
            data_list: List of dictionaries containing bus data
            platform: 'traveloka' or 'redbus'
        
        Returns:
            Dictionary with statistics: {'inserted': count, 'errors': count}
        """
        stats = {'inserted': 0, 'errors': 0}
        
        for data in data_list:
            try:
                if self.insert_bus_data(data, platform):
                    stats['inserted'] += 1
            except Exception as e:
                stats['errors'] += 1
                print(f"Error processing record: {e}")
        
        return stats
    
    def start_crawl_session(self, platform: str, route_name: str, route_date: str) -> int:
        """
        Start a new crawl session
        
        Returns:
            session_id
        """
        sql = """
        INSERT INTO crawl_sessions (platform, route_name, route_date, start_time, status)
        VALUES (?, ?, ?, ?, 'running')
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            self.cursor.execute(sql, (platform, route_name, route_date, datetime.now()))
            self.conn.commit()
            
            # Get the inserted ID
            if self.db_type == 'sqlite':
                return self.cursor.lastrowid
            else:
                self.cursor.execute("SELECT LASTVAL()")
                return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error starting crawl session: {e}")
            self.conn.rollback()
            raise
    
    def end_crawl_session(self, session_id: int, total_records: int, status: str = 'completed', error_message: str = None):
        """
        End a crawl session
        
        Args:
            session_id: ID of the session to end
            total_records: Total number of records crawled
            status: 'completed', 'failed', or 'stopped'
            error_message: Error message if failed
        """
        sql = """
        UPDATE crawl_sessions
        SET end_time = ?, total_records = ?, status = ?, error_message = ?
        WHERE id = ?
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            self.cursor.execute(sql, (datetime.now(), total_records, status, error_message, session_id))
            self.conn.commit()
        except Exception as e:
            print(f"Error ending crawl session: {e}")
            self.conn.rollback()
    
    def import_from_csv(self, csv_path: str, platform: str) -> Dict:
        """
        Import data from CSV file
        
        Args:
            csv_path: Path to CSV file
            platform: 'traveloka' or 'redbus'
        
        Returns:
            Dictionary with import statistics
        """
        try:
            df = pd.read_csv(csv_path)
            data_list = df.to_dict('records')
            stats = self.insert_bulk_data(data_list, platform)
            print(f"✓ Imported {csv_path}: {stats['inserted']} inserted, {stats['duplicates']} duplicates, {stats['errors']} errors")
            return stats
        except Exception as e:
            print(f"Error importing CSV {csv_path}: {e}")
            return {'inserted': 0, 'duplicates': 0, 'errors': 1}
    
    def import_from_directory(self, directory: str, platform: Optional[str] = None) -> Dict:
        """
        Import all CSV files from a directory
        
        Args:
            directory: Path to directory containing CSV files
            platform: Optional platform filter ('traveloka' or 'redbus')
                     If None, auto-detect from filename
        
        Returns:
            Dictionary with cumulative statistics
        """
        total_stats = {'inserted': 0, 'duplicates': 0, 'errors': 0, 'files': 0}
        
        if not os.path.exists(directory):
            print(f"Directory not found: {directory}")
            return total_stats
        
        for filename in os.listdir(directory):
            if not filename.endswith('.csv'):
                continue
            
            # Auto-detect platform from filename
            detected_platform = platform
            if not detected_platform:
                if 'traveloka' in filename.lower():
                    detected_platform = 'traveloka'
                elif 'redbus' in filename.lower():
                    detected_platform = 'redbus'
                else:
                    print(f"⚠ Cannot detect platform for {filename}, skipping")
                    continue
            
            csv_path = os.path.join(directory, filename)
            stats = self.import_from_csv(csv_path, detected_platform)
            
            total_stats['inserted'] += stats['inserted']
            total_stats['duplicates'] += stats['duplicates']
            total_stats['errors'] += stats['errors']
            total_stats['files'] += 1
        
        print(f"\n{'='*60}")
        print(f"IMPORT SUMMARY:")
        print(f"  Files processed: {total_stats['files']}")
        print(f"  Records inserted: {total_stats['inserted']}")
        print(f"  Duplicates skipped: {total_stats['duplicates']}")
        print(f"  Errors: {total_stats['errors']}")
        print(f"{'='*60}")
        
        return total_stats
    
    def query_data(self, platform: Optional[str] = None, route_name: Optional[str] = None, 
                   route_date: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
        """
        Query bus data
        
        Args:
            platform: Filter by platform
            route_name: Filter by route
            route_date: Filter by date
            limit: Maximum number of records
        
        Returns:
            DataFrame with results
        """
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
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        sql = f"SELECT * FROM bus_data WHERE {where_sql} ORDER BY crawl_timestamp DESC LIMIT ?"
        params.append(limit)
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            df = pd.read_sql_query(sql, self.conn, params=params)
            return df
        except Exception as e:
            print(f"Error querying data: {e}")
            return pd.DataFrame()
    
    def get_statistics(self) -> Dict:
        """Get database statistics"""
        stats = {}
        
        try:
            # Total records
            self.cursor.execute("SELECT COUNT(*) as total FROM bus_data")
            result = self.cursor.fetchone()
            stats['total_records'] = result[0] if isinstance(result, tuple) else result.get('total', 0)
            
            # Records by platform
            self.cursor.execute("SELECT platform, COUNT(*) as count FROM bus_data GROUP BY platform")
            rows = self.cursor.fetchall()
            if rows and isinstance(rows[0], dict):
                stats['by_platform'] = {row['platform']: row['count'] for row in rows}
            else:
                stats['by_platform'] = {row[0]: row[1] for row in rows}
            
            # Records by route
            self.cursor.execute("SELECT route_name, COUNT(*) as count FROM bus_data GROUP BY route_name ORDER BY count DESC LIMIT 10")
            rows = self.cursor.fetchall()
            if rows and isinstance(rows[0], dict):
                stats['top_routes'] = {row['route_name']: row['count'] for row in rows}
            else:
                stats['top_routes'] = {row[0]: row[1] for row in rows}
            
            # Latest crawl
            self.cursor.execute("SELECT MAX(crawl_timestamp) FROM bus_data")
            result = self.cursor.fetchone()
            latest = result[0] if isinstance(result, tuple) else result.get('MAX(crawl_timestamp)')
            stats['latest_crawl'] = latest if latest else 'N/A'
            
            # Crawl sessions
            self.cursor.execute("SELECT COUNT(*) as total FROM crawl_sessions")
            result = self.cursor.fetchone()
            stats['total_sessions'] = result[0] if isinstance(result, tuple) else result.get('total', 0)
            
            return stats
        except Exception as e:
            print(f"Error getting statistics: {e}")
            import traceback
            traceback.print_exc()
            return stats
    
    def create_prediction_session(self, prediction_period: str, start_date: str, end_date: str, 
                                  model_version: str = None, training_days: int = None) -> int:
        """
        Create a new prediction session
        
        Args:
            prediction_period: 'next_week', 'next_month', 'next_year'
            start_date: Start date of prediction range (YYYY-MM-DD)
            end_date: End date of prediction range (YYYY-MM-DD)
            model_version: Optional model version identifier
            training_days: Number of days used for training
        
        Returns:
            session_id
        """
        sql = """
        INSERT INTO prediction_sessions 
        (prediction_period, prediction_start_date, prediction_end_date, model_version, training_data_days)
        VALUES (?, ?, ?, ?, ?)
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            self.cursor.execute(sql, (prediction_period, start_date, end_date, model_version, training_days))
            self.conn.commit()
            
            # Get the inserted ID
            if self.db_type == 'sqlite':
                return self.cursor.lastrowid
            elif self.db_type == 'mysql':
                return self.cursor.lastrowid
            else:  # postgresql
                self.cursor.execute("SELECT LASTVAL()")
                return self.cursor.fetchone()[0]
        except Exception as e:
            print(f"Error creating prediction session: {e}")
            self.conn.rollback()
            raise
    
    def save_predictions(self, session_id: int, predictions_df) -> int:
        """
        Save predictions to database
        
        Args:
            session_id: ID of the prediction session
            predictions_df: DataFrame with predictions
        
        Returns:
            Number of records inserted
        """
        if predictions_df.empty:
            return 0
        
        sql = """
        INSERT INTO predictions 
        (session_id, prediction_date, day_of_week, is_weekend, platform, route_name, bus_name,
         predicted_total, predicted_vip, predicted_executive, predicted_other,
         predicted_departing_time, predicted_reaching_time, predicted_price)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        inserted_count = 0
        
        try:
            for _, row in predictions_df.iterrows():
                is_weekend = 1 if row['is_weekend'] == 'Weekend' else 0
                day_of_week = row.get('day_of_week', 0)
                
                # If day_of_week not in dataframe, calculate from date
                if 'day_of_week' not in row or pd.isna(day_of_week):
                    from datetime import datetime
                    date_obj = datetime.strptime(row['date'], '%Y-%m-%d')
                    day_of_week = date_obj.weekday()
                
                # Get new prediction fields with defaults if not present
                pred_departing = row.get('predicted_departing_time', None)
                pred_reaching = row.get('predicted_reaching_time', None)
                pred_price = int(row['predicted_price']) if 'predicted_price' in row and pd.notna(row['predicted_price']) else None
                
                self.cursor.execute(sql, (
                    session_id,
                    row['date'],
                    int(day_of_week),
                    is_weekend,
                    row['platform'],
                    row['route_name'],
                    row['bus_name'],
                    int(row['predicted_total']),
                    int(row['predicted_vip']),
                    int(row['predicted_executive']),
                    int(row['predicted_other']),
                    pred_departing,
                    pred_reaching,
                    pred_price
                ))
                inserted_count += 1
            
            self.conn.commit()
            print(f"✓ Saved {inserted_count} predictions to database")
            return inserted_count
            
        except Exception as e:
            print(f"Error saving predictions: {e}")
            self.conn.rollback()
            raise
    
    def get_prediction_sessions(self, limit: int = 10) -> pd.DataFrame:
        """Get recent prediction sessions"""
        sql = """
        SELECT 
            id, prediction_period, prediction_start_date, prediction_end_date,
            model_version, training_data_days, created_at
        FROM prediction_sessions
        ORDER BY created_at DESC
        LIMIT ?
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            self.cursor.execute(sql, (limit,))
            rows = self.cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            if isinstance(rows[0], dict):
                return pd.DataFrame(rows)
            else:
                columns = ['id', 'prediction_period', 'prediction_start_date', 'prediction_end_date',
                          'model_version', 'training_data_days', 'created_at']
                return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error getting prediction sessions: {e}")
            return pd.DataFrame()
    
    def get_predictions(self, session_id: int = None, date: str = None, 
                       bus_name: str = None, limit: int = 100) -> pd.DataFrame:
        """
        Get predictions with optional filters
        
        Args:
            session_id: Filter by session ID
            date: Filter by prediction date
            bus_name: Filter by bus company name
            limit: Maximum number of records
        
        Returns:
            DataFrame with predictions
        """
        where_clauses = []
        params = []
        
        if session_id:
            where_clauses.append("session_id = ?")
            params.append(int(session_id))  # Convert to native int
        
        if date:
            where_clauses.append("prediction_date = ?")
            params.append(str(date))
        
        if bus_name:
            where_clauses.append("bus_name = ?")
            params.append(str(bus_name))
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sql = f"""
        SELECT 
            p.id, p.session_id, p.prediction_date, p.day_of_week, p.is_weekend,
            p.platform, p.route_name, p.bus_name,
            p.predicted_total, p.predicted_vip, p.predicted_executive, p.predicted_other,
            p.predicted_departing_time, p.predicted_reaching_time, p.predicted_price,
            p.actual_total, p.actual_vip, p.actual_executive, p.actual_other,
            p.accuracy_score, p.created_at,
            ps.prediction_period
        FROM predictions p
        JOIN prediction_sessions ps ON p.session_id = ps.id
        WHERE {where_sql}
        ORDER BY p.prediction_date, p.bus_name
        LIMIT ?
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        params.append(int(limit))  # Convert to native int
        
        try:
            self.cursor.execute(sql, tuple(params))
            rows = self.cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            if isinstance(rows[0], dict):
                return pd.DataFrame(rows)
            else:
                columns = ['id', 'session_id', 'prediction_date', 'day_of_week', 'is_weekend',
                          'platform', 'route_name', 'bus_name',
                          'predicted_total', 'predicted_vip', 'predicted_executive', 'predicted_other',
                          'predicted_departing_time', 'predicted_reaching_time', 'predicted_price',
                          'actual_total', 'actual_vip', 'actual_executive', 'actual_other',
                          'accuracy_score', 'created_at', 'prediction_period']
                return pd.DataFrame(rows, columns=columns)
        except Exception as e:
            print(f"Error getting predictions: {e}")
            return pd.DataFrame()
    
    def update_actual_values(self, prediction_date: str, bus_name: str, 
                            actual_total: int, actual_vip: int, actual_executive: int):
        """
        Update predictions with actual values for accuracy tracking
        
        Args:
            prediction_date: Date of the prediction
            bus_name: Bus company name
            actual_total: Actual total buses observed
            actual_vip: Actual VIP buses observed
            actual_executive: Actual Executive buses observed
        """
        actual_other = max(0, actual_total - actual_vip - actual_executive)
        
        # Get predictions for this date and company
        sql_get = """
        SELECT id, predicted_total, predicted_vip, predicted_executive
        FROM predictions
        WHERE prediction_date = ? AND bus_name = ?
        """
        
        if self.db_type in ['mysql', 'postgresql']:
            sql_get = sql_get.replace('?', '%s')
        
        try:
            self.cursor.execute(sql_get, (prediction_date, bus_name))
            predictions = self.cursor.fetchall()
            
            if not predictions:
                print(f"No predictions found for {bus_name} on {prediction_date}")
                return
            
            # Update each prediction
            sql_update = """
            UPDATE predictions
            SET actual_total = ?, actual_vip = ?, actual_executive = ?, actual_other = ?,
                accuracy_score = ?
            WHERE id = ?
            """
            
            if self.db_type in ['mysql', 'postgresql']:
                sql_update = sql_update.replace('?', '%s')
            
            for pred in predictions:
                pred_id = pred['id'] if isinstance(pred, dict) else pred[0]
                pred_total = pred['predicted_total'] if isinstance(pred, dict) else pred[1]
                pred_vip = pred['predicted_vip'] if isinstance(pred, dict) else pred[2]
                pred_executive = pred['predicted_executive'] if isinstance(pred, dict) else pred[3]
                
                # Calculate accuracy (1 - MAPE)
                if actual_total > 0:
                    accuracy = 100 * (1 - abs(pred_total - actual_total) / actual_total)
                    accuracy = max(0, min(100, accuracy))  # Clamp between 0-100
                else:
                    accuracy = 0 if pred_total > 0 else 100
                
                self.cursor.execute(sql_update, (
                    actual_total, actual_vip, actual_executive, actual_other,
                    accuracy, pred_id
                ))
            
            self.conn.commit()
            print(f"✓ Updated actual values for {bus_name} on {prediction_date}")
            
        except Exception as e:
            print(f"Error updating actual values: {e}")
            self.conn.rollback()
    
    def get_prediction_accuracy(self, session_id: int = None) -> Dict:
        """
        Calculate prediction accuracy statistics
        
        Args:
            session_id: Optional filter by session ID
        
        Returns:
            Dictionary with accuracy metrics
        """
        where_clause = f"WHERE session_id = {session_id}" if session_id else "WHERE actual_total IS NOT NULL"
        
        if self.db_type in ['mysql', 'postgresql']:
            where_clause = where_clause.replace('?', '%s')
        
        sql = f"""
        SELECT 
            COUNT(*) as total_predictions,
            AVG(accuracy_score) as avg_accuracy,
            MIN(accuracy_score) as min_accuracy,
            MAX(accuracy_score) as max_accuracy,
            AVG(ABS(predicted_total - actual_total)) as mae_total,
            AVG(ABS(predicted_vip - actual_vip)) as mae_vip,
            AVG(ABS(predicted_executive - actual_executive)) as mae_executive
        FROM predictions
        {where_clause}
        """
        
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchone()
            
            if isinstance(result, dict):
                return result
            else:
                return {
                    'total_predictions': result[0] or 0,
                    'avg_accuracy': result[1] or 0,
                    'min_accuracy': result[2] or 0,
                    'max_accuracy': result[3] or 0,
                    'mae_total': result[4] or 0,
                    'mae_vip': result[5] or 0,
                    'mae_executive': result[6] or 0
                }
        except Exception as e:
            print(f"Error calculating accuracy: {e}")
            return {}
    
    def get_training_data(self, days_back=90):
        """
        Get data for training ML models
        
        Args:
            days_back: Number of days to look back from today
        
        Returns:
            DataFrame with training data
        """
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # SQL query to get training data
            query = """
                SELECT 
                    platform,
                    route_name,
                    route_date,
                    bus_name,
                    bus_type,
                    price,
                    star_rating,
                    seat_availability,
                    departing_time,
                    reaching_time,
                    duration,
                    crawl_timestamp
                FROM bus_data
                WHERE crawl_timestamp >= ?
                ORDER BY crawl_timestamp
            """
            
            if self.db_type == 'sqlite':
                cursor = self.conn.cursor()
                cursor.execute(query, (start_date.strftime('%Y-%m-%d %H:%M:%S'),))
                rows = cursor.fetchall()
                
                # Get column names
                columns = [description[0] for description in cursor.description]
                
                # Convert to DataFrame
                import pandas as pd
                df = pd.DataFrame(rows, columns=columns)
                
            elif self.db_type == 'mysql':
                cursor = self.conn.cursor(dictionary=True)
                cursor.execute(query.replace('?', '%s'), (start_date.strftime('%Y-%m-%d %H:%M:%S'),))
                rows = cursor.fetchall()
                
                import pandas as pd
                df = pd.DataFrame(rows)
                
            elif self.db_type == 'postgresql':
                cursor = self.conn.cursor()
                cursor.execute(query.replace('?', '%s'), (start_date.strftime('%Y-%m-%d %H:%M:%S'),))
                rows = cursor.fetchall()
                
                columns = [desc[0] for desc in cursor.description]
                
                import pandas as pd
                df = pd.DataFrame(rows, columns=columns)
            
            cursor.close()
            
            # Convert data types
            if not df.empty:
                df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'])
                df['route_date'] = pd.to_datetime(df['route_date'])
                df['price'] = pd.to_numeric(df['price'], errors='coerce')
                df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
            
            return df
            
        except Exception as e:
            print(f"Error getting training data: {str(e)}")
            import pandas as pd
            return pd.DataFrame()

    def close(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        print("✓ Database connection closed")


def load_db_config():
    """Load database configuration from config file"""
    config_files = ['config_db.json', 'config.json']
    
    for config_file in config_files:
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
                if 'database' in config:
                    return config['database']
    
    # Default SQLite config
    return {
        'type': 'sqlite',
        'database': 'data/bus_data.db'
    }


# Example usage
if __name__ == '__main__':
    # Initialize database
    db_config = load_db_config()
    db = BusDatabase(
        db_type=db_config.get('type', 'sqlite'),
        db_config=db_config
    )
    
    # Show current statistics
    print("\nCurrent Database Statistics:")
    stats = db.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Example: Import from data directory
    print("\n" + "="*60)
    choice = input("Import CSV files from data directory? (y/n): ").strip().lower()
    
    if choice == 'y':
        # Import from both directories
        print("\nImporting Traveloka data...")
        traveloka_stats = db.import_from_directory('data/traveloka', 'traveloka')
        
        print("\nImporting Redbus data...")
        redbus_stats = db.import_from_directory('data/redbus', 'redbus')
        
        # Show updated statistics
        print("\nUpdated Database Statistics:")
        stats = db.get_statistics()
        for key, value in stats.items():
            print(f"  {key}: {value}")
    
    # Close connection
    db.close()
