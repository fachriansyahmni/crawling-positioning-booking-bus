"""
Migration Script: Add crawl_sequence column
This script adds a new column to track how many times a route has been crawled on the same day
"""

import sys
import json
from datetime import datetime


def load_db_config():
    """Load database configuration from config file"""
    import os
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


def migrate_sqlite(conn):
    """Add crawl_sequence column to SQLite database"""
    cursor = conn.cursor()
    
    try:
        # Check if column already exists
        cursor.execute("PRAGMA table_info(bus_data)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'crawl_sequence' in columns:
            print("✓ Column 'crawl_sequence' already exists in bus_data table")
            return True
        
        # Add the new column after crawl_timestamp
        print("Adding 'crawl_sequence' column to bus_data table...")
        cursor.execute("""
            ALTER TABLE bus_data 
            ADD COLUMN crawl_sequence INTEGER DEFAULT 1
        """)
        
        # Create index for faster queries
        print("Creating index on crawl_sequence...")
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_crawl_sequence 
            ON bus_data(route_name, route_date, crawl_sequence)
        """)
        
        conn.commit()
        print("✓ Migration completed successfully for SQLite!")
        return True
        
    except Exception as e:
        print(f"❌ Error during SQLite migration: {e}")
        conn.rollback()
        return False


def migrate_mysql(conn):
    """Add crawl_sequence column to MySQL database"""
    cursor = conn.cursor()
    
    try:
        # Check if column already exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_NAME = 'bus_data' 
            AND COLUMN_NAME = 'crawl_sequence'
        """)
        
        result = cursor.fetchone()
        if result[0] > 0:
            print("✓ Column 'crawl_sequence' already exists in bus_data table")
            return True
        
        # Add the new column after crawl_timestamp
        print("Adding 'crawl_sequence' column to bus_data table...")
        cursor.execute("""
            ALTER TABLE bus_data 
            ADD COLUMN crawl_sequence INT DEFAULT 1 
            AFTER crawl_timestamp
        """)
        
        # Create index for faster queries
        print("Creating index on crawl_sequence...")
        cursor.execute("""
            CREATE INDEX idx_crawl_sequence 
            ON bus_data(route_name, route_date, crawl_sequence)
        """)
        
        conn.commit()
        print("✓ Migration completed successfully for MySQL!")
        return True
        
    except Exception as e:
        print(f"❌ Error during MySQL migration: {e}")
        conn.rollback()
        return False


def migrate_postgresql(conn):
    """Add crawl_sequence column to PostgreSQL database"""
    cursor = conn.cursor()
    
    try:
        # Check if column already exists
        cursor.execute("""
            SELECT COUNT(*) 
            FROM information_schema.columns 
            WHERE table_name = 'bus_data' 
            AND column_name = 'crawl_sequence'
        """)
        
        result = cursor.fetchone()
        if result[0] > 0:
            print("✓ Column 'crawl_sequence' already exists in bus_data table")
            return True
        
        # Add the new column
        print("Adding 'crawl_sequence' column to bus_data table...")
        cursor.execute("""
            ALTER TABLE bus_data 
            ADD COLUMN crawl_sequence INTEGER DEFAULT 1
        """)
        
        # Create index for faster queries
        print("Creating index on crawl_sequence...")
        cursor.execute("""
            CREATE INDEX idx_crawl_sequence 
            ON bus_data(route_name, route_date, crawl_sequence)
        """)
        
        conn.commit()
        print("✓ Migration completed successfully for PostgreSQL!")
        return True
        
    except Exception as e:
        print(f"❌ Error during PostgreSQL migration: {e}")
        conn.rollback()
        return False


def update_existing_records(conn, db_type):
    """Update existing records with proper crawl_sequence values"""
    print("\nUpdating existing records with crawl_sequence values...")
    
    cursor = conn.cursor()
    
    try:
        if db_type == 'sqlite':
            # For SQLite, we need to update in a different way
            cursor.execute("""
                SELECT id, route_name, route_date, 
                       DATE(crawl_timestamp) as crawl_date,
                       crawl_timestamp
                FROM bus_data
                ORDER BY route_name, route_date, crawl_timestamp
            """)
            
        elif db_type == 'mysql':
            cursor.execute("""
                SELECT id, route_name, route_date, 
                       DATE(crawl_timestamp) as crawl_date,
                       crawl_timestamp
                FROM bus_data
                ORDER BY route_name, route_date, crawl_timestamp
            """)
            
        elif db_type == 'postgresql':
            cursor.execute("""
                SELECT id, route_name, route_date, 
                       DATE(crawl_timestamp) as crawl_date,
                       crawl_timestamp
                FROM bus_data
                ORDER BY route_name, route_date, crawl_timestamp
            """)
        
        records = cursor.fetchall()
        
        if not records:
            print("✓ No existing records to update")
            return True
        
        # Track crawl sequence per route+date combination
        sequence_tracker = {}
        updates = []
        
        for record in records:
            if db_type == 'mysql':
                # For MySQL with dictionary cursor
                if isinstance(record, dict):
                    record_id = record['id']
                    route_name = record['route_name']
                    route_date = record['route_date']
                    crawl_date = record['crawl_date']
                else:
                    record_id, route_name, route_date, crawl_date, crawl_timestamp = record
            else:
                record_id, route_name, route_date, crawl_date, crawl_timestamp = record
            
            # Create unique key for this route+date+crawl_date combination
            key = f"{route_name}|{route_date}|{crawl_date}"
            
            # Get or initialize sequence number
            if key not in sequence_tracker:
                sequence_tracker[key] = 0
            
            sequence_tracker[key] += 1
            sequence_num = sequence_tracker[key]
            
            updates.append((sequence_num, record_id))
        
        # Batch update
        print(f"Updating {len(updates)} records...")
        
        update_sql = "UPDATE bus_data SET crawl_sequence = ? WHERE id = ?"
        if db_type in ['mysql', 'postgresql']:
            update_sql = update_sql.replace('?', '%s')
        
        cursor.executemany(update_sql, updates)
        conn.commit()
        
        print(f"✓ Updated {len(updates)} records with sequence numbers")
        
        # Show summary
        print("\nCrawl Sequence Summary:")
        for key, max_seq in sorted(sequence_tracker.items()):
            route_name, route_date, crawl_date = key.split('|')
            print(f"  {route_name} on {route_date} (crawled on {crawl_date}): {max_seq} crawls")
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating existing records: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
        return False


def main():
    print("="*70)
    print("MIGRATION: Add crawl_sequence column to bus_data table")
    print("="*70)
    print("\nThis migration adds a new column 'crawl_sequence' to track how many")
    print("times each route has been crawled on the same day.")
    print("\nThe column will be added after 'crawl_timestamp'.")
    print("="*70)
    
    # Load database configuration
    db_config = load_db_config()
    db_type = db_config.get('type', 'sqlite')
    
    print(f"\nDatabase Type: {db_type.upper()}")
    
    if db_type == 'sqlite':
        import sqlite3
        db_path = db_config.get('database', 'data/bus_data.db')
        print(f"Database Path: {db_path}")
        
        try:
            conn = sqlite3.connect(db_path)
            print("✓ Connected to SQLite database")
            
            if migrate_sqlite(conn):
                update_existing_records(conn, db_type)
            
            conn.close()
            print("\n✓ Database connection closed")
            
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    elif db_type == 'mysql':
        try:
            import mysql.connector
            
            print(f"Host: {db_config.get('host')}:{db_config.get('port', 3306)}")
            print(f"Database: {db_config['database']}")
            
            conn = mysql.connector.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 3306),
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            print("✓ Connected to MySQL database")
            
            if migrate_mysql(conn):
                update_existing_records(conn, db_type)
            
            conn.close()
            print("\n✓ Database connection closed")
            
        except ImportError:
            print("❌ MySQL connector not installed. Run: pip install mysql-connector-python")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    elif db_type == 'postgresql':
        try:
            import psycopg2
            
            print(f"Host: {db_config.get('host')}:{db_config.get('port', 5432)}")
            print(f"Database: {db_config['database']}")
            
            conn = psycopg2.connect(
                host=db_config.get('host', 'localhost'),
                port=db_config.get('port', 5432),
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password']
            )
            print("✓ Connected to PostgreSQL database")
            
            if migrate_postgresql(conn):
                update_existing_records(conn, db_type)
            
            conn.close()
            print("\n✓ Database connection closed")
            
        except ImportError:
            print("❌ PostgreSQL connector not installed. Run: pip install psycopg2-binary")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Error: {e}")
            sys.exit(1)
    
    else:
        print(f"❌ Unsupported database type: {db_type}")
        sys.exit(1)
    
    print("\n" + "="*70)
    print("✅ MIGRATION COMPLETED SUCCESSFULLY!")
    print("="*70)
    print("\nNext steps:")
    print("1. The crawl_sequence column has been added to the bus_data table")
    print("2. Existing records have been updated with sequence numbers")
    print("3. New crawls will automatically track the sequence number")
    print("4. Run 'python test_crawl_sequence.py' to verify the migration")
    print("="*70 + "\n")


if __name__ == '__main__':
    main()
