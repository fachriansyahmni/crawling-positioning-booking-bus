"""
Database Management Utility
Run this script to manage your bus crawling database
"""

import argparse
from database import BusDatabase, load_db_config
import os


def show_statistics(db):
    """Display database statistics"""
    print("\n" + "="*60)
    print("DATABASE STATISTICS")
    print("="*60)
    
    stats = db.get_statistics()
    
    print(f"\n📊 Total Records: {stats.get('total_records', 0)}")
    
    if 'by_platform' in stats and stats['by_platform']:
        print("\n📱 Records by Platform:")
        for platform, count in stats['by_platform'].items():
            print(f"   {platform.upper()}: {count:,}")
    
    if 'top_routes' in stats and stats['top_routes']:
        print("\n🚌 Top Routes:")
        for route, count in stats['top_routes'].items():
            print(f"   {route}: {count:,}")
    
    print(f"\n🕐 Latest Crawl: {stats.get('latest_crawl', 'N/A')}")
    print(f"📋 Total Sessions: {stats.get('total_sessions', 0)}")
    print("="*60 + "\n")


def import_data(db, args):
    """Import data from CSV files"""
    if args.file:
        # Import single file
        platform = args.platform or 'auto'
        if platform == 'auto':
            if 'traveloka' in args.file.lower():
                platform = 'traveloka'
            elif 'redbus' in args.file.lower():
                platform = 'redbus'
            else:
                print("❌ Cannot auto-detect platform. Please specify with --platform")
                return
        
        print(f"\n📥 Importing {args.file} as {platform}...")
        stats = db.import_from_csv(args.file, platform)
        print(f"\n✅ Import completed:")
        print(f"   Inserted: {stats['inserted']}")
        print(f"   Duplicates: {stats['duplicates']}")
        print(f"   Errors: {stats['errors']}\n")
        
    elif args.directory:
        # Import directory
        print(f"\n📥 Importing all CSV files from {args.directory}...")
        platform = args.platform if args.platform != 'auto' else None
        stats = db.import_from_directory(args.directory, platform)
        print(f"\n✅ Import completed:")
        print(f"   Files: {stats['files']}")
        print(f"   Inserted: {stats['inserted']}")
        print(f"   Duplicates: {stats['duplicates']}")
        print(f"   Errors: {stats['errors']}\n")


def query_data(db, args):
    """Query and display data"""
    print(f"\n🔍 Querying data...")
    
    df = db.query_data(
        platform=args.platform if args.platform != 'auto' else None,
        route_name=args.route,
        route_date=args.date,
        limit=args.limit
    )
    
    if df.empty:
        print("❌ No data found matching your criteria\n")
        return
    
    print(f"\n✅ Found {len(df)} records:\n")
    
    # Display options
    if args.output:
        df.to_csv(args.output, index=False)
        print(f"✅ Data exported to {args.output}\n")
    else:
        # Display in console
        print(df.to_string(max_rows=20))
        if len(df) > 20:
            print(f"\n... and {len(df) - 20} more rows")
        print()


def export_all(db, args):
    """Export all data to CSV"""
    output_dir = args.output or 'exports'
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\n📤 Exporting all data to {output_dir}/...")
    
    # Export by platform
    for platform in ['traveloka', 'redbus']:
        df = db.query_data(platform=platform, limit=1000000)  # Get all
        if not df.empty:
            output_file = os.path.join(output_dir, f'{platform}_all_data.csv')
            df.to_csv(output_file, index=False)
            print(f"   ✅ {platform}: {len(df)} records -> {output_file}")
    
    print(f"\n✅ Export completed\n")


def interactive_mode(db):
    """Interactive mode for database management"""
    print("\n" + "="*60)
    print("BUS CRAWLER - DATABASE MANAGER (Interactive Mode)")
    print("="*60)
    
    while True:
        print("\nAvailable Commands:")
        print("  1. Show statistics")
        print("  2. Import CSV file")
        print("  3. Import directory")
        print("  4. Query data")
        print("  5. Export all data")
        print("  6. Exit")
        
        choice = input("\nYour choice (1-6): ").strip()
        
        if choice == '1':
            show_statistics(db)
            
        elif choice == '2':
            csv_path = input("CSV file path: ").strip()
            platform = input("Platform (traveloka/redbus/auto): ").strip().lower() or 'auto'
            
            if not os.path.exists(csv_path):
                print("❌ File not found")
                continue
            
            if platform == 'auto':
                if 'traveloka' in csv_path.lower():
                    platform = 'traveloka'
                elif 'redbus' in csv_path.lower():
                    platform = 'redbus'
                else:
                    platform = input("Cannot auto-detect. Specify platform (traveloka/redbus): ").strip().lower()
            
            stats = db.import_from_csv(csv_path, platform)
            print(f"\n✅ Imported: {stats['inserted']}, Duplicates: {stats['duplicates']}, Errors: {stats['errors']}")
            
        elif choice == '3':
            directory = input("Directory path (default: data): ").strip() or 'data'
            platform = input("Platform filter (traveloka/redbus/auto): ").strip().lower() or 'auto'
            
            if not os.path.exists(directory):
                print("❌ Directory not found")
                continue
            
            platform_filter = None if platform == 'auto' else platform
            stats = db.import_from_directory(directory, platform_filter)
            
        elif choice == '4':
            platform = input("Platform (traveloka/redbus/all): ").strip().lower()
            route = input("Route name (or leave empty): ").strip() or None
            date = input("Date YYYY-MM-DD (or leave empty): ").strip() or None
            limit = input("Limit (default: 100): ").strip()
            limit = int(limit) if limit.isdigit() else 100
            
            platform_filter = None if platform == 'all' else platform
            df = db.query_data(platform_filter, route, date, limit)
            
            if df.empty:
                print("❌ No data found")
            else:
                print(f"\n✅ Found {len(df)} records:")
                print(df.to_string(max_rows=20))
                if len(df) > 20:
                    print(f"\n... and {len(df) - 20} more rows")
                
                export = input("\nExport to CSV? (y/n): ").strip().lower()
                if export == 'y':
                    filename = input("Output filename: ").strip()
                    df.to_csv(filename, index=False)
                    print(f"✅ Exported to {filename}")
            
        elif choice == '5':
            output_dir = input("Output directory (default: exports): ").strip() or 'exports'
            os.makedirs(output_dir, exist_ok=True)
            
            for platform in ['traveloka', 'redbus']:
                df = db.query_data(platform=platform, limit=1000000)
                if not df.empty:
                    output_file = os.path.join(output_dir, f'{platform}_all_data.csv')
                    df.to_csv(output_file, index=False)
                    print(f"✅ {platform}: {len(df)} records -> {output_file}")
            
        elif choice == '6':
            print("\n👋 Goodbye!\n")
            break
        
        else:
            print("❌ Invalid choice")


def main():
    parser = argparse.ArgumentParser(
        description='Bus Crawler Database Manager',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Show statistics
  python db_manager.py --stats
  
  # Import single CSV file
  python db_manager.py --import-file data/traveloka/traveloka_Jakarta-Semarang-15.csv --platform traveloka
  
  # Import entire directory (auto-detect platform)
  python db_manager.py --import-dir data/traveloka
  
  # Query data
  python db_manager.py --query --platform traveloka --route "Jakarta-Semarang" --limit 50
  
  # Export all data
  python db_manager.py --export-all --output exports
  
  # Interactive mode
  python db_manager.py --interactive
        """
    )
    
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    parser.add_argument('--import-file', dest='file', help='Import single CSV file')
    parser.add_argument('--import-dir', dest='directory', help='Import all CSV files from directory')
    parser.add_argument('--platform', choices=['traveloka', 'redbus', 'auto'], default='auto',
                       help='Platform filter')
    parser.add_argument('--query', action='store_true', help='Query data')
    parser.add_argument('--route', help='Filter by route name')
    parser.add_argument('--date', help='Filter by date (YYYY-MM-DD)')
    parser.add_argument('--limit', type=int, default=100, help='Limit number of results')
    parser.add_argument('--output', help='Output file/directory for export')
    parser.add_argument('--export-all', action='store_true', help='Export all data')
    parser.add_argument('--interactive', '-i', action='store_true', help='Interactive mode')
    
    args = parser.parse_args()
    
    # Load database configuration
    db_config = load_db_config()
    db = BusDatabase(
        db_type=db_config.get('type', 'sqlite'),
        db_config=db_config
    )
    
    try:
        # Execute commands
        if args.interactive:
            interactive_mode(db)
        elif args.stats:
            show_statistics(db)
        elif args.file or args.directory:
            import_data(db, args)
        elif args.query:
            query_data(db, args)
        elif args.export_all:
            export_all(db, args)
        else:
            # No arguments, show help
            parser.print_help()
    
    finally:
        db.close()


if __name__ == '__main__':
    main()
