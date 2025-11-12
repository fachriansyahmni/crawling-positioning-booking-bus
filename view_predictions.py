"""
View Historical Bus Predictions
=================================
View saved predictions and their accuracy
"""

import sys
import pandas as pd
from datetime import datetime
from database import BusDatabase

def format_date(dt):
    """Format datetime for display"""
    if pd.isna(dt):
        return "N/A"
    if isinstance(dt, str):
        return dt
    return dt.strftime("%Y-%m-%d %H:%M")

def print_sessions(db):
    """Print all prediction sessions"""
    sessions = db.get_prediction_sessions(limit=50)
    
    if not sessions:
        print("\n📋 No prediction sessions found")
        return
    
    print("\n" + "="*100)
    print("📋 PREDICTION SESSIONS")
    print("="*100)
    print(f"{'ID':<5} {'Date':<20} {'Period':<15} {'Route':<25} {'Training Days':<15}")
    print("-"*100)
    
    for session in sessions:
        sid = session['session_id']
        created = format_date(session['created_at'])
        period = session['prediction_period']
        route = session.get('route', 'All Routes')
        training_days = session.get('training_days', 'N/A')
        
        print(f"{sid:<5} {created:<20} {period:<15} {route:<25} {training_days:<15}")
    
    print("-"*100)
    print(f"Total: {len(sessions)} sessions\n")

def print_predictions(db, session_id):
    """Print predictions for a session"""
    predictions = db.get_predictions(session_id=session_id)
    
    if not predictions:
        print(f"\n📋 No predictions found for session {session_id}")
        return
    
    # Convert to DataFrame for easier display
    df = pd.DataFrame(predictions)
    
    print(f"\n" + "="*120)
    print(f"📊 PREDICTIONS FOR SESSION {session_id}")
    print("="*120)
    
    # Group by company
    for company in df['company_name'].unique():
        company_df = df[df['company_name'] == company].copy()
        
        print(f"\n🚌 {company}")
        print("-"*120)
        print(f"{'Date':<12} {'Cat':<10} {'Pred Total':<12} {'Pred VIP':<10} {'Pred Exec':<12} {'Pred Other':<12} {'Actual':<10} {'Accuracy':<10}")
        print("-"*120)
        
        for _, row in company_df.iterrows():
            date = row['prediction_date'].strftime("%Y-%m-%d") if isinstance(row['prediction_date'], datetime) else row['prediction_date']
            cat = row['bus_category']
            pred_total = f"{row['predicted_total']:.1f}"
            pred_vip = f"{row['predicted_vip']:.1f}"
            pred_exec = f"{row['predicted_executive']:.1f}"
            pred_other = f"{row['predicted_other']:.1f}"
            
            # Check if actual values exist
            if pd.notna(row.get('actual_total')):
                actual = f"{row['actual_total']:.0f}"
            else:
                actual = "-"
            
            # Check if accuracy exists
            if pd.notna(row.get('accuracy_score')):
                accuracy = f"{row['accuracy_score']:.1%}"
            else:
                accuracy = "-"
            
            print(f"{date:<12} {cat:<10} {pred_total:<12} {pred_vip:<10} {pred_exec:<12} {pred_other:<12} {actual:<10} {accuracy:<10}")
    
    print("-"*120)
    print(f"Total predictions: {len(df)}\n")

def print_accuracy(db, session_id):
    """Print accuracy metrics for a session"""
    accuracy = db.get_prediction_accuracy(session_id=session_id)
    
    if not accuracy:
        print(f"\n📊 No accuracy data available for session {session_id}")
        print("   (Actual values haven't been recorded yet)")
        return
    
    print(f"\n" + "="*80)
    print(f"📈 ACCURACY METRICS FOR SESSION {session_id}")
    print("="*80)
    
    for metric in accuracy:
        company = metric['company_name']
        cat = metric['bus_category']
        total_preds = metric['total_predictions']
        with_actuals = metric['predictions_with_actuals']
        
        print(f"\n🚌 {company} - {cat}")
        print(f"   Total Predictions: {total_preds}")
        print(f"   With Actual Data: {with_actuals}")
        
        if with_actuals > 0:
            mae_total = metric.get('mae_total')
            mae_vip = metric.get('mae_vip')
            mae_exec = metric.get('mae_executive')
            mae_other = metric.get('mae_other')
            avg_accuracy = metric.get('avg_accuracy')
            
            print(f"\n   Mean Absolute Error:")
            if mae_total is not None:
                print(f"      Total Buses: {mae_total:.2f}")
            if mae_vip is not None:
                print(f"      VIP:         {mae_vip:.2f}")
            if mae_exec is not None:
                print(f"      Executive:   {mae_exec:.2f}")
            if mae_other is not None:
                print(f"      Other:       {mae_other:.2f}")
            
            if avg_accuracy is not None:
                print(f"\n   Average Accuracy: {avg_accuracy:.1%}")
        else:
            print("   (No actual data recorded yet)")
    
    print("-"*80 + "\n")

def compare_predictions(db, date_str):
    """Compare predictions vs actual for a specific date"""
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        print(f"\n❌ Invalid date format: {date_str}")
        print("   Use format: YYYY-MM-DD")
        return
    
    predictions = db.get_predictions(prediction_date=target_date)
    
    if not predictions:
        print(f"\n📋 No predictions found for {date_str}")
        return
    
    df = pd.DataFrame(predictions)
    
    print(f"\n" + "="*100)
    print(f"📊 PREDICTIONS vs ACTUAL for {date_str}")
    print("="*100)
    
    for company in df['company_name'].unique():
        company_df = df[df['company_name'] == company]
        
        print(f"\n🚌 {company}")
        print("-"*100)
        print(f"{'Category':<15} {'Predicted':<12} {'Actual':<12} {'Difference':<15} {'Accuracy':<10}")
        print("-"*100)
        
        for _, row in company_df.iterrows():
            cat = row['bus_category']
            pred = row['predicted_total']
            
            if pd.notna(row.get('actual_total')):
                actual = row['actual_total']
                diff = actual - pred
                acc = row.get('accuracy_score', 0) * 100
                
                print(f"{cat:<15} {pred:<12.1f} {actual:<12.0f} {diff:+.1f} ({diff/pred*100:+.1f}%){'':<2} {acc:.1f}%")
            else:
                print(f"{cat:<15} {pred:<12.1f} {'N/A':<12} {'-':<15} {'-':<10}")
    
    print("-"*100 + "\n")

def main():
    """Main function"""
    print("\n" + "="*80)
    print("🔍 VIEW HISTORICAL BUS PREDICTIONS")
    print("="*80)
    
    # Initialize database
    db = BusDatabase()
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "list":
            # List all sessions
            print_sessions(db)
            
        elif command == "view" and len(sys.argv) > 2:
            # View specific session
            try:
                session_id = int(sys.argv[2])
                print_predictions(db, session_id)
                print_accuracy(db, session_id)
            except ValueError:
                print(f"\n❌ Invalid session ID: {sys.argv[2]}")
                
        elif command == "compare" and len(sys.argv) > 2:
            # Compare predictions vs actual for date
            date_str = sys.argv[2]
            compare_predictions(db, date_str)
            
        else:
            print("\n❌ Invalid command")
            print_usage()
    else:
        # Interactive mode
        interactive_mode(db)
    
    db.close()

def print_usage():
    """Print usage instructions"""
    print("\nUsage:")
    print("  python view_predictions.py list")
    print("    - List all prediction sessions")
    print()
    print("  python view_predictions.py view <session_id>")
    print("    - View predictions for a specific session")
    print()
    print("  python view_predictions.py compare <date>")
    print("    - Compare predictions vs actual for a date (YYYY-MM-DD)")
    print()
    print("  python view_predictions.py")
    print("    - Interactive mode")
    print()

def interactive_mode(db):
    """Interactive mode"""
    while True:
        print("\n" + "="*50)
        print("Select an option:")
        print("  1. List all sessions")
        print("  2. View session details")
        print("  3. Compare predictions vs actual")
        print("  4. Exit")
        print("="*50)
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == "1":
            print_sessions(db)
            
        elif choice == "2":
            try:
                session_id = int(input("\nEnter session ID: ").strip())
                print_predictions(db, session_id)
                print_accuracy(db, session_id)
            except ValueError:
                print("\n❌ Invalid session ID")
                
        elif choice == "3":
            date_str = input("\nEnter date (YYYY-MM-DD): ").strip()
            compare_predictions(db, date_str)
            
        elif choice == "4":
            print("\n👋 Goodbye!")
            break
            
        else:
            print("\n❌ Invalid choice")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Interrupted by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
