"""
Bus Availability Prediction Script
Use trained model to predict future bus availability
"""

import pandas as pd
import sys
from datetime import datetime
from train_bus_prediction_model import BusPredictionModel
from database import BusDatabase, load_db_config


class BusPredictor:
    """Wrapper class for web interface predictions"""
    
    def __init__(self, db_config=None):
        """Initialize predictor with database config"""
        self.db_config = db_config or load_db_config()
        self.model = BusPredictionModel()
        self.db = None
        
    def predict_period(self, period='week'):
        """
        Generate predictions for a period
        
        Args:
            period: 'week', 'month', or 'year'
        
        Returns:
            DataFrame with predictions
        """
        # Map period names
        period_map = {
            'week': 'next_week',
            'month': 'next_month',
            'year': 'next_year'
        }
        prediction_period = period_map.get(period, 'next_week')
        
        # Load model
        if not self.model.load_model('models/bus_prediction_model.pkl'):
            raise Exception("Model not found. Please train the model first.")
        
        # Connect to database
        self.db = BusDatabase(db_type=self.db_config['type'], db_config=self.db_config)
        
        # Load historical data
        df_raw = self.model.load_data_from_database(self.db, days_back=90)
        
        if df_raw.empty:
            raise Exception("No historical data available")
        
        df_features = self.model.prepare_features(df_raw)
        df_agg = self.model.aggregate_daily_data(df_features)
        
        # Make predictions
        pred_df = self.model.predict_future(df_agg, prediction_period=prediction_period)
        
        if pred_df.empty:
            raise Exception("No predictions generated")
        
        return pred_df
    
    def predict_date_range(self, start_date, end_date, route_filter=None):
        """
        Generate predictions for a specific date range (route dates)
        
        Args:
            start_date: Start date (YYYY-MM-DD string or datetime)
            end_date: End date (YYYY-MM-DD string or datetime)
            route_filter: Optional route name to filter predictions
        
        Returns:
            DataFrame with predictions
        """
        from datetime import datetime, timedelta
        import pandas as pd
        
        # Parse dates
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, '%Y-%m-%d')
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Load model
        if not self.model.load_model('models/bus_prediction_model.pkl'):
            raise Exception("Model not found. Please train the model first.")
        
        # Connect to database
        if self.db is None:
            self.db = BusDatabase(db_type=self.db_config['type'], db_config=self.db_config)
        
        # Load historical data
        df_raw = self.model.load_data_from_database(self.db, days_back=90)
        
        if df_raw.empty:
            raise Exception("No historical data available")
        
        # Filter by route if specified
        if route_filter:
            df_raw = df_raw[df_raw['route_name'] == route_filter]
            if df_raw.empty:
                raise Exception(f"No historical data found for route: {route_filter}")
        
        df_features = self.model.prepare_features(df_raw)
        df_agg = self.model.aggregate_daily_data(df_features)
        
        # Generate date range
        dates = pd.date_range(start_date, end_date, freq='D')
        
        # Get unique companies and routes from historical data
        companies = df_agg['bus_name'].unique()
        routes = df_agg['route_name'].unique()
        platforms = df_agg['platform'].unique()
        
        # Filter routes if needed
        if route_filter:
            routes = [r for r in routes if r == route_filter]
        
        predictions = []
        
        for date in dates:
            is_weekend = 1 if date.weekday() >= 5 else 0
            day_of_week = date.weekday()
            month = date.month
            week_of_year = date.isocalendar()[1]
            
            for platform in platforms:
                for route in routes:
                    for company in companies:
                        # Create feature dictionary
                        feature_dict = {
                            'platform': platform,
                            'route_name': route,
                            'bus_name': company,
                            'is_weekend': is_weekend,
                            'day_of_week': day_of_week,
                            'month': month,
                            'week_of_year': week_of_year,
                        }
                        
                        # Get historical averages for this company
                        company_hist = df_agg[df_agg['bus_name'] == company]
                        if not company_hist.empty:
                            feature_dict['avg_price'] = company_hist['avg_price'].mean()
                            feature_dict['min_price'] = company_hist['min_price'].mean()
                            feature_dict['max_price'] = company_hist['max_price'].mean()
                            feature_dict['avg_rating'] = company_hist['avg_rating'].mean()
                            feature_dict['hist_avg_total'] = company_hist['total_buses'].mean()
                            feature_dict['hist_avg_vip'] = company_hist['vip_count'].mean()
                            feature_dict['hist_avg_executive'] = company_hist['executive_count'].mean()
                            feature_dict['hist_avg_price'] = company_hist['avg_price'].mean()
                            feature_dict['hist_avg_departing'] = company_hist['avg_departing_time'].mean()
                            feature_dict['hist_avg_reaching'] = company_hist['avg_reaching_time'].mean()
                        else:
                            # Use overall averages if company not found
                            feature_dict['avg_price'] = df_agg['avg_price'].mean()
                            feature_dict['min_price'] = df_agg['min_price'].mean()
                            feature_dict['max_price'] = df_agg['max_price'].mean()
                            feature_dict['avg_rating'] = df_agg['avg_rating'].mean()
                            feature_dict['hist_avg_total'] = df_agg['total_buses'].mean()
                            feature_dict['hist_avg_vip'] = df_agg['vip_count'].mean()
                            feature_dict['hist_avg_executive'] = df_agg['executive_count'].mean()
                            feature_dict['hist_avg_price'] = df_agg['avg_price'].mean()
                            feature_dict['hist_avg_departing'] = df_agg['avg_departing_time'].mean()
                            feature_dict['hist_avg_reaching'] = df_agg['avg_reaching_time'].mean()
                        
                        # Encode categorical features
                        try:
                            feature_dict['platform_encoded'] = self.model.label_encoders['platform'].transform([platform])[0]
                            feature_dict['route_name_encoded'] = self.model.label_encoders['route_name'].transform([route])[0]
                            feature_dict['bus_name_encoded'] = self.model.label_encoders['bus_name'].transform([company])[0]
                        except:
                            # Skip if unseen category
                            continue
                        
                        # Create feature vector
                        X_pred = pd.DataFrame([feature_dict])[self.model.feature_columns].fillna(0)
                        
                        # Make predictions with error handling for old models
                        pred_total = max(0, round(self.model.models['total'].predict(X_pred)[0]))
                        pred_vip = max(0, round(self.model.models['vip'].predict(X_pred)[0]))
                        pred_executive = max(0, round(self.model.models['executive'].predict(X_pred)[0]))
                        
                        # Try to predict new features (time and price)
                        try:
                            pred_departing = max(0, round(self.model.models['departing_time'].predict(X_pred)[0]))
                        except (KeyError, AttributeError):
                            pred_departing = None  # Old model doesn't have this
                        
                        try:
                            pred_reaching = max(0, round(self.model.models['reaching_time'].predict(X_pred)[0]))
                        except (KeyError, AttributeError):
                            pred_reaching = None  # Old model doesn't have this
                        
                        try:
                            pred_price = max(0, round(self.model.models['price'].predict(X_pred)[0]))
                        except (KeyError, AttributeError):
                            pred_price = None  # Old model doesn't have this
                        
                        # Convert time predictions from minutes to HH:MM format
                        def minutes_to_time(minutes):
                            """Convert minutes since midnight to HH:MM format"""
                            if minutes is None:
                                return None
                            hours = int(minutes // 60)
                            mins = int(minutes % 60)
                            return f"{hours:02d}:{mins:02d}"
                        
                        predictions.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'day_name': date.strftime('%A'),
                            'is_weekend': 'Weekend' if is_weekend else 'Weekday',
                            'platform': platform,
                            'route_name': route,
                            'bus_name': company,
                            'predicted_total': pred_total,
                            'predicted_vip': pred_vip,
                            'predicted_executive': pred_executive,
                            'predicted_other': max(0, pred_total - pred_vip - pred_executive),
                            'predicted_departing_time': minutes_to_time(pred_departing) if pred_departing else None,
                            'predicted_reaching_time': minutes_to_time(pred_reaching) if pred_reaching else None,
                            'predicted_price': pred_price
                        })
        
        pred_df = pd.DataFrame(predictions)
        
        if pred_df.empty:
            raise Exception("No predictions generated")
        
        return pred_df
    
    def predict_custom_days(self, days=7, route_filter=None):
        """
        Generate predictions for custom number of days from tomorrow
        
        Args:
            days: Number of days to predict (1-365)
            route_filter: Optional route name to filter predictions
        
        Returns:
            DataFrame with predictions
        """
        from datetime import datetime, timedelta
        
        # Load model
        if not self.model.load_model('models/bus_prediction_model.pkl'):
            raise Exception("Model not found. Please train the model first.")
        
        # Connect to database
        if self.db is None:
            self.db = BusDatabase(db_type=self.db_config['type'], db_config=self.db_config)
        
        # Load historical data
        df_raw = self.model.load_data_from_database(self.db, days_back=90)
        
        if df_raw.empty:
            raise Exception("No historical data available")
        
        # Filter by route if specified
        if route_filter:
            df_raw = df_raw[df_raw['route_name'] == route_filter]
            if df_raw.empty:
                raise Exception(f"No historical data found for route: {route_filter}")
        
        df_features = self.model.prepare_features(df_raw)
        df_agg = self.model.aggregate_daily_data(df_features)
        
        # Generate date range
        today = datetime.now()
        dates = [today + timedelta(days=i) for i in range(1, days + 1)]
        
        # Get unique companies and routes from historical data
        companies = df_agg['bus_name'].unique()
        routes = df_agg['route_name'].unique()
        platforms = df_agg['platform'].unique()
        
        # Filter routes if needed
        if route_filter:
            routes = [r for r in routes if r == route_filter]
        
        predictions = []
        
        for date in dates:
            is_weekend = 1 if date.weekday() >= 5 else 0
            day_of_week = date.weekday()
            month = date.month
            week_of_year = date.isocalendar()[1]
            
            for platform in platforms:
                for route in routes:
                    for company in companies:
                        # Create feature dictionary
                        feature_dict = {
                            'platform': platform,
                            'route_name': route,
                            'bus_name': company,
                            'is_weekend': is_weekend,
                            'day_of_week': day_of_week,
                            'month': month,
                            'week_of_year': week_of_year,
                        }
                        
                        # Get historical averages for this company
                        company_hist = df_agg[df_agg['bus_name'] == company]
                        if not company_hist.empty:
                            feature_dict['avg_price'] = company_hist['avg_price'].mean()
                            feature_dict['min_price'] = company_hist['min_price'].mean()
                            feature_dict['max_price'] = company_hist['max_price'].mean()
                            feature_dict['avg_rating'] = company_hist['avg_rating'].mean()
                            feature_dict['hist_avg_total'] = company_hist['total_buses'].mean()
                            feature_dict['hist_avg_vip'] = company_hist['vip_count'].mean()
                            feature_dict['hist_avg_executive'] = company_hist['executive_count'].mean()
                            feature_dict['hist_avg_price'] = company_hist['avg_price'].mean()
                            feature_dict['hist_avg_departing'] = company_hist['avg_departing_time'].mean()
                            feature_dict['hist_avg_reaching'] = company_hist['avg_reaching_time'].mean()
                        else:
                            # Use overall averages if company not found
                            feature_dict['avg_price'] = df_agg['avg_price'].mean()
                            feature_dict['min_price'] = df_agg['min_price'].mean()
                            feature_dict['max_price'] = df_agg['max_price'].mean()
                            feature_dict['avg_rating'] = df_agg['avg_rating'].mean()
                            feature_dict['hist_avg_total'] = df_agg['total_buses'].mean()
                            feature_dict['hist_avg_vip'] = df_agg['vip_count'].mean()
                            feature_dict['hist_avg_executive'] = df_agg['executive_count'].mean()
                            feature_dict['hist_avg_price'] = df_agg['avg_price'].mean()
                            feature_dict['hist_avg_departing'] = df_agg['avg_departing_time'].mean()
                            feature_dict['hist_avg_reaching'] = df_agg['avg_reaching_time'].mean()
                        
                        # Encode categorical features
                        try:
                            feature_dict['platform_encoded'] = self.model.label_encoders['platform'].transform([platform])[0]
                            feature_dict['route_name_encoded'] = self.model.label_encoders['route_name'].transform([route])[0]
                            feature_dict['bus_name_encoded'] = self.model.label_encoders['bus_name'].transform([company])[0]
                        except:
                            # Skip if unseen category
                            continue
                        
                        # Create feature vector using model's feature columns
                        X_pred = pd.DataFrame([feature_dict])[self.model.feature_columns].fillna(0)
                        
                        # Make predictions
                        pred_total = max(0, round(self.model.models['total'].predict(X_pred)[0]))
                        pred_vip = max(0, round(self.model.models['vip'].predict(X_pred)[0]))
                        pred_executive = max(0, round(self.model.models['executive'].predict(X_pred)[0]))
                        
                        # New predictions (with fallback for old models)
                        try:
                            pred_departing = max(0, round(self.model.models['departing_time'].predict(X_pred)[0]))
                        except (KeyError, AttributeError):
                            pred_departing = None  # Model not trained yet
                        
                        try:
                            pred_reaching = max(0, round(self.model.models['reaching_time'].predict(X_pred)[0]))
                        except (KeyError, AttributeError):
                            pred_reaching = None  # Model not trained yet
                        
                        try:
                            pred_price = max(0, round(self.model.models['price'].predict(X_pred)[0]))
                        except (KeyError, AttributeError):
                            pred_price = None  # Model not trained yet
                        
                        # Convert time predictions from minutes to HH:MM format
                        def minutes_to_time(minutes):
                            """Convert minutes since midnight to HH:MM format"""
                            if minutes is None:
                                return None
                            hours = int(minutes // 60)
                            mins = int(minutes % 60)
                            return f"{hours:02d}:{mins:02d}"
                        
                        predictions.append({
                            'date': date.strftime('%Y-%m-%d'),
                            'day_name': date.strftime('%A'),
                            'is_weekend': 'Weekend' if is_weekend else 'Weekday',
                            'platform': platform,
                            'route_name': route,
                            'bus_name': company,
                            'predicted_total': pred_total,
                            'predicted_vip': pred_vip,
                            'predicted_executive': pred_executive,
                            'predicted_other': max(0, pred_total - pred_vip - pred_executive),
                            'predicted_departing_time': minutes_to_time(pred_departing) if pred_departing else None,
                            'predicted_reaching_time': minutes_to_time(pred_reaching) if pred_reaching else None,
                            'predicted_price': pred_price
                        })
        
        pred_df = pd.DataFrame(predictions)
        
        if pred_df.empty:
            raise Exception("No predictions generated")
        
        return pred_df
    
    def save_predictions(self, predictions_df, period='week'):
        """
        Save predictions to database
        
        Args:
            predictions_df: DataFrame with predictions
            period: Period name for the session
        
        Returns:
            session_id: Unique session identifier
        """
        if self.db is None:
            self.db = BusDatabase(db_type=self.db_config['type'], db_config=self.db_config)
        
        # Get date range from predictions
        start_date = predictions_df['date'].min()
        end_date = predictions_df['date'].max()
        
        # Map period names to database format
        period_map = {
            'week': 'next_week',
            'month': 'next_month',
            'year': 'next_year'
        }
        prediction_period = period_map.get(period, period)
        
        # Get model version and training info
        model_version = datetime.now().strftime('%Y%m%d_%H%M%S')
        training_days = 90  # Default from model
        
        # Create prediction session FIRST (required for foreign key)
        session_id = self.db.create_prediction_session(
            prediction_period=prediction_period,
            start_date=start_date,
            end_date=end_date,
            model_version=model_version,
            training_days=training_days
        )
        
        # Now save predictions with the session_id
        inserted_count = self.db.save_predictions(session_id, predictions_df)
        
        return session_id


def print_table(summary_df, title=""):
    """Print formatted table like your requested format"""
    if summary_df.empty:
        print("No data available")
        return
    
    # Get companies (columns will be companies)
    companies = summary_df.index.tolist()
    
    # Print title
    if title:
        print(f"\n{title}")
    
    # Print separator
    sep_length = 20 + len(companies) * 15
    print("-" * sep_length)
    
    # Print header
    header = f"| {'Category':<18} |"
    for company in companies:
        header += f" {company[:12]:<12} |"
    print(header)
    print("-" * sep_length)
    
    # Print BUS TOTAL row
    row = f"| {'BUS TOTAL':<18} |"
    for company in companies:
        total = summary_df.loc[company, 'Total']
        row += f" {int(total):>12} |"
    print(row)
    print("-" * sep_length)
    
    # Print VIP row
    row = f"| {'VIP (VIP types)':<18} |"
    for company in companies:
        vip = summary_df.loc[company, 'VIP']
        row += f" {int(vip):>12} |"
    print(row)
    print("-" * sep_length)
    
    # Print EXECUTIVE row
    row = f"| {'EXECUTIVE (EKS)':<18} |"
    for company in companies:
        eks = summary_df.loc[company, 'Executive']
        row += f" {int(eks):>12} |"
    print(row)
    print("-" * sep_length)
    
    # Print OTHER row
    row = f"| {'OTHER':<18} |"
    for company in companies:
        other = summary_df.loc[company, 'Other']
        row += f" {int(other):>12} |"
    print(row)
    print("-" * sep_length)


def predict_and_display(model, df_historical, period='next_week', route=None, show_details=False, save_to_db=True, db=None):
    """Make predictions and display in table format"""
    
    print(f"\n{'='*70}")
    print(f"PREDICTION FOR: {period.replace('_', ' ').upper()}")
    print(f"{'='*70}")
    
    # Make predictions
    pred_df = model.predict_future(df_historical, prediction_period=period)
    
    if pred_df.empty:
        print("❌ No predictions generated")
        return None
    
    # Save to database if requested
    session_id = None
    if save_to_db and db:
        try:
            # Get date range
            start_date = pred_df['date'].min()
            end_date = pred_df['date'].max()
            
            # Get model info
            model_version = datetime.now().strftime('%Y%m%d_%H%M%S')
            training_days = 90  # Default from model
            
            # Create prediction session
            session_id = db.create_prediction_session(
                prediction_period=period,
                start_date=start_date,
                end_date=end_date,
                model_version=model_version,
                training_days=training_days
            )
            
            # Save predictions
            db.save_predictions(session_id, pred_df)
            
            print(f"\n💾 Saved predictions to database (Session ID: {session_id})")
            
        except Exception as e:
            print(f"\n⚠️  Failed to save predictions to database: {e}")
    
    # Filter by route if specified
    if route:
        pred_df = pred_df[pred_df['route_name'] == route]
        print(f"\n🛣️  Route: {route}")
    
    # Create summary tables
    summary_tables = model.create_summary_table(pred_df, group_by_weekend=True)
    
    # Display tables
    for period_type, summary in summary_tables.items():
        if not summary.empty:
            print_table(summary, title=f"\n📊 {period_type.upper()}")
    
    # Show detailed predictions if requested
    if show_details:
        print(f"\n\n📅 DETAILED DAY-BY-DAY PREDICTIONS:")
        print("="*70)
        
        for date in pred_df['date'].unique()[:14]:  # Show first 2 weeks
            date_data = pred_df[pred_df['date'] == date]
            day_name = date_data.iloc[0]['day_name']
            is_weekend = date_data.iloc[0]['is_weekend']
            
            print(f"\n{date} ({day_name}) - {is_weekend}")
            print("-"*70)
            
            for _, row in date_data.iterrows():
                print(f"  {row['bus_name']:<20} | "
                      f"Total: {int(row['predicted_total']):>2} | "
                      f"VIP: {int(row['predicted_vip']):>2} | "
                      f"Exec: {int(row['predicted_executive']):>2} | "
                      f"Other: {int(row['predicted_other']):>2}")
    
    # Statistics
    print(f"\n\n📈 PREDICTION STATISTICS:")
    print("="*70)
    
    avg_by_company = pred_df.groupby('bus_name').agg({
        'predicted_total': ['mean', 'min', 'max'],
        'predicted_vip': 'mean',
        'predicted_executive': 'mean'
    }).round(1)
    
    print("\nAverage buses per company:")
    for company in avg_by_company.index:
        total_mean = avg_by_company.loc[company, ('predicted_total', 'mean')]
        total_min = avg_by_company.loc[company, ('predicted_total', 'min')]
        total_max = avg_by_company.loc[company, ('predicted_total', 'max')]
        vip_mean = avg_by_company.loc[company, ('predicted_vip', 'mean')]
        exec_mean = avg_by_company.loc[company, ('predicted_executive', 'mean')]
        
        print(f"  {company:<20}: "
              f"Total: {total_mean:.1f} (range: {total_min:.0f}-{total_max:.0f}), "
              f"VIP: {vip_mean:.1f}, Exec: {exec_mean:.1f}")
    
    # Weekday vs Weekend comparison
    print("\n\nWeekday vs Weekend Comparison:")
    weekday_avg = pred_df[pred_df['is_weekend'] == 'Weekday']['predicted_total'].mean()
    weekend_avg = pred_df[pred_df['is_weekend'] == 'Weekend']['predicted_total'].mean()
    
    print(f"  Average buses on Weekday: {weekday_avg:.1f}")
    print(f"  Average buses on Weekend: {weekend_avg:.1f}")
    
    if weekend_avg > weekday_avg:
        diff_pct = ((weekend_avg - weekday_avg) / weekday_avg) * 100
        print(f"  📈 Weekends have {diff_pct:.1f}% more buses")
    elif weekday_avg > weekend_avg:
        diff_pct = ((weekday_avg - weekend_avg) / weekend_avg) * 100
        print(f"  📈 Weekdays have {diff_pct:.1f}% more buses")
    else:
        print(f"  ≈ Similar availability")


def main():
    """Main prediction interface"""
    
    print("="*70)
    print("BUS AVAILABILITY PREDICTION")
    print("="*70)
    
    # Load model
    model = BusPredictionModel()
    
    if not model.load_model('models/bus_prediction_model.pkl'):
        print("\n❌ Model not found. Please train the model first:")
        print("   python train_bus_prediction_model.py")
        return
    
    # Show training statistics
    if model.training_stats:
        print("\n📊 Model Performance:")
        for target, stats in model.training_stats.items():
            print(f"   {target.upper()}: Test MAE = {stats['test_mae']:.2f}, Test R² = {stats['test_r2']:.3f}")
    
    # Load historical data for prediction
    try:
        db_config = load_db_config()
        db = BusDatabase(db_type=db_config['type'], db_config=db_config)
        print(f"\n✅ Connected to database")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
    
    try:
        # Load historical data
        df_raw = model.load_data_from_database(db, days_back=90)
        
        if df_raw.empty:
            print("\n❌ No historical data available")
            return
        
        df_features = model.prepare_features(df_raw)
        df_agg = model.aggregate_daily_data(df_features)
        
        # Command line arguments
        if len(sys.argv) > 1:
            period = sys.argv[1]  # next_week, next_month, next_year
            route = sys.argv[2] if len(sys.argv) > 2 else None
            show_details = '--details' in sys.argv
        else:
            # Interactive mode
            print("\n" + "="*70)
            print("PREDICTION OPTIONS")
            print("="*70)
            print("1. Next Week")
            print("2. Next Month")
            print("3. Next Year")
            print("4. All periods")
            
            choice = input("\nSelect prediction period (1-4): ").strip()
            
            period_map = {
                '1': 'next_week',
                '2': 'next_month',
                '3': 'next_year',
                '4': 'all'
            }
            
            period = period_map.get(choice, 'next_week')
            route = None
            
            show_details_input = input("Show detailed day-by-day predictions? (y/n): ").strip().lower()
            show_details = show_details_input == 'y'
        
        # Make predictions
        session_ids = []
        if period == 'all':
            for p in ['next_week', 'next_month', 'next_year']:
                sid = predict_and_display(model, df_agg, period=p, route=route, show_details=show_details, save_to_db=True, db=db)
                if sid:
                    session_ids.append(sid)
        else:
            sid = predict_and_display(model, df_agg, period=period, route=route, show_details=show_details, save_to_db=True, db=db)
            if sid:
                session_ids.append(sid)
        
        print("\n" + "="*70)
        print("✅ PREDICTION COMPLETED")
        print("="*70)
        
        if session_ids:
            print(f"\n📝 Prediction sessions saved: {', '.join(map(str, session_ids))}")
            print("   You can view them later using: python view_predictions.py")
        
    except Exception as e:
        print(f"\n❌ Error during prediction: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()


if __name__ == "__main__":
    main()
