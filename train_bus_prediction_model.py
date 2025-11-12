"""
Bus Availability Prediction Model
Predicts bus counts by company and category based on historical crawling data
Supports predictions for: next week, next month, next year
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sklearn.model_selection import train_test_split, TimeSeriesSplit
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import joblib
import json
import os
from database import BusDatabase, load_db_config
import warnings
warnings.filterwarnings('ignore')

class BusPredictionModel:
    """Machine Learning model for bus availability prediction"""
    
    def __init__(self):
        self.models = {
            'total': None,
            'vip': None,
            'executive': None,
            'departing_time': None,
            'reaching_time': None,
            'price': None
        }
        self.label_encoders = {
            'platform': LabelEncoder(),
            'route_name': LabelEncoder(),
            'bus_name': LabelEncoder(),
            'day_of_week': LabelEncoder()
        }
        self.feature_columns = []
        self.training_stats = {}
        
    def load_data_from_database(self, db, days_back=90, platform=None, route_name=None):
        """
        Load historical data from database
        
        Args:
            db: BusDatabase instance
            days_back: Number of days to look back (default 90)
            platform: Filter by platform (optional)
            route_name: Filter by route (optional)
        
        Returns:
            DataFrame with crawling data
        """
        print(f"📊 Loading data from database (last {days_back} days)...")
        
        # Build query
        where_clauses = []
        params = []
        
        # Date filter
        cutoff_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        where_clauses.append("crawl_timestamp >= ?")
        params.append(cutoff_date)
        
        if platform:
            where_clauses.append("platform = ?")
            params.append(platform)
        
        if route_name:
            where_clauses.append("route_name = ?")
            params.append(route_name)
        
        where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"
        
        sql = f"""
        SELECT 
            platform, route_name, route_date, bus_name, bus_type,
            departing_time, reaching_time, duration, price, star_rating, seat_availability,
            crawl_timestamp
        FROM bus_data
        WHERE {where_sql}
        ORDER BY crawl_timestamp
        """
        
        if db.db_type in ['mysql', 'postgresql']:
            sql = sql.replace('?', '%s')
        
        try:
            db.cursor.execute(sql, tuple(params))
            rows = db.cursor.fetchall()
            
            if not rows:
                print("⚠️  No data found in the specified time range")
                return pd.DataFrame()
            
            # Convert to DataFrame
            if isinstance(rows[0], dict):
                df = pd.DataFrame(rows)
            else:
                columns = ['platform', 'route_name', 'route_date', 'bus_name', 'bus_type',
                          'departing_time', 'reaching_time', 'duration', 'price', 'star_rating', 
                          'seat_availability', 'crawl_timestamp']
                df = pd.DataFrame(rows, columns=columns)
            
            print(f"✅ Loaded {len(df):,} records from database")
            return df
            
        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return pd.DataFrame()
    
    def prepare_features(self, df):
        """
        Prepare features for machine learning
        
        Extracts temporal features and categorizes bus types
        """
        print("\n🔧 Preparing features...")
        
        if df.empty:
            print("❌ No data to prepare")
            return df
        
        df = df.copy()
        
        # Convert crawl_timestamp to datetime
        df['crawl_timestamp'] = pd.to_datetime(df['crawl_timestamp'])
        df['route_date'] = pd.to_datetime(df['route_date'])
        
        # Extract temporal features
        df['year'] = df['crawl_timestamp'].dt.year
        df['month'] = df['crawl_timestamp'].dt.month
        df['day'] = df['crawl_timestamp'].dt.day
        df['day_of_week'] = df['crawl_timestamp'].dt.dayofweek  # 0=Monday, 6=Sunday
        df['week_of_year'] = df['crawl_timestamp'].dt.isocalendar().week
        df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)  # Saturday, Sunday
        df['hour'] = df['crawl_timestamp'].dt.hour
        
        # Days until travel
        df['days_until_travel'] = (df['route_date'] - df['crawl_timestamp']).dt.days
        
        # Categorize bus types
        def categorize_bus_type(bus_type):
            if pd.isna(bus_type):
                return 'UNKNOWN'
            bus_type_lower = str(bus_type).lower()
            if any(x in bus_type_lower for x in ['vip', '2+2', '2-2']):
                return 'VIP'
            elif any(x in bus_type_lower for x in ['executive', 'eksekutif', 'eks']):
                return 'EXECUTIVE'
            elif any(x in bus_type_lower for x in ['economy', 'ekonomi']):
                return 'ECONOMY'
            else:
                return 'OTHER'
        
        df['bus_category'] = df['bus_type'].apply(categorize_bus_type)
        
        # Clean price
        df['price'] = pd.to_numeric(df['price'], errors='coerce').fillna(0)
        df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce').fillna(0)
        
        # Keep time columns (departing_time, reaching_time) - these are strings like "14:30"
        # They will be converted to minutes in aggregate_daily_data()
        
        print(f"✅ Features prepared: {len(df.columns)} columns")
        print(f"   Date range: {df['crawl_timestamp'].min()} to {df['crawl_timestamp'].max()}")
        print(f"   Platforms: {df['platform'].unique()}")
        print(f"   Routes: {df['route_name'].nunique()}")
        print(f"   Bus companies: {df['bus_name'].nunique()}")
        print(f"   Bus categories: {df['bus_category'].value_counts().to_dict()}")
        
        return df
    
    def aggregate_daily_data(self, df):
        """
        Aggregate data by day, route, company, and category
        Creates training samples for prediction
        """
        print("\n📈 Aggregating daily data...")
        
        if df.empty:
            return pd.DataFrame()
        
        # Group by date, route, company, and category
        agg_data = []
        
        for (platform, route, date, is_weekend), group in df.groupby(
            ['platform', 'route_name', 'route_date', 'is_weekend']
        ):
            # Get unique crawl timestamps for this date
            crawl_times = group['crawl_timestamp'].unique()
            
            # For each crawl session
            for crawl_time in crawl_times:
                session_data = group[group['crawl_timestamp'] == crawl_time]
                
                # Aggregate by company
                for bus_name, company_group in session_data.groupby('bus_name'):
                    # Helper function to convert time string to minutes
                    def time_to_minutes(time_str):
                        """Convert time string (HH:MM) to minutes since midnight"""
                        try:
                            if pd.isna(time_str):
                                return None
                            time_str = str(time_str).strip()
                            if ':' in time_str:
                                hours, minutes = time_str.split(':')[:2]
                                return int(hours) * 60 + int(minutes)
                            return None
                        except:
                            return None
                    
                    # Convert time columns to minutes
                    departing_minutes = company_group['departing_time'].apply(time_to_minutes)
                    reaching_minutes = company_group['reaching_time'].apply(time_to_minutes)
                    
                    row = {
                        'platform': platform,
                        'route_name': route,
                        'route_date': date,
                        'crawl_timestamp': crawl_time,
                        'bus_name': bus_name,
                        'is_weekend': is_weekend,
                        'day_of_week': pd.to_datetime(date).dayofweek,
                        'month': pd.to_datetime(date).month,
                        'week_of_year': pd.to_datetime(date).isocalendar().week,
                        
                        # Total buses
                        'total_buses': len(company_group),
                        
                        # By category
                        'vip_count': len(company_group[company_group['bus_category'] == 'VIP']),
                        'executive_count': len(company_group[company_group['bus_category'] == 'EXECUTIVE']),
                        'economy_count': len(company_group[company_group['bus_category'] == 'ECONOMY']),
                        'other_count': len(company_group[company_group['bus_category'] == 'OTHER']),
                        
                        # Average metrics
                        'avg_price': company_group['price'].mean(),
                        'min_price': company_group['price'].min(),
                        'max_price': company_group['price'].max(),
                        'avg_rating': company_group['star_rating'].mean(),
                        
                        # Time metrics (in minutes since midnight)
                        'avg_departing_time': departing_minutes.mean() if departing_minutes.notna().any() else None,
                        'min_departing_time': departing_minutes.min() if departing_minutes.notna().any() else None,
                        'max_departing_time': departing_minutes.max() if departing_minutes.notna().any() else None,
                        'avg_reaching_time': reaching_minutes.mean() if reaching_minutes.notna().any() else None,
                        'min_reaching_time': reaching_minutes.min() if reaching_minutes.notna().any() else None,
                        'max_reaching_time': reaching_minutes.max() if reaching_minutes.notna().any() else None,
                    }
                    agg_data.append(row)
        
        agg_df = pd.DataFrame(agg_data)
        
        print(f"✅ Aggregated to {len(agg_df):,} daily samples")
        print(f"   Unique dates: {agg_df['route_date'].nunique()}")
        print(f"   Unique companies: {agg_df['bus_name'].nunique()}")
        
        return agg_df
    
    def create_features_for_training(self, df):
        """
        Create feature matrix for ML training
        """
        print("\n🎯 Creating feature matrix...")
        
        if df.empty:
            return None, None
        
        df = df.copy()
        
        # Encode categorical variables
        categorical_cols = ['platform', 'route_name', 'bus_name']
        for col in categorical_cols:
            if col in df.columns:
                df[f'{col}_encoded'] = self.label_encoders[col].fit_transform(df[col].astype(str))
        
        # Select features
        feature_cols = [
            'platform_encoded', 'route_name_encoded', 'bus_name_encoded',
            'is_weekend', 'day_of_week', 'month', 'week_of_year',
            'avg_price', 'min_price', 'max_price', 'avg_rating'
        ]
        
        # Add historical averages (if enough data)
        if len(df) > 10:
            # Calculate rolling averages by company
            for bus_name in df['bus_name'].unique():
                mask = df['bus_name'] == bus_name
                df.loc[mask, 'hist_avg_total'] = df.loc[mask, 'total_buses'].expanding().mean().shift(1)
                df.loc[mask, 'hist_avg_vip'] = df.loc[mask, 'vip_count'].expanding().mean().shift(1)
                df.loc[mask, 'hist_avg_executive'] = df.loc[mask, 'executive_count'].expanding().mean().shift(1)
                df.loc[mask, 'hist_avg_price'] = df.loc[mask, 'avg_price'].expanding().mean().shift(1)
                df.loc[mask, 'hist_avg_departing'] = df.loc[mask, 'avg_departing_time'].expanding().mean().shift(1)
                df.loc[mask, 'hist_avg_reaching'] = df.loc[mask, 'avg_reaching_time'].expanding().mean().shift(1)
            
            df['hist_avg_total'] = df['hist_avg_total'].fillna(df['total_buses'].mean())
            df['hist_avg_vip'] = df['hist_avg_vip'].fillna(df['vip_count'].mean())
            df['hist_avg_executive'] = df['hist_avg_executive'].fillna(df['executive_count'].mean())
            df['hist_avg_price'] = df['hist_avg_price'].fillna(df['avg_price'].mean())
            df['hist_avg_departing'] = df['hist_avg_departing'].fillna(df['avg_departing_time'].mean())
            df['hist_avg_reaching'] = df['hist_avg_reaching'].fillna(df['avg_reaching_time'].mean())
            
            feature_cols.extend(['hist_avg_total', 'hist_avg_vip', 'hist_avg_executive', 
                               'hist_avg_price', 'hist_avg_departing', 'hist_avg_reaching'])
        
        self.feature_columns = feature_cols
        
        X = df[feature_cols].fillna(0)
        y = {
            'total': df['total_buses'],
            'vip': df['vip_count'],
            'executive': df['executive_count'],
            'departing_time': df['avg_departing_time'],
            'reaching_time': df['avg_reaching_time'],
            'price': df['avg_price']
        }
        
        print(f"✅ Feature matrix created: {X.shape}")
        print(f"   Features: {len(feature_cols)}")
        
        return X, y, df
    
    def train_models(self, X, y, test_size=0.2):
        """
        Train prediction models for each category
        """
        print("\n🚀 Training models...")
        
        if X is None or not y:
            print("❌ No data available for training")
            return
        
        results = {}
        
        # Split data (preserve temporal order)
        split_idx = int(len(X) * (1 - test_size))
        X_train, X_test = X[:split_idx], X[split_idx:] 

        for target_name, y_values in y.items():
            print(f"\n   Training {target_name.upper()} model...")
            
            y_train, y_test = y_values[:split_idx], y_values[split_idx:]
            
            # Random Forest model
            # model = RandomForestRegressor(
            #     n_estimators=100,
            #     max_depth=10,
            #     min_samples_split=5,
            #     min_samples_leaf=2,
            #     random_state=42,
            #     n_jobs=-1
            # )

            model = GradientBoostingRegressor(
                n_estimators=200,          # Ditingkatkan dari default 100
                learning_rate=0.05,        # Diturunkan dari default 0.1 untuk akurasi yang lebih baik
                max_depth=5,               # Kedalaman dangkal untuk mencegah overfitting
                min_samples_split=5,       # Dipertahankan dari RF
                min_samples_leaf=2,        # Dipertahankan dari RF
                random_state=42
            )
            
            model.fit(X_train, y_train)
            
            # Predictions
            y_pred_train = model.predict(X_train)
            y_pred_test = model.predict(X_test)
            
            # Ensure non-negative predictions
            y_pred_train = np.maximum(0, y_pred_train)
            y_pred_test = np.maximum(0, y_pred_test)
            
            # Metrics
            train_mae = mean_absolute_error(y_train, y_pred_train)
            test_mae = mean_absolute_error(y_test, y_pred_test)
            train_rmse = np.sqrt(mean_squared_error(y_train, y_pred_train))
            test_rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))
            train_r2 = r2_score(y_train, y_pred_train)
            test_r2 = r2_score(y_test, y_pred_test)
            
            results[target_name] = {
                'train_mae': train_mae,
                'test_mae': test_mae,
                'train_rmse': train_rmse,
                'test_rmse': test_rmse,
                'train_r2': train_r2,
                'test_r2': test_r2
            }
            
            print(f"      Train MAE: {train_mae:.2f}, Test MAE: {test_mae:.2f}")
            print(f"      Train RMSE: {train_rmse:.2f}, Test RMSE: {test_rmse:.2f}")
            print(f"      Train R²: {train_r2:.3f}, Test R²: {test_r2:.3f}")
            
            # Feature importance
            feature_importance = pd.DataFrame({
                'feature': self.feature_columns,
                'importance': model.feature_importances_
            }).sort_values('importance', ascending=False)
            
            print(f"      Top 5 features:")
            for idx, row in feature_importance.head(5).iterrows():
                print(f"         {row['feature']}: {row['importance']:.4f}")
            
            self.models[target_name] = model
        
        self.training_stats = results
        print("\n✅ All models trained successfully")
        
        return results
    
    def train(self, data_df):
        """
        Complete training pipeline - wrapper for web interface
        
        Args:
            data_df: Raw DataFrame from database with columns:
                    platform, route_name, route_date, bus_name, bus_type, etc.
        
        Returns:
            dict: Training metrics (mae, rmse, r2)
        """
        print("\n🚀 Starting complete training pipeline...")
        
        if data_df.empty:
            raise ValueError("No data provided for training")
        
        # Step 1: Prepare features
        print("   Step 1: Preparing features...")
        df_features = self.prepare_features(data_df)
        
        # Step 2: Aggregate daily data
        print("   Step 2: Aggregating daily data...")
        df_agg = self.aggregate_daily_data(df_features)
        
        if len(df_agg) < 10:
            print(f"   ⚠️  Only {len(df_agg)} samples available")
            print("      Minimum 10 samples recommended for reliable predictions")
        
        # Step 3: Create training features
        print("   Step 3: Creating feature matrix...")
        X, y, df_with_features = self.create_features_for_training(df_agg)
        
        if X is None:
            raise ValueError("Failed to create feature matrix")
        
        # Step 4: Train models
        print("   Step 4: Training models...")
        training_results = self.train_models(X, y)
        
        # Step 5: Save models
        print("   Step 5: Saving models...")
        self.save_model()
        
        # Return average metrics
        avg_metrics = {
            'mae': np.mean([r['test_mae'] for r in training_results.values()]),
            'rmse': np.mean([r['test_rmse'] for r in training_results.values()]),
            'r2': np.mean([r['test_r2'] for r in training_results.values()])
        }
        
        print(f"\n✅ Training completed successfully!")
        print(f"   Average Test MAE: {avg_metrics['mae']:.2f}")
        print(f"   Average Test RMSE: {avg_metrics['rmse']:.2f}")
        print(f"   Average Test R²: {avg_metrics['r2']:.4f}")
        
        return avg_metrics
    
    def predict_future(self, df_historical, prediction_period='next_week', companies=None):
        """
        Predict bus availability for future period
        
        Args:
            df_historical: Historical data DataFrame
            prediction_period: 'next_week', 'next_month', 'next_year'
            companies: List of company names to predict (None = all)
        
        Returns:
            DataFrame with predictions
        """
        print(f"\n🔮 Predicting for: {prediction_period}")
        
        if df_historical.empty:
            print("❌ No historical data available")
            return pd.DataFrame()
        
        # Determine prediction dates
        today = datetime.now()
        
        if prediction_period == 'next_week':
            start_date = today + timedelta(days=1)
            end_date = today + timedelta(days=7)
            dates = pd.date_range(start_date, end_date, freq='D')
        elif prediction_period == 'next_month':
            start_date = today + timedelta(days=1)
            end_date = today + timedelta(days=30)
            dates = pd.date_range(start_date, end_date, freq='D')
        elif prediction_period == 'next_year':
            start_date = today + timedelta(days=1)
            end_date = today + timedelta(days=365)
            # Sample weekly for year predictions
            dates = pd.date_range(start_date, end_date, freq='W')
        else:
            print(f"❌ Unknown prediction period: {prediction_period}")
            return pd.DataFrame()
        
        # Get unique companies from historical data
        if companies is None:
            companies = df_historical['bus_name'].unique()
        
        # Get common routes and platforms
        routes = df_historical['route_name'].unique()
        platforms = df_historical['platform'].unique()
        
        predictions = []
        
        for date in dates:
            is_weekend = date.dayofweek in [5, 6]
            
            for platform in platforms:
                for route in routes:
                    for company in companies:
                        # Create feature vector
                        feature_dict = {
                            'platform': platform,
                            'route_name': route,
                            'bus_name': company,
                            'is_weekend': int(is_weekend),
                            'day_of_week': date.dayofweek,
                            'month': date.month,
                            'week_of_year': date.isocalendar().week,
                        }
                        
                        # Get historical averages for this company
                        company_hist = df_historical[df_historical['bus_name'] == company]
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
                            feature_dict['avg_price'] = df_historical['avg_price'].mean()
                            feature_dict['min_price'] = df_historical['min_price'].mean()
                            feature_dict['max_price'] = df_historical['max_price'].mean()
                            feature_dict['avg_rating'] = df_historical['avg_rating'].mean()
                            feature_dict['hist_avg_total'] = df_historical['total_buses'].mean()
                            feature_dict['hist_avg_vip'] = df_historical['vip_count'].mean()
                            feature_dict['hist_avg_executive'] = df_historical['executive_count'].mean()
                            feature_dict['hist_avg_price'] = df_historical['avg_price'].mean()
                            feature_dict['hist_avg_departing'] = df_historical['avg_departing_time'].mean()
                            feature_dict['hist_avg_reaching'] = df_historical['avg_reaching_time'].mean()
                        
                        # Encode categorical features
                        try:
                            feature_dict['platform_encoded'] = self.label_encoders['platform'].transform([platform])[0]
                            feature_dict['route_name_encoded'] = self.label_encoders['route_name'].transform([route])[0]
                            feature_dict['bus_name_encoded'] = self.label_encoders['bus_name'].transform([company])[0]
                        except:
                            # Skip if unseen category
                            continue
                        
                        # Create feature vector
                        X_pred = pd.DataFrame([feature_dict])[self.feature_columns].fillna(0)
                        
                        # Make predictions
                        pred_total = max(0, round(self.models['total'].predict(X_pred)[0]))
                        pred_vip = max(0, round(self.models['vip'].predict(X_pred)[0]))
                        pred_executive = max(0, round(self.models['executive'].predict(X_pred)[0]))
                        pred_departing = max(0, round(self.models['departing_time'].predict(X_pred)[0]))
                        pred_reaching = max(0, round(self.models['reaching_time'].predict(X_pred)[0]))
                        pred_price = max(0, round(self.models['price'].predict(X_pred)[0]))
                        
                        # Convert time predictions from minutes to HH:MM format
                        def minutes_to_time(minutes):
                            """Convert minutes since midnight to HH:MM format"""
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
                            'predicted_departing_time': minutes_to_time(pred_departing),
                            'predicted_reaching_time': minutes_to_time(pred_reaching),
                            'predicted_price': pred_price
                        })
        
        pred_df = pd.DataFrame(predictions)
        
        print(f"✅ Generated {len(pred_df):,} predictions")
        print(f"   Date range: {pred_df['date'].min()} to {pred_df['date'].max()}")
        print(f"   Companies: {pred_df['bus_name'].nunique()}")
        
        return pred_df
    
    def create_summary_table(self, predictions_df, group_by_weekend=True):
        """
        Create summary table in requested format
        
        Returns aggregated predictions by company and category
        """
        if predictions_df.empty:
            return pd.DataFrame()
        
        if group_by_weekend:
            # Separate weekday and weekend
            summary_tables = {}
            
            for period_type in ['Weekday', 'Weekend']:
                period_data = predictions_df[predictions_df['is_weekend'] == period_type]
                
                if period_data.empty:
                    continue
                
                # Aggregate by company
                summary = period_data.groupby('bus_name').agg({
                    'predicted_total': 'mean',
                    'predicted_vip': 'mean',
                    'predicted_executive': 'mean',
                    'predicted_other': 'mean'
                }).round(1)
                
                summary.columns = ['Total', 'VIP', 'Executive', 'Other']
                summary_tables[period_type] = summary
            
            return summary_tables
        else:
            # Overall summary
            summary = predictions_df.groupby('bus_name').agg({
                'predicted_total': 'mean',
                'predicted_vip': 'mean',
                'predicted_executive': 'mean',
                'predicted_other': 'mean'
            }).round(1)
            
            summary.columns = ['Total', 'VIP', 'Executive', 'Other']
            return summary
    
    def save_model(self, filepath='models/bus_prediction_model.pkl'):
        """Save trained model to file"""
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        model_data = {
            'models': self.models,
            'label_encoders': self.label_encoders,
            'feature_columns': self.feature_columns,
            'training_stats': self.training_stats,
            'trained_date': datetime.now().isoformat()
        }
        
        joblib.dump(model_data, filepath)
        print(f"\n✅ Model saved to: {filepath}")
    
    def load_model(self, filepath='models/bus_prediction_model.pkl'):
        """Load trained model from file"""
        if not os.path.exists(filepath):
            print(f"❌ Model file not found: {filepath}")
            return False
        
        model_data = joblib.load(filepath)
        
        self.models = model_data['models']
        self.label_encoders = model_data['label_encoders']
        self.feature_columns = model_data['feature_columns']
        self.training_stats = model_data.get('training_stats', {})
        
        print(f"✅ Model loaded from: {filepath}")
        print(f"   Trained on: {model_data.get('trained_date', 'Unknown')}")
        return True


def main(days_back=90):
    """Main training pipeline"""
    print("="*70)
    print("BUS AVAILABILITY PREDICTION MODEL - TRAINING")
    print("="*70)
    
    # Connect to database
    try:
        db_config = load_db_config()
        db = BusDatabase(db_type=db_config['type'], db_config=db_config)
        print(f"✅ Connected to {db_config['type'].upper()} database\n")
    except Exception as e:
        print(f"❌ Failed to connect to database: {e}")
        return
    
    try:
        # Initialize model
        model = BusPredictionModel()
        
        # Load data
        print(f"📊 Training with last {days_back} days of data\n")
        df_raw = model.load_data_from_database(db, days_back=days_back)
        
        if df_raw.empty:
            print("\n❌ No data available for training")
            print("   Please crawl some data first!")
            return
        
        # Prepare features
        df_features = model.prepare_features(df_raw)
        
        # Aggregate daily data
        df_agg = model.aggregate_daily_data(df_features)
        
        if len(df_agg) < 10:
            print(f"\n⚠️  Only {len(df_agg)} samples available")
            print("   Minimum 10 samples recommended for reliable predictions")
            print("   Consider crawling more data over multiple days")
        
        # Create training features
        X, y, df_with_features = model.create_features_for_training(df_agg)
        
        if X is None:
            print("\n❌ Failed to create feature matrix")
            return
        
        # Train models
        training_results = model.train_models(X, y)
        
        # Save model
        model.save_model('models/bus_prediction_model.pkl')
        
        # Make sample predictions
        print("\n" + "="*70)
        print("SAMPLE PREDICTIONS")
        print("="*70)
        
        for period in ['next_week', 'next_month']:
            pred_df = model.predict_future(df_agg, prediction_period=period)
            
            if not pred_df.empty:
                print(f"\n📊 Predictions for {period.replace('_', ' ').upper()}:")
                
                # Create summary table
                summary_tables = model.create_summary_table(pred_df, group_by_weekend=True)
                
                for period_type, summary in summary_tables.items():
                    print(f"\n   {period_type}:")
                    print("   " + "-"*60)
                    print(summary.to_string())
        
        print("\n" + "="*70)
        print("✅ TRAINING COMPLETED SUCCESSFULLY")
        print("="*70)
        print("\nYou can now use the model for predictions!")
        print("Run: python predict_bus_availability.py")
        
    except Exception as e:
        print(f"\n❌ Error during training: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()


if __name__ == "__main__":
    import sys
    
    # Support command-line arguments
    days_back = 90  # Default
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "--help" or sys.argv[1] == "-h":
            print("\nUsage: python train_bus_prediction_model.py [--days DAYS]")
            print("\nOptions:")
            print("  --days DAYS    Number of days of historical data to use (default: 90)")
            print("\nExamples:")
            print("  python train_bus_prediction_model.py")
            print("  python train_bus_prediction_model.py --days 30")
            print("  python train_bus_prediction_model.py --days 180")
            print()
            sys.exit(0)
        elif sys.argv[1] == "--days" and len(sys.argv) > 2:
            try:
                days_back = int(sys.argv[2])
                print(f"\n📊 Using custom training period: {days_back} days\n")
            except ValueError:
                print(f"\n❌ Invalid days value: {sys.argv[2]}")
                sys.exit(1)
    
    main(days_back=days_back)
