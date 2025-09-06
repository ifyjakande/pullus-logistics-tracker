#!/usr/bin/env python3
"""
Pullus Logistics Dashboard Updater

This script replaces manual Google Sheets formulas with automated calculations
and provides detailed monthly breakdowns with beautiful formatting.

Key Features:
- Automated calculations using Grand Total Cost (Logistics + Fuel + Miscellaneous)
- Monthly breakdown with movement categorization
- Location normalization for accurate categorization
- Colorful, professional formatting
"""

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import os
import json
from typing import Dict
import pytz

class LogisticsDashboardUpdater:
    def __init__(self, credentials_path: str = None, spreadsheet_id: str = None):
        """Initialize the dashboard updater with credentials and spreadsheet ID."""
        # Use environment variables or parameters
        self.credentials_path = credentials_path or os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE', 'service_account.json')
        self.spreadsheet_id = spreadsheet_id or os.getenv('SPREADSHEET_ID')
        self.gc = None
        self.spreadsheet = None
        self.data_sheet = None
        self.dashboard_sheet = None
        self.cash_flow_sheet = None
        self.new_sheet_name = "Automated Dashboard"
        self.cash_flow_sheet_name = "Cash Flow Timeline"
        
        # Colors for formatting (eye-friendly)
        self.colors = {
            'header': {'red': 0.91, 'green': 0.96, 'blue': 0.99},      # Light blue
            'subheader': {'red': 0.94, 'green': 0.97, 'blue': 0.91},  # Light green
            'calculated': {'red': 1.0, 'green': 0.97, 'blue': 0.88},  # Light yellow
            'data': {'red': 1.0, 'green': 1.0, 'blue': 1.0},          # White
            'border': {'red': 0.56, 'green': 0.79, 'blue': 0.97},     # Medium blue
            'positive': {'red': 0.85, 'green': 0.95, 'blue': 0.85},   # Light green
            'negative': {'red': 0.98, 'green': 0.85, 'blue': 0.85},   # Light red
            'primary': {'red': 0.1, 'green': 0.46, 'blue': 0.82},     # Primary blue
            'timestamp': {'red': 0.96, 'green': 0.94, 'blue': 0.98},  # Subtle lavender
        }
        
        # Rate limiting configuration
        self.max_retries = 5
        self.base_delay = 1.0
        self.max_delay = 60.0
        self.requests_per_minute = 100  # Google Sheets API limit
        self.request_count = 0
        self.minute_start = time.time()
    
    def connect(self):
        """Connect to Google Sheets using service account credentials."""
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            
            # Handle both file path and JSON string from environment
            service_account_env = os.getenv('GOOGLE_SERVICE_ACCOUNT')
            if service_account_env:
                # Use environment variable (GitHub Actions)
                try:
                    service_account_info = json.loads(service_account_env)
                    creds = Credentials.from_service_account_info(
                        service_account_info, 
                        scopes=scopes
                    )
                except json.JSONDecodeError:
                    raise ValueError("Invalid service account JSON in environment variable")
            elif os.path.isfile(self.credentials_path):
                # Use local file
                creds = Credentials.from_service_account_file(
                    self.credentials_path, 
                    scopes=scopes
                )
            else:
                raise ValueError("No valid service account configuration found")
            
            self.gc = gspread.authorize(creds)
            self.spreadsheet = self.gc.open_by_key(self.spreadsheet_id)
            self.data_sheet = self.spreadsheet.worksheet('Logistics Data')
            
            # Create or get the sheets
            try:
                self.dashboard_sheet = self.spreadsheet.worksheet(self.new_sheet_name)
                print(f"✓ Found existing '{self.new_sheet_name}' sheet")
            except gspread.WorksheetNotFound:
                self.dashboard_sheet = self.spreadsheet.add_worksheet(
                    title=self.new_sheet_name, rows=100, cols=20
                )
                print(f"✓ Created new '{self.new_sheet_name}' sheet")
            
            try:
                self.cash_flow_sheet = self.spreadsheet.worksheet(self.cash_flow_sheet_name)
                print(f"✓ Found existing '{self.cash_flow_sheet_name}' sheet")
            except gspread.WorksheetNotFound:
                self.cash_flow_sheet = self.spreadsheet.add_worksheet(
                    title=self.cash_flow_sheet_name, rows=200, cols=10
                )
                print(f"✓ Created new '{self.cash_flow_sheet_name}' sheet")
            
            print("✓ Successfully connected to Google Sheets")
            return True
            
        except Exception as e:
            print(f"✗ Error connecting to Google Sheets: Connection failed")
            return False
    
    def rate_limit_check(self):
        """Check and enforce rate limits."""
        current_time = time.time()
        
        # Reset counter if a minute has passed
        if current_time - self.minute_start > 60:
            self.request_count = 0
            self.minute_start = current_time
        
        # If approaching limit, wait
        if self.request_count >= self.requests_per_minute - 5:  # Buffer of 5 requests
            sleep_time = 60 - (current_time - self.minute_start) + 1
            if sleep_time > 0:
                print(f"⏳ Rate limit approaching, waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.minute_start = time.time()
        
        self.request_count += 1
    
    def execute_with_retry(self, func, *args, **kwargs):
        """Execute a function with exponential backoff retry logic."""
        for attempt in range(self.max_retries):
            try:
                self.rate_limit_check()
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                if '429' in str(e) or 'quota' in str(e).lower() or 'rate' in str(e).lower():
                    if attempt < self.max_retries - 1:
                        delay = min(self.base_delay * (2 ** attempt) + 0.5, self.max_delay)
                        print(f"🔄 Rate limit hit (attempt {attempt + 1}/{self.max_retries}), waiting {delay:.1f}s...")
                        time.sleep(delay)
                        continue
                    else:
                        print(f"❌ Max retries exceeded for rate limiting")
                        raise
                else:
                    # Non-rate-limit error, re-raise immediately
                    raise
        return None
    
    def get_last_updated_timestamp(self) -> str:
        """Get current timestamp in WAT timezone with 12-hour format."""
        try:
            # Create WAT timezone (UTC+1)
            wat_timezone = pytz.timezone('Africa/Lagos')  # Lagos is in WAT
            
            # Get current time in WAT
            wat_time = datetime.now(wat_timezone)
            
            # Format as 12-hour format with AM/PM
            formatted_time = wat_time.strftime("%B %d, %Y at %I:%M %p WAT")
            
            return f"Last Updated: {formatted_time}"
            
        except Exception as e:
            # Fallback to UTC if timezone fails
            utc_time = datetime.utcnow()
            wat_time = utc_time + timedelta(hours=1)  # Manual WAT conversion
            formatted_time = wat_time.strftime("%B %d, %Y at %I:%M %p WAT")
            return f"Last Updated: {formatted_time}"
    
    def normalize_location(self, location: str) -> str:
        """Normalize location names for consistent comparison."""
        if not location or pd.isna(location):
            return ""
        # Clean and normalize
        normalized = str(location).upper().strip()
        # Remove common punctuation and extra spaces
        normalized = re.sub(r'[.,;\\-_]+', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()
        return normalized
    
    def get_abuja_metropolitan_areas(self) -> dict:
        """Get comprehensive mapping of Abuja metropolitan area locations including FCT and Nasarawa border areas."""
        abuja_metro_areas = {
            # FCT Area Councils and Districts
            'fct_areas': {
                # AMAC (Abuja Municipal Area Council) - Main districts
                'MAITAMA', 'GARKI', 'ASOKORO', 'WUSE', 'WUYE', 'UTAKO', 'GWARINPA', 
                'LOKOGOMA', 'JAHI', 'JABI', 'GUDU', 'GADUWA', 'AGUNGI', 'GUZAPE',
                'KATAMPE', 'MBORA', 'KADO', 'LIFE CAMP', 'KARMO', 'JUKWOYI',
                'KUBWA', 'LUGBE', 'KUJE TOWN', 'GWAGWALADA TOWN', 'BWARI TOWN',
                'KWALI TOWN', 'ABAJI TOWN',
                # General FCT references
                'FCT', 'FEDERAL CAPITAL TERRITORY', 'ABUJA', 'AMAC'
            },
            
            # Nasarawa State border areas (Abuja suburbs/satellite towns)
            'nasarawa_metro_areas': {
                # Karu LGA - Major Abuja satellite towns
                'KARU', 'NEW KARU', 'MARARABA', 'MARABA', 'NYANYA', 'NEW NYANYA', 
                'KARSHI', 'KCENYI', 'GITATA', 'GURKU', 'ONE MAN VILLAGE',
                # Toto LGA areas close to FCT
                'TOTO', 'GADABUKE',
                # Keffi LGA 
                'KEFFI',
                # Other satellite areas specifically close to Abuja
                'MASAKA', 'NASARAWA TOWN', 'NASARAWA'
                # Note: 'NASARAWA' in logistics data typically refers to Nasarawa town, not the state
            }
        }
        
        # Create a unified set of all Abuja metropolitan areas
        all_metro_areas = set()
        all_metro_areas.update(abuja_metro_areas['fct_areas'])
        all_metro_areas.update(abuja_metro_areas['nasarawa_metro_areas'])
        
        return {
            'fct_areas': abuja_metro_areas['fct_areas'],
            'nasarawa_metro_areas': abuja_metro_areas['nasarawa_metro_areas'],
            'all_metro_areas': all_metro_areas
        }
    
    def is_abuja_metropolitan_area(self, location: str) -> tuple:
        """Check if a location is in the Abuja metropolitan area and return classification."""
        if not location or pd.isna(location):
            return False, None
            
        normalized_location = self.normalize_location(location)
        metro_areas = self.get_abuja_metropolitan_areas()
        
        # Special rule: ANY mention of "NASARAWA" should be treated as Abuja metro area
        if 'NASARAWA' in normalized_location:
            return True, 'NASARAWA_METRO'
        
        # Check for exact matches first (more precise)
        for area in metro_areas['all_metro_areas']:
            if normalized_location == area:
                if area in metro_areas['fct_areas']:
                    return True, 'FCT'
                elif area in metro_areas['nasarawa_metro_areas']:
                    return True, 'NASARAWA_METRO'
        
        # Then check for substring matches (less precise, but catch variations)
        for area in metro_areas['all_metro_areas']:
            if area in normalized_location and len(area) > 3:  # Avoid matching very short strings
                if area in metro_areas['fct_areas']:
                    return True, 'FCT'
                elif area in metro_areas['nasarawa_metro_areas']:
                    return True, 'NASARAWA_METRO'
        
        return False, None
    
    def create_location_mapping(self, df: pd.DataFrame) -> dict:
        """Create a mapping of location variations to standardized names using pattern matching."""
        location_mapping = {}
        
        # Get all unique locations from both From and To columns
        all_locations = list(set())
        for col in ['From_Normalized', 'To_Normalized']:
            all_locations.extend(df[col].dropna().unique())
        
        # Remove duplicates and empty strings
        all_locations = [loc for loc in set(all_locations) if loc and loc.strip()]
        
        if not all_locations:
            return location_mapping
        
        # Define pattern-based grouping rules
        def get_location_group(location: str) -> str:
            location = location.upper().strip()
            
            # Handle specific known patterns first
            if 'MARABA' in location or 'MARARABA' in location:
                return 'MARARABA'  # Standardize to correct spelling
            elif 'U/BARDE' in location or 'UBARDE' in location:
                return 'U/BARDE'  # Group U/BARDE variations
            elif 'MANDO' in location and 'KUDENDE' in location:
                return 'MANDO'  # Group MANDO & KUDENDE variations
            elif any(word in location for word in ['COLD', 'STORAGE', 'ROOM', 'COOLING']):
                return 'COLD ROOM'  # Group all cold storage variations
            elif any(word in location for word in ['FCT', 'FEDERAL CAPITAL TERRITORY']):
                return 'ABUJA'  # Group FCT variations to ABUJA
            elif 'PLETHORA' in location:
                # For locations with PLETHORA, extract the main location name
                if 'JOS' in location:
                    return 'JOS'
                elif 'RIDO' in location:
                    return 'RIDO'
                elif 'MARABA' in location or 'MARARABA' in location:
                    return 'MARARABA'
                else:
                    return location.replace('PLETHORA', '').strip()
            else:
                # For single-word locations or unmatched patterns, use as-is
                return location
        
        # Create groups based on patterns
        location_groups = {}
        for location in all_locations:
            group_name = get_location_group(location)
            if group_name not in location_groups:
                location_groups[group_name] = []
            location_groups[group_name].append(location)
        
        # Create final mapping
        for standard_name, variations in location_groups.items():
            for location in variations:
                location_mapping[location] = standard_name
        
        return location_mapping
    
    def standardize_location(self, location: str, mapping: dict) -> str:
        """Apply location mapping to standardize location names."""
        normalized = self.normalize_location(location)
        return mapping.get(normalized, normalized)
    
    def read_logistics_data(self) -> pd.DataFrame:
        """Read and process data from the Logistics Data sheet."""
        try:
            # Get all data from the sheet
            all_data = self.data_sheet.get_all_values()
            
            # Headers are in row 3 (index 2)
            headers = all_data[2]  # Row 3: actual headers
            
            # Map the headers correctly
            header_mapping = {
                0: 'Date',
                1: 'From', 
                2: 'To',
                3: 'Logistics Type',
                4: 'Transportation Mode',
                5: 'Number of Birds',
                6: 'Total Weight (kg)',
                7: 'Added Funds',
                8: 'Logistics Cost',
                9: 'Fuel Cost',
                10: 'Miscellaneous Cost',
                11: 'Is Abuja'
            }
            
            # Data starts from row 4 (index 3)
            data_rows = all_data[3:]
            
            # Create DataFrame with proper column names
            df_data = []
            for row in data_rows:
                if len(row) > 0 and row[0]:  # Skip empty rows
                    row_dict = {}
                    for col_idx, col_name in header_mapping.items():
                        if col_idx < len(row):
                            row_dict[col_name] = row[col_idx]
                        else:
                            row_dict[col_name] = ""
                    df_data.append(row_dict)
            
            if not df_data:
                print("No data found in the sheet")
                return pd.DataFrame()
            
            df = pd.DataFrame(df_data)
            
            # Data cleaning and type conversion
            df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
            
            # Normalize location names
            df['From_Normalized'] = df['From'].apply(self.normalize_location)
            df['To_Normalized'] = df['To'].apply(self.normalize_location)
            df['Logistics_Type_Normalized'] = df['Logistics Type'].apply(lambda x: str(x).upper().strip())
            
            # Convert numeric columns
            numeric_columns = ['Number of Birds', 'Total Weight (kg)', 'Added Funds', 
                             'Logistics Cost', 'Fuel Cost', 'Miscellaneous Cost']
            
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
            # Calculate Grand Total Cost (Logistics + Fuel + Miscellaneous)
            df['Grand Total Cost'] = (
                df['Logistics Cost'] + 
                df['Fuel Cost'] + 
                df['Miscellaneous Cost']
            )
            
            # Calculate per bird metrics using Grand Total Cost
            df['Cost per Bird'] = df.apply(
                lambda row: row['Logistics Cost'] / row['Number of Birds'] 
                if row['Number of Birds'] > 0 and row['Logistics Cost'] > 0 else 0, axis=1
            )
            
            df['Cost per kg'] = df.apply(
                lambda row: row['Logistics Cost'] / row['Total Weight (kg)']
                if row['Total Weight (kg)'] > 0 and row['Logistics Cost'] > 0 else 0, axis=1
            )
            
            # Calculate Grand Total per bird/kg using Grand Total Cost
            df['Grand Total per Bird'] = df.apply(
                lambda row: row['Grand Total Cost'] / row['Number of Birds']
                if row['Number of Birds'] > 0 and row['Grand Total Cost'] > 0 else 0, axis=1
            )
            
            df['Grand Total per kg'] = df.apply(
                lambda row: row['Grand Total Cost'] / row['Total Weight (kg)']
                if row['Total Weight (kg)'] > 0 and row['Grand Total Cost'] > 0 else 0, axis=1
            )
            
            # Calculate Balance
            df['Balance'] = df.apply(
                lambda row: row['Added Funds'] - row['Grand Total Cost']
                if row['Added Funds'] > 0 and row['Grand Total Cost'] > 0 else 0, axis=1
            )
            
            # Create location mapping for standardization
            location_mapping = self.create_location_mapping(df)
            
            # Add movement categorization using location mapping
            df['Movement Category'] = df.apply(lambda row: self.categorize_movement(row, location_mapping), axis=1)
            
            # Add month-year for grouping
            df['Month_Year'] = df['Date'].dt.to_period('M').astype(str)
            
            print(f"✓ Successfully processed {len(df)} records")
            return df
            
        except Exception as e:
            print(f"✗ Error reading logistics data: {e}")
            return pd.DataFrame()
    
    def format_location_for_display(self, location: str) -> str:
        """Format location name for display with proper capitalization."""
        if not location:
            return location
        
        # Handle special cases first
        if location == 'COLD ROOM':
            return 'Cold Room'
        elif location == 'FCT' or location == 'FEDERAL CAPITAL TERRITORY':
            return 'FCT'
        elif location == 'AMAC':
            return 'AMAC'
        else:
            # Convert to title case for regular locations
            return location.title()
    
    def categorize_movement(self, row, location_mapping: dict) -> str:
        """Categorize movements based on logistics type and standardized locations with Abuja internal detection."""
        logistics_type = row['Logistics_Type_Normalized']
        from_loc_std = self.standardize_location(row['From'], location_mapping)
        to_loc_std = self.standardize_location(row['To'], location_mapping)
        
        # Check if both locations are in Abuja metropolitan area
        from_is_abuja_metro, _ = self.is_abuja_metropolitan_area(from_loc_std)
        to_is_abuja_metro, _ = self.is_abuja_metropolitan_area(to_loc_std)
        
        # Format locations for display
        from_display = self.format_location_for_display(from_loc_std)
        to_display = self.format_location_for_display(to_loc_std)
        
        if logistics_type == 'OFFTAKE':
            if 'COLD ROOM' in to_loc_std:
                return 'Offtake to Cold Room'
            else:
                # Generate dynamic category based on destination
                return f'Offtake to {to_display}'
        
        elif logistics_type == 'SUPPLY':
            # Check if both locations are in Abuja metropolitan area (FCT + Nasarawa suburbs)
            if from_is_abuja_metro and to_is_abuja_metro:
                return 'Abuja Internal Supply'
            # Check if it's an internal route (same standardized location)
            elif from_loc_std == to_loc_std:
                return f'Supply - {from_display} Internal'
            else:
                # Create specific route-based category
                return f'Supply - {from_display} to {to_display}'
        
        return 'Uncategorized'
    
    def calculate_overall_metrics(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate overall KPIs for the dashboard."""
        try:
            # Filter out rows with zero values for averages
            df_with_data = df[df['Grand Total Cost'] > 0]
            df_offtake = df_with_data[df_with_data['Logistics_Type_Normalized'] == 'OFFTAKE']
            df_supply = df_with_data[df_with_data['Logistics_Type_Normalized'] == 'SUPPLY']
            # Use our new intelligent Abuja detection instead of manual "Is Abuja" flag
            df_abuja_supply = df_supply[df_supply['Movement Category'] == 'Abuja Internal Supply']
            
            metrics = {
                # Bird-based metrics (using AGGREGATE METHOD - industry standard)
                'avg_purchase_cost_per_bird': df_offtake['Grand Total Cost'].sum() / df_offtake['Number of Birds'].sum() if df_offtake['Number of Birds'].sum() > 0 else 0,
                'avg_supply_cost_per_bird': df_supply['Grand Total Cost'].sum() / df_supply['Number of Birds'].sum() if df_supply['Number of Birds'].sum() > 0 else 0,
                'avg_abuja_supply_cost_per_bird': df_abuja_supply['Grand Total Cost'].sum() / df_abuja_supply['Number of Birds'].sum() if df_abuja_supply['Number of Birds'].sum() > 0 else 0,
                'total_birds_moved': df['Number of Birds'].sum(),
                'avg_grand_total_per_bird': df_with_data['Grand Total Cost'].sum() / df_with_data['Number of Birds'].sum() if df_with_data['Number of Birds'].sum() > 0 else 0,
                
                # Weight-based metrics (using AGGREGATE METHOD - industry standard)
                'avg_purchase_cost_per_kg': df_offtake['Grand Total Cost'].sum() / df_offtake['Total Weight (kg)'].sum() if df_offtake['Total Weight (kg)'].sum() > 0 else 0,
                'avg_supply_cost_per_kg': df_supply['Grand Total Cost'].sum() / df_supply['Total Weight (kg)'].sum() if df_supply['Total Weight (kg)'].sum() > 0 else 0,
                'avg_abuja_supply_cost_per_kg': df_abuja_supply['Grand Total Cost'].sum() / df_abuja_supply['Total Weight (kg)'].sum() if df_abuja_supply['Total Weight (kg)'].sum() > 0 else 0,
                'total_weight_moved': df['Total Weight (kg)'].sum(),
                'avg_grand_total_per_kg': df_with_data['Grand Total Cost'].sum() / df_with_data['Total Weight (kg)'].sum() if df_with_data['Total Weight (kg)'].sum() > 0 else 0,
                
                # General metrics
                'avg_fuel_cost': df[df['Fuel Cost'] > 0]['Fuel Cost'].mean() if len(df[df['Fuel Cost'] > 0]) > 0 else 0,
                'third_party_percentage': (len(df[df['Transportation Mode'] == 'Third Party']) / len(df) * 100) if len(df) > 0 else 0,
                'current_running_balance': self.get_current_running_balance(df),
            }
            
            return metrics
            
        except Exception as e:
            print(f"Error calculating overall metrics: {e}")
            return {}
    
    def get_current_running_balance(self, df: pd.DataFrame) -> float:
        """Get the current running balance (final balance after all transactions).
        
        Note: Excludes fuel costs as logistics manager doesn't pay fuel from allocated funds.
        """
        try:
            # Sort by date to get chronological order
            df_sorted = df.sort_values('Date').copy()
            
            # Initialize running balance
            running_balance = 0
            
            for _, row in df_sorted.iterrows():
                # Add funds when they come in
                if row['Added Funds'] > 0:
                    running_balance += row['Added Funds']
                
                # Deduct only logistics cost + miscellaneous cost (EXCLUDE fuel cost)
                logistics_cost = row.get('Logistics Cost', 0)
                misc_cost = row.get('Miscellaneous Cost', 0)
                actual_expense = logistics_cost + misc_cost
                
                if actual_expense > 0:
                    running_balance -= actual_expense
            
            return running_balance
            
        except Exception as e:
            print(f"Error calculating current running balance: {e}")
            return 0
    
    def calculate_monthly_breakdown(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate monthly breakdown by movement categories with monthly totals and averages."""
        try:
            # Group by month and movement category
            monthly_data = []
            
            # Get unique months and categories
            months = sorted(df['Month_Year'].dropna().unique())
            categories = df['Movement Category'].unique()
            
            for month in months:
                month_data = df[df['Month_Year'] == month]
                
                # Add category breakdowns for this month
                for category in categories:
                    cat_data = month_data[month_data['Movement Category'] == category]
                    
                    if len(cat_data) > 0:
                        # Calculate metrics for this month-category combination
                        total_trips = len(cat_data)
                        total_birds = cat_data['Number of Birds'].sum()
                        total_weight = cat_data['Total Weight (kg)'].sum()
                        total_logistics_cost = cat_data['Logistics Cost'].sum()
                        total_grand_cost = cat_data['Grand Total Cost'].sum()
                        
                        # Calculate averages using AGGREGATE METHOD (industry standard)
                        # Avg Cost per Bird = Total Grand Cost / Total Birds for the period
                        avg_cost_per_bird = total_grand_cost / total_birds if total_birds > 0 else 0
                        # Avg Cost per kg = Total Grand Cost / Total Weight for the period  
                        avg_cost_per_kg = total_grand_cost / total_weight if total_weight > 0 else 0
                        
                        monthly_data.append({
                            'Month': month,
                            'Category': category,
                            'Trips': total_trips,
                            'Total Birds': total_birds,
                            'Total Weight (kg)': total_weight,
                            'Total Logistics Cost': total_logistics_cost,
                            'Total Grand Cost': total_grand_cost,
                            'Avg Cost per Bird': avg_cost_per_bird if pd.notna(avg_cost_per_bird) else 0,
                            'Avg Cost per kg': avg_cost_per_kg if pd.notna(avg_cost_per_kg) else 0,
                            'Is_Month_Total': False
                        })
                
                # Add MONTH TOTAL row after each month's categories
                if not month_data.empty:
                    month_total_trips = len(month_data)
                    month_total_birds = month_data['Number of Birds'].sum()
                    month_total_weight = month_data['Total Weight (kg)'].sum()
                    month_total_logistics_cost = month_data['Logistics Cost'].sum()
                    month_total_grand_cost = month_data['Grand Total Cost'].sum()
                    
                    # Calculate MONTHLY AGGREGATE AVERAGES across all categories
                    month_avg_cost_per_bird = month_total_grand_cost / month_total_birds if month_total_birds > 0 else 0
                    month_avg_cost_per_kg = month_total_grand_cost / month_total_weight if month_total_weight > 0 else 0
                    
                    monthly_data.append({
                        'Month': month,
                        'Category': 'MONTH TOTAL',
                        'Trips': month_total_trips,
                        'Total Birds': month_total_birds,
                        'Total Weight (kg)': month_total_weight,
                        'Total Logistics Cost': month_total_logistics_cost,
                        'Total Grand Cost': month_total_grand_cost,
                        'Avg Cost per Bird': month_avg_cost_per_bird,
                        'Avg Cost per kg': month_avg_cost_per_kg,
                        'Is_Month_Total': True
                    })
            
            # Convert to DataFrame for easier manipulation
            monthly_df = pd.DataFrame(monthly_data)
            
            # Calculate month-over-month percentage changes for Avg Cost/kg only (exclude totals)
            if not monthly_df.empty:
                # Add percentage change column for Cost/kg
                monthly_df['Cost_kg % Change'] = 0.0
                
                # Sort by category and month for proper comparison
                monthly_df = monthly_df.sort_values(['Category', 'Month'])
                
                # Calculate Cost/kg percentage changes for each category (skip month totals)
                for category in monthly_df[monthly_df['Is_Month_Total'] == False]['Category'].unique():
                    cat_mask = (monthly_df['Category'] == category) & (monthly_df['Is_Month_Total'] == False)
                    cat_data = monthly_df[cat_mask].copy().sort_values('Month')
                    
                    if len(cat_data) > 1:
                        # Calculate month-over-month Cost/kg changes
                        for i in range(1, len(cat_data)):
                            current_idx = cat_data.index[i]
                            previous_idx = cat_data.index[i-1]
                            
                            # Cost/kg % change (unit economics)
                            prev_cost_kg = cat_data.loc[previous_idx, 'Avg Cost per kg']
                            curr_cost_kg = cat_data.loc[current_idx, 'Avg Cost per kg']
                            if prev_cost_kg > 0:
                                cost_kg_change = ((curr_cost_kg - prev_cost_kg) / prev_cost_kg) * 100
                                monthly_df.loc[current_idx, 'Cost_kg % Change'] = cost_kg_change
                
                # Sort by month first, then put MONTH TOTAL at the end of each month
                monthly_df['Sort_Order'] = monthly_df.apply(lambda x: (x['Month'], 1 if x['Is_Month_Total'] else 0, x['Category']), axis=1)
                monthly_df = monthly_df.sort_values('Sort_Order')
                monthly_df = monthly_df.drop('Sort_Order', axis=1)
            
            return monthly_df
            
        except Exception as e:
            print(f"Error calculating monthly breakdown: {e}")
            return pd.DataFrame()
    
    
    def format_cell_range(self, range_name: str, bg_color: dict, text_color: dict = None, 
                         bold: bool = False, font_size: int = 10, format_type: str = None, sheet=None):
        """Apply formatting to a cell range - now collects requests for batch processing."""
        if text_color is None:
            text_color = {'red': 0, 'green': 0, 'blue': 0}  # Black
        
        format_dict = {
            "backgroundColor": bg_color,
            "textFormat": {
                "foregroundColor": text_color,
                "fontSize": font_size,
                "bold": bold
            },
            "horizontalAlignment": "CENTER"
        }
        
        if format_type == "currency":
            format_dict["numberFormat"] = {
                "type": "CURRENCY",
                "pattern": "#,##0.00"
            }
        elif format_type == "number":
            format_dict["numberFormat"] = {
                "type": "NUMBER", 
                "pattern": "#,##0.00"
            }
        
        # Store format request for batch processing
        if not hasattr(self, '_format_queue'):
            self._format_queue = []
        
        self._format_queue.append({
            'range': range_name,
            'format': format_dict,
            'sheet': sheet if sheet else self.dashboard_sheet
        })
    
    def flush_formatting_queue(self, sheet=None):
        """Process all queued formatting requests efficiently."""
        if not hasattr(self, '_format_queue') or not self._format_queue:
            return
        
        try:
            # Group by sheet
            sheet_requests = {}
            for req in self._format_queue:
                target_sheet = req['sheet']
                if target_sheet not in sheet_requests:
                    sheet_requests[target_sheet] = []
                sheet_requests[target_sheet].append(req)
            
            # Process each sheet's requests
            for target_sheet, requests in sheet_requests.items():
                if sheet and target_sheet != sheet:
                    continue
                    
                # Process in smaller batches
                batch_size = 20
                for i in range(0, len(requests), batch_size):
                    batch = requests[i:i + batch_size]
                    
                    for req in batch:
                        try:
                            self.execute_with_retry(
                                target_sheet.format,
                                req['range'],
                                req['format']
                            )
                        except:
                            continue  # Skip failed individual formats
                    
                    # Small delay between batches
                    if i + batch_size < len(requests):
                        time.sleep(0.3)
            
            # Clear the queue
            self._format_queue = []
            
        except Exception as e:
            print(f"⚠ Warning: Some formatting could not be applied: {e}")
            self._format_queue = []
    
    def calculate_cash_flow_timeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate running balance over time (cash flow timeline).
        
        Note: Excludes fuel costs as logistics manager doesn't pay fuel from allocated funds.
        """
        try:
            # Sort by date to get chronological order
            df_sorted = df.sort_values('Date').copy()
            
            # Initialize running balance
            running_balance = 0
            cash_flow_data = []
            
            for _, row in df_sorted.iterrows():
                # Add funds when they come in
                if row['Added Funds'] > 0:
                    running_balance += row['Added Funds']
                    cash_flow_data.append({
                        'Date': row['Date'].strftime('%Y-%m-%d'),
                        'Transaction Type': 'Fund Addition',
                        'From': '',
                        'To': '',
                        'Amount': row['Added Funds'],
                        'Running Balance': running_balance
                    })
                
                # Deduct only logistics cost + miscellaneous cost (EXCLUDE fuel cost)
                logistics_cost = row.get('Logistics Cost', 0)
                misc_cost = row.get('Miscellaneous Cost', 0)
                actual_expense = logistics_cost + misc_cost
                
                if actual_expense > 0:
                    running_balance -= actual_expense
                    cash_flow_data.append({
                        'Date': row['Date'].strftime('%Y-%m-%d'),
                        'Transaction Type': 'Expense',
                        'From': row['From'],
                        'To': row['To'],
                        'Amount': -actual_expense,  # Negative for expenses
                        'Running Balance': running_balance
                    })
            
            return pd.DataFrame(cash_flow_data)
            
        except Exception as e:
            print(f"Error calculating cash flow timeline: {e}")
            return pd.DataFrame()

    def auto_resize_columns(self, sheet, start_col='A', end_col='K'):
        """Auto-resize columns for better display."""
        try:
            # Auto-resize columns using Google Sheets API
            requests = [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": ord(start_col) - ord('A'),
                        "endIndex": ord(end_col) - ord('A') + 1
                    }
                }
            }]
            
            self.execute_with_retry(
                self.spreadsheet.batch_update,
                {"requests": requests}
            )
            
        except Exception as e:
            print(f"⚠ Warning: Could not auto-resize columns: {e}")

    def update_main_dashboard(self, df: pd.DataFrame, overall_metrics: dict):
        """Update the main dashboard sheet with KPIs - optimized for minimal API calls."""
        try:
            # Clear existing content AND formatting completely with retry
            self.execute_with_retry(self.dashboard_sheet.clear)
            
            # Additional comprehensive clear to ensure no residual formatting remains
            # Clear a large range to handle cases where previous data extended further
            try:
                self.execute_with_retry(
                    self.dashboard_sheet.batch_clear,
                    ["A1:Z200"]  # Clear first 200 rows completely
                )
            except Exception:
                # Fallback: clear row by row if batch clear fails
                pass
            
            # Initialize format queue
            self._format_queue = []
            
            current_row = 1
            
            # Title - single merged cell
            self.execute_with_retry(
                self.dashboard_sheet.update, 
                [['PULLUS LOGISTICS DASHBOARD & METRICS']], 
                f'A{current_row}'
            )
            self.execute_with_retry(self.dashboard_sheet.merge_cells, f'A{current_row}:H{current_row}')
            
            # Only format essential elements - title
            self.format_cell_range(f'A{current_row}:H{current_row}', 
                                 self.colors['header'], bold=True, font_size=16)
            current_row += 1
            
            # Add timestamp with subtle formatting
            timestamp_text = self.get_last_updated_timestamp()
            self.execute_with_retry(
                self.dashboard_sheet.update,
                [[timestamp_text]],
                f'A{current_row}'
            )
            self.execute_with_retry(self.dashboard_sheet.merge_cells, f'A{current_row}:H{current_row}')
            self.format_cell_range(f'A{current_row}:H{current_row}', 
                                 self.colors['timestamp'], 
                                 {'red': 0.4, 'green': 0.4, 'blue': 0.6},  # Soft purple text
                                 font_size=9)
            current_row += 2
            
            # KPI Section - combine header and data in one update
            kpi_section_data = [
                ['KEY PERFORMANCE INDICATORS', '', ''],
                ['Metric', 'Value', 'Description']
            ]
            
            self.execute_with_retry(
                self.dashboard_sheet.update,
                kpi_section_data,
                f'A{current_row}:C{current_row + 1}'
            )
            
            # Format only essential headers
            self.format_cell_range(f'A{current_row}:C{current_row}', 
                                 self.colors['subheader'], bold=True, font_size=12)
            self.format_cell_range(f'A{current_row + 1}:C{current_row + 1}', 
                                 self.colors['primary'], {'red': 1, 'green': 1, 'blue': 1}, bold=True)
            current_row += 2
            
            # KPI Data with explanations (with proper comma formatting)
            kpi_data = [
                ['Average Purchase Cost per Bird', f"₦{overall_metrics.get('avg_purchase_cost_per_bird', 0):,.2f}", "Cost per bird for offtake operations"],
                ['Average Supply Cost per Bird', f"₦{overall_metrics.get('avg_supply_cost_per_bird', 0):,.2f}", "Cost per bird for supply deliveries"],
                ['Total Birds Moved', f"{overall_metrics.get('total_birds_moved', 0):,.0f}", "Total number of birds transported"],
                ['Average Grand Total per Bird', f"₦{overall_metrics.get('avg_grand_total_per_bird', 0):,.2f}", "Overall average cost per bird (all operations)"],
                ['Average Purchase Cost per kg', f"₦{overall_metrics.get('avg_purchase_cost_per_kg', 0):,.2f}", "Cost per kg for offtake operations"],
                ['Average Supply Cost per kg', f"₦{overall_metrics.get('avg_supply_cost_per_kg', 0):,.2f}", "Cost per kg for supply deliveries"],
                ['Total Weight Moved (kg)', f"{overall_metrics.get('total_weight_moved', 0):,.1f}", "Total weight of birds transported"],
                ['Average Grand Total per kg', f"₦{overall_metrics.get('avg_grand_total_per_kg', 0):,.2f}", "Overall average cost per kg (all operations)"],
                ['Average Fuel Cost', f"₦{overall_metrics.get('avg_fuel_cost', 0):,.2f}", "Average fuel cost per trip"],
                ['Third Party Trip Percentage', f"{overall_metrics.get('third_party_percentage', 0):.1f}%", "Percentage of trips using third party transport"],
                ['Current Running Balance', f"₦{overall_metrics.get('current_running_balance', 0):,.2f}", "Current cash position after all transactions (excludes fuel costs)"],
            ]
            
            kpi_start_row = current_row
            self.execute_with_retry(
                self.dashboard_sheet.update,
                kpi_data,
                f'A{current_row}:C{current_row + len(kpi_data) - 1}'
            )
            
            # Minimal formatting - only color the current running balance
            for i, row_data in enumerate(kpi_data):
                row = kpi_start_row + i
                
                # Only color current running balance - be more explicit to avoid any confusion
                if row_data[0] == 'Current Running Balance':
                    running_balance_value = overall_metrics.get('current_running_balance', 0)
                    if running_balance_value > 0:
                        self.format_cell_range(f'B{row}', self.colors['positive'], bold=True)
                    elif running_balance_value < 0:
                        self.format_cell_range(f'B{row}', self.colors['negative'], bold=True)
            
            current_row += len(kpi_data) + 2
            
            # Monthly Breakdown Section - combine headers in one update
            monthly_breakdown = self.calculate_monthly_breakdown(df)
            if not monthly_breakdown.empty:
                monthly_section_data = [
                    ['MONTHLY BREAKDOWN BY MOVEMENT CATEGORY', '', '', '', '', '', '', '', '', ''],
                    ['Month', 'Category', 'Trips', 'Total Birds', 'Total Weight (kg)', 
                     'Total Grand Cost', 'Avg Cost/Bird', 'Avg Cost/kg', 'Cost/kg %', 'Total Logistics Cost']
                ]
                
                self.execute_with_retry(
                    self.dashboard_sheet.update,
                    monthly_section_data,
                    f'A{current_row}:J{current_row + 1}'
                )
                
                # Format only section header and column headers
                self.format_cell_range(f'A{current_row}:J{current_row}', 
                                     self.colors['subheader'], bold=True, font_size=12)
                self.format_cell_range(f'A{current_row + 1}:J{current_row + 1}', 
                                     self.colors['primary'], {'red': 1, 'green': 1, 'blue': 1}, bold=True)
                current_row += 2
                
                # Monthly breakdown data with color separation between months only
                monthly_start_row = current_row
                monthly_data = []
                
                for _, row in monthly_breakdown.iterrows():
                    # Format Cost/kg percentage change only
                    cost_kg_pct = f"{row['Cost_kg % Change']:+.1f}%" if row['Cost_kg % Change'] != 0 else "-"
                    
                    monthly_data.append([
                        row['Month'],
                        row['Category'],
                        int(row['Trips']),
                        f"{row['Total Birds']:,.0f}",
                        f"{row['Total Weight (kg)']:,.1f}",
                        f"₦{row['Total Grand Cost']:,.2f}",
                        f"₦{row['Avg Cost per Bird']:,.2f}",
                        f"₦{row['Avg Cost per kg']:,.2f}",
                        cost_kg_pct,
                        f"₦{row['Total Logistics Cost']:,.2f}"
                    ])
                
                if monthly_data:
                    self.execute_with_retry(
                        self.dashboard_sheet.update,
                        monthly_data, 
                        f'A{current_row}:J{current_row + len(monthly_data) - 1}'
                    )
                    
                    # Add alternating colors for different months (4-color cycle)
                    current_month = None
                    month_color_index = 0
                    month_colors = [
                        self.colors['data'],  # White
                        {'red': 0.94, 'green': 0.97, 'blue': 1.0},  # Light blue
                        {'red': 0.94, 'green': 0.98, 'blue': 0.94},  # Light green
                        {'red': 1.0, 'green': 0.98, 'blue': 0.90},   # Light yellow
                    ]
                    
                    for i, row_data in enumerate(monthly_data):
                        # Check if this is a new month
                        if current_month != row_data[0]:
                            current_month = row_data[0]
                            month_color_index = (month_color_index + 1) % 4
                        
                        row_num = monthly_start_row + i
                        row_data_dict = monthly_breakdown.iloc[i]
                        
                        # Special formatting for MONTH TOTAL rows
                        if row_data_dict.get('Is_Month_Total', False) or 'MONTH TOTAL' in str(row_data[1]):
                            # Bold formatting with darker background for monthly totals
                            self.format_cell_range(f'A{row_num}:J{row_num}', 
                                                 {'red': 0.85, 'green': 0.92, 'blue': 0.95},  # Darker blue for totals
                                                 bold=True, 
                                                 sheet=self.dashboard_sheet)
                        else:
                            # Apply regular month-based coloring to category rows
                            self.format_cell_range(f'A{row_num}:J{row_num}', 
                                                 month_colors[month_color_index], 
                                                 sheet=self.dashboard_sheet)
                        
                        # Color Cost/kg % change (red for increase, green for decrease - unit costs should go down)
                        # Skip percentage changes for month totals since they don't apply
                        if not (row_data_dict.get('Is_Month_Total', False) or 'MONTH TOTAL' in str(row_data[1])):
                            if row_data_dict['Cost_kg % Change'] > 0:
                                self.format_cell_range(f'I{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict['Cost_kg % Change'] < 0:
                                self.format_cell_range(f'I{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease
            
            # Apply all formatting in batch and auto-resize columns
            print("🎨 Applying final formatting and auto-sizing...")
            self.flush_formatting_queue(self.dashboard_sheet)
            self.auto_resize_columns(self.dashboard_sheet, 'A', 'J')
                    
        except Exception as e:
            print(f"✗ Error updating main dashboard: {e}")
    
    
    def update_cash_flow_sheet(self, cash_flow_timeline: pd.DataFrame):
        """Update the cash flow timeline sheet - optimized."""
        try:
            # Clear existing content AND formatting completely with retry
            self.execute_with_retry(self.cash_flow_sheet.clear)
            
            # Additional comprehensive clear to ensure no residual formatting remains
            # Clear a large range to handle cases where previous data extended further
            try:
                self.execute_with_retry(
                    self.cash_flow_sheet.batch_clear,
                    ["A1:Z200"]  # Clear first 200 rows completely
                )
            except Exception:
                # Fallback: clear row by row if batch clear fails
                pass
            
            # Initialize format queue for cash flow sheet
            self._format_queue = []
            
            current_row = 1
            
            # Title
            self.execute_with_retry(
                self.cash_flow_sheet.update,
                [['CASH FLOW TIMELINE - RUNNING BALANCE']],
                f'A{current_row}'
            )
            # Try to merge cells, but handle errors gracefully if already merged
            try:
                self.execute_with_retry(self.cash_flow_sheet.merge_cells, f'A{current_row}:F{current_row}')
            except Exception as merge_error:
                print(f"⚠ Warning: Could not merge title cells (may already be merged): {merge_error}")
            self.format_cell_range(f'A{current_row}:F{current_row}', 
                                 self.colors['header'], bold=True, font_size=16, sheet=self.cash_flow_sheet)
            current_row += 1
            
            # Add timestamp with subtle formatting
            timestamp_text = self.get_last_updated_timestamp()
            self.execute_with_retry(
                self.cash_flow_sheet.update,
                [[timestamp_text]],
                f'A{current_row}'
            )
            try:
                self.execute_with_retry(self.cash_flow_sheet.merge_cells, f'A{current_row}:F{current_row}')
            except Exception:
                pass  # Handle merge errors gracefully
            self.format_cell_range(f'A{current_row}:F{current_row}', 
                                 self.colors['timestamp'], 
                                 {'red': 0.4, 'green': 0.4, 'blue': 0.6},  # Soft purple text
                                 font_size=9, sheet=self.cash_flow_sheet)
            current_row += 2
            
            if not cash_flow_timeline.empty:
                # Cash flow headers
                cash_flow_headers = [['Date', 'Transaction Type', 'From', 'To', 'Amount', 'Running Balance']]
                self.execute_with_retry(
                    self.cash_flow_sheet.update,
                    cash_flow_headers,
                    f'A{current_row}:F{current_row}'
                )
                self.format_cell_range(f'A{current_row}:F{current_row}', 
                                     self.colors['primary'], {'red': 1, 'green': 1, 'blue': 1}, 
                                     bold=True, sheet=self.cash_flow_sheet)
                current_row += 1
                
                # Cash flow data
                cash_flow_start_row = current_row
                cash_flow_data = []
                for _, row in cash_flow_timeline.iterrows():
                    cash_flow_data.append([
                        row['Date'],
                        row['Transaction Type'],
                        row['From'],
                        row['To'],
                        f"₦{row['Amount']:,.2f}",
                        f"₦{row['Running Balance']:,.2f}"
                    ])
                
                if cash_flow_data:
                    self.execute_with_retry(
                        self.cash_flow_sheet.update,
                        cash_flow_data, 
                        f'A{current_row}:F{current_row + len(cash_flow_data) - 1}'
                    )
                    
                    # Format only critical cells - Fund additions and final balance
                    for i in range(len(cash_flow_data)):
                        row = cash_flow_start_row + i
                        transaction_type = cash_flow_timeline.iloc[i]['Transaction Type']
                        running_balance = cash_flow_timeline.iloc[i]['Running Balance']
                        
                        # Only color fund additions (NOT expenses)
                        if transaction_type == 'Fund Addition':
                            self.format_cell_range(f'A{row}:F{row}', self.colors['positive'], sheet=self.cash_flow_sheet)
                        # Explicitly do NOT color expense rows - leave them with default formatting
                        
                        # Color the running balance column based on positive/negative
                        if running_balance < 0:
                            self.format_cell_range(f'F{row}', self.colors['negative'], bold=True, sheet=self.cash_flow_sheet)
                        elif running_balance > 0 and i == len(cash_flow_data) - 1:  # Only highlight the very final balance
                            self.format_cell_range(f'F{row}', self.colors['positive'], bold=True, sheet=self.cash_flow_sheet)
            
            # Apply all formatting and auto-resize columns
            print("💰 Finalizing cash flow sheet...")
            self.flush_formatting_queue(self.cash_flow_sheet)
            self.auto_resize_columns(self.cash_flow_sheet, 'A', 'F')
                        
        except Exception as e:
            print(f"✗ Error updating cash flow sheet: {e}")
    
    def update_dashboard(self, df: pd.DataFrame):
        """Update all dashboard sheets."""
        try:
            print("🔄 Updating all dashboard sheets...")
            
            # Calculate metrics
            overall_metrics = self.calculate_overall_metrics(df)
            cash_flow_timeline = self.calculate_cash_flow_timeline(df)
            
            # Update each sheet
            print("📊 Updating main dashboard with monthly breakdown...")
            self.update_main_dashboard(df, overall_metrics)
            
            print("💰 Updating cash flow timeline...")
            self.update_cash_flow_sheet(cash_flow_timeline)
            
            print("✅ All sheets updated successfully with colorful formatting!")
            
        except Exception as e:
            print(f"✗ Error updating dashboard: {e}")
    
    def run_update(self):
        """Main method to run the complete dashboard update."""
        try:
            print("🚀 Starting Pullus Logistics Dashboard Update...")
            print("=" * 60)
            
            # Connect to Google Sheets
            if not self.connect():
                return False
            
            # Initialize rate limiting
            print("⚡ Initializing with intelligent rate limiting...")
            self.request_count = 0
            self.minute_start = time.time()
            
            # Read and process data
            print("📊 Reading logistics data...")
            df = self.read_logistics_data()
            
            if df.empty:
                print("❌ No data found. Update cancelled.")
                return False
            
            # Processing calculations
            print("⚡ Processing calculations with smart API management...")
            # Small delay to let any previous operations complete
            time.sleep(0.5)
            
            # Update dashboard
            self.update_dashboard(df)
            
            print("=" * 60)
            print("🎉 Dashboard update completed successfully!")
            print(f"📈 Processed {len(df)} records")
            if len(df) > 0:
                print(f"📅 Data processing completed successfully")
            
            return True
            
        except Exception as e:
            print(f"❌ Error during update process: {e}")
            return False

if __name__ == "__main__":
    # Configuration - use environment variables in CI/CD, fallback to local files
    credentials_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE') or "pullus-pipeline-40a5302e034d.json"
    spreadsheet_id = os.getenv('SPREADSHEET_ID') or "1m9gF4396C5qshf7jhYMboHFGUTC9dbaQqMI4R4Tk9jY"
    
    # Create updater instance
    updater = LogisticsDashboardUpdater(credentials_path, spreadsheet_id)
    
    # Run the update
    success = updater.run_update()
    
    if success:
        print(f"\n✨ Results written to 2 sheets:")
        print(f"   📊 '{updater.new_sheet_name}' - KPIs & Monthly Breakdown")
        print(f"   💰 '{updater.cash_flow_sheet_name}' - Running Balance Timeline")
    else:
        print("\n❌ Update failed. Please check the error messages above.")