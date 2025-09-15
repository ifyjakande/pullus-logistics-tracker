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

# Load environment variables from .env file if it exists
def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    os.environ[key.strip()] = value.strip()

# Load .env file at module import
load_env_file()

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
        
        # Rate limiting configuration - more conservative settings
        self.max_retries = 8  # Increased retries
        self.base_delay = 2.0  # Start with longer delay
        self.max_delay = 120.0  # Increased maximum delay
        self.requests_per_minute = 90  # Optimized for 2025 API limits (100/100s with buffer)
        self.request_count = 0
        self.minute_start = time.time()

        # Benchmark configuration
        self.cost_benchmarks = {
            'offtake_cost_per_kg': 40,              # ₦40/kg for purchase operations
            'supply_cost_per_kg': 80,               # ₦80/kg for delivery operations
            'kaduna_to_abuja_supply_per_kg': 50,    # ₦50/kg Kaduna→Abuja deliveries
            'abuja_internal_supply_per_kg': 30,     # ₦30/kg internal Abuja deliveries
        }
    
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
        """Check and enforce rate limits with more conservative approach."""
        current_time = time.time()

        # Reset counter if a minute has passed
        if current_time - self.minute_start > 60:
            self.request_count = 0
            self.minute_start = current_time

        # More conservative approach - wait if approaching limit with larger buffer
        if self.request_count >= self.requests_per_minute - 15:  # Larger buffer of 15 requests
            sleep_time = 60 - (current_time - self.minute_start) + 2  # Extra 2 second buffer
            if sleep_time > 0:
                print(f"⏳ Rate limit approaching, waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)
                self.request_count = 0
                self.minute_start = time.time()

        # Add a small delay between all requests to be extra conservative
        elif self.request_count > 0 and self.request_count % 10 == 0:
            time.sleep(0.1)  # Minimal pause every 10 requests
            print(f"⏳ Proactive rate limiting pause after {self.request_count} requests...")

        self.request_count += 1
    
    def execute_with_retry(self, func, *args, **kwargs):
        """Execute a function with exponential backoff retry logic."""
        for attempt in range(self.max_retries):
            try:
                self.rate_limit_check()
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                error_str = str(e).lower()
                is_rate_limit_error = (
                    '429' in str(e) or
                    'quota' in error_str or
                    'rate' in error_str or
                    'too many requests' in error_str or
                    'limit exceeded' in error_str or
                    'service temporarily overloaded' in error_str
                )

                if is_rate_limit_error:
                    if attempt < self.max_retries - 1:
                        # More aggressive exponential backoff with jitter
                        delay = min(self.base_delay * (2 ** attempt) + 1.0, self.max_delay)
                        print(f"🔄 Rate limit hit (attempt {attempt + 1}/{self.max_retries}), waiting {delay:.1f}s...")
                        time.sleep(delay)

                        # Reset request counter after a rate limit hit
                        self.request_count = 0
                        self.minute_start = time.time()
                        continue
                    else:
                        print(f"❌ Max retries exceeded for rate limiting. Trying to continue with partial success...")
                        # Instead of raising, return None to allow partial completion
                        return None
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
        """Normalize location names for consistent comparison with smart fuzzy matching."""
        if not location or pd.isna(location):
            return ""
        # Clean and normalize
        normalized = str(location).upper().strip()
        # Remove common punctuation and extra spaces
        normalized = re.sub(r'[.,;\\-_]+', ' ', normalized)
        normalized = re.sub(r'\s+', ' ', normalized).strip()

        # Apply smart fuzzy matching for common locations
        normalized = self._apply_smart_location_matching(normalized)
        return normalized

    def _apply_smart_location_matching(self, location: str) -> str:
        """Apply intelligent fuzzy matching using Levenshtein distance and similarity scoring."""
        if not location or len(location) < 2:
            return location

        # Define master locations with their common aliases
        master_locations = {
            'ABUJA': ['ABUJA', 'FCT', 'FEDERAL CAPITAL TERRITORY', 'AMAC'],
            'KADUNA': ['KADUNA'],
            'COLD ROOM': ['COLD ROOM', 'COLDROOM', 'COLD STORAGE', 'COOLING ROOM'],
            'KANO': ['KANO'],
            'JOS': ['JOS', 'PLATEAU'],
            'MARARABA': ['MARARABA', 'MARABA'],
            'NASARAWA': ['NASARAWA'],
            'LAGOS': ['LAGOS', 'LASGIDI'],
            'ZARIA': ['ZARIA'],
            'ILORIN': ['ILORIN'],
            'BAUCHI': ['BAUCHI'],
            'SOKOTO': ['SOKOTO']
        }

        best_match = None
        best_similarity = 0.0
        similarity_threshold = 0.75  # 75% similarity required

        for standard_name, aliases in master_locations.items():
            for alias in aliases:
                # Calculate similarity using multiple methods
                similarity = max(
                    self._calculate_similarity(location, alias),
                    self._calculate_similarity(location, standard_name),
                    self._substring_similarity(location, alias),
                    self._substring_similarity(location, standard_name)
                )

                # Special handling for exact substring matches
                if alias in location or location in alias:
                    similarity = max(similarity, 0.9)

                if similarity > best_similarity and similarity >= similarity_threshold:
                    best_similarity = similarity
                    best_match = standard_name

        return best_match if best_match else location

    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using Levenshtein distance."""
        if str1 == str2:
            return 1.0

        # Handle edge cases
        if not str1 or not str2:
            return 0.0

        # Calculate Levenshtein distance
        len1, len2 = len(str1), len(str2)
        if len1 < len2:
            str1, str2 = str2, str1
            len1, len2 = len2, len1

        # Dynamic programming matrix
        prev_row = list(range(len2 + 1))
        for i, c1 in enumerate(str1):
            curr_row = [i + 1]
            for j, c2 in enumerate(str2):
                insertions = prev_row[j + 1] + 1
                deletions = curr_row[j] + 1
                substitutions = prev_row[j] + (c1 != c2)
                curr_row.append(min(insertions, deletions, substitutions))
            prev_row = curr_row

        # Convert distance to similarity (0-1 scale)
        max_len = max(len1, len2)
        return 1.0 - (prev_row[-1] / max_len)

    def _substring_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity based on common substrings and character overlap."""
        if not str1 or not str2:
            return 0.0

        # Calculate character frequency overlap
        chars1 = set(str1)
        chars2 = set(str2)
        overlap = len(chars1.intersection(chars2))
        total = len(chars1.union(chars2))
        char_similarity = overlap / total if total > 0 else 0.0

        # Calculate longest common substring ratio
        def longest_common_substring(s1, s2):
            longest = 0
            for i in range(len(s1)):
                for j in range(len(s2)):
                    k = 0
                    while (i + k < len(s1) and j + k < len(s2) and
                           s1[i + k] == s2[j + k]):
                        k += 1
                    longest = max(longest, k)
            return longest

        lcs_len = longest_common_substring(str1, str2)
        lcs_similarity = (2.0 * lcs_len) / (len(str1) + len(str2)) if (len(str1) + len(str2)) > 0 else 0.0

        # Return weighted average
        return (char_similarity * 0.3) + (lcs_similarity * 0.7)
    
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
            elif any(pattern in location for pattern in ['COLDROOM', 'COLDROM', 'COLROOM', 'COLDROOOM']):
                return 'COLD ROOM'  # Group cold room typos
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

    def calculate_product_unit_economics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate product-specific unit economics with smart cost allocation."""
        # Define product weight columns (excluding crates which use count)
        # Note: Using actual column names from sheet - will automatically adapt to column changes
        product_weight_columns = [
            'Gizzard Weight', 'Whole Chicken Weight', 'Laps Weight',
            'Breast Weight', 'Fillet Weight', 'Wings Weight', 'Bone Weight'
        ]

        # Crate space equivalent factor (configurable)
        CRATE_SPACE_FACTOR = 2  # 1 crate = 2kg equivalent space

        # Initialize product-specific cost columns
        for product in product_weight_columns:
            product_name = product.replace(' Weight', '').replace(' ', '_')
            df[f'{product_name}_Allocated_Cost'] = 0.0
            df[f'{product_name}_Cost_per_kg'] = 0.0

        # Add bird and crate specific columns
        df['Whole_Chicken_Cost_per_Bird'] = 0.0
        df['Egg_Allocated_Cost'] = 0.0
        df['Egg_Cost_per_Crate'] = 0.0

        # Process each row for cost allocation
        for idx, row in df.iterrows():
            grand_total = row['Grand Total Cost']
            if grand_total <= 0:
                continue

            # Calculate actual weight from all products
            total_product_weight = sum(row[col] for col in product_weight_columns if row[col] > 0)

            # Handle different scenarios
            has_crates = row['Number of Crates'] > 0
            has_birds = row['Number of Birds'] > 0
            has_weights = total_product_weight > 0

            if has_crates and has_weights:
                # Mixed shipment: weighted products + crates
                crate_equivalent_weight = row['Number of Crates'] * CRATE_SPACE_FACTOR
                total_equivalent_weight = total_product_weight + crate_equivalent_weight

                if total_equivalent_weight > 0:
                    # Allocate cost to eggs based on equivalent space
                    egg_allocated_cost = (crate_equivalent_weight / total_equivalent_weight) * grand_total
                    df.loc[idx, 'Egg_Allocated_Cost'] = egg_allocated_cost
                    df.loc[idx, 'Egg_Cost_per_Crate'] = egg_allocated_cost / row['Number of Crates']

                    # Remaining cost for weighted products
                    remaining_cost = grand_total - egg_allocated_cost
                    self._allocate_weight_based_costs(df, idx, row, remaining_cost, total_product_weight, product_weight_columns)

            elif has_crates and not has_weights:
                # Pure egg shipment
                df.loc[idx, 'Egg_Allocated_Cost'] = grand_total
                df.loc[idx, 'Egg_Cost_per_Crate'] = grand_total / row['Number of Crates']

            elif has_weights:
                # Pure weighted products shipment
                self._allocate_weight_based_costs(df, idx, row, grand_total, total_product_weight, product_weight_columns)

        return df

    def _allocate_weight_based_costs(self, df: pd.DataFrame, idx: int, row: pd.Series,
                                   total_cost: float, total_weight: float,
                                   product_weight_columns: list):
        """Allocate costs among weight-based products."""
        if total_weight <= 0:
            return

        # Allocate cost proportionally by weight
        for product_col in product_weight_columns:
            product_weight = row[product_col]
            if product_weight > 0:
                product_name = product_col.replace(' Weight', '').replace(' ', '_')
                allocated_cost = (product_weight / total_weight) * total_cost

                df.loc[idx, f'{product_name}_Allocated_Cost'] = allocated_cost
                df.loc[idx, f'{product_name}_Cost_per_kg'] = allocated_cost / product_weight

                # Special handling for whole chicken - calculate cost per bird
                if product_col == 'Whole Chicken Weight' and row['Number of Birds'] > 0:
                    df.loc[idx, 'Whole_Chicken_Cost_per_Bird'] = allocated_cost / row['Number of Birds']

    def read_logistics_data(self) -> pd.DataFrame:
        """Read and process data from the Logistics Data sheet."""
        try:
            # Get all data from the sheet
            all_data = self.data_sheet.get_all_values()
            
            # Headers are in row 3 (index 2) - use actual column names from the sheet
            headers = all_data[2]  # Row 3: actual headers

            # Clean up headers (remove empty strings and strip whitespace)
            cleaned_headers = []
            for header in headers:
                if header and header.strip():
                    cleaned_headers.append(header.strip())
                else:
                    cleaned_headers.append("")

            # Use the actual headers from the sheet
            print(f"📋 Found {len(cleaned_headers)} columns in source data")
            
            # Data starts from row 4 (index 3)
            data_rows = all_data[3:]

            # Create DataFrame using actual column names from the sheet
            df_data = []
            for row in data_rows:
                if len(row) > 0 and row[0]:  # Skip empty rows
                    row_dict = {}
                    for col_idx, header in enumerate(cleaned_headers):
                        if col_idx < len(row):
                            # Use the actual header name, or create a generic name for empty headers
                            col_name = header if header else f"Column_{col_idx}"
                            row_dict[col_name] = row[col_idx]
                        else:
                            col_name = header if header else f"Column_{col_idx}"
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
            numeric_columns = ['Number of Birds', 'Number of Crates', 'Gizzard Weight',
                             'Whole Chicken Weight', 'Laps Weight', 'Breast Weight',
                             'Fillet Weight', 'Wings Weight', 'Bone Weight',
                             'Total Weight (kg)', 'Added Funds', 'Logistics Cost',
                             'Fuel Cost', 'Miscellaneous Cost']
            
            for col in numeric_columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            
            # Calculate Grand Total Cost (Logistics + Fuel + Miscellaneous)
            df['Grand Total Cost'] = (
                df['Logistics Cost'] +
                df['Fuel Cost'] +
                df['Miscellaneous Cost']
            )

            # Add product-specific unit economics calculations
            df = self.calculate_product_unit_economics(df)

            # Legacy calculations for backward compatibility (now using Grand Total Cost)
            df['Cost per Bird'] = df['Whole_Chicken_Cost_per_Bird']  # Use whole chicken cost per bird
            df['Cost per kg'] = df.apply(
                lambda row: row['Grand Total Cost'] / row['Total Weight (kg)']
                if row['Total Weight (kg)'] > 0 and row['Grand Total Cost'] > 0 else 0, axis=1
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

            # Add benchmark analysis
            df = self.calculate_benchmark_violations(df)

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
        
        # Simplified approach - no longer need complex metro area detection
        
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
            # Simplified logic: Any supply from Abuja = Abuja Internal Supply
            if from_loc_std.startswith('ABUJA') or 'ABUJA' in from_loc_std:
                return 'Abuja Internal Supply'
            # Check if it's an internal route (same standardized location)
            elif from_loc_std == to_loc_std:
                return f'Supply - {from_display} Internal'
            else:
                # Create specific route-based category
                return f'Supply - {from_display} to {to_display}'
        
        return 'Uncategorized'
    
    def calculate_overall_metrics(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate overall KPIs for the dashboard including product-specific metrics."""
        try:
            # Filter out rows with zero values for averages
            df_with_data = df[df['Grand Total Cost'] > 0]
            df_offtake = df_with_data[df_with_data['Logistics_Type_Normalized'] == 'OFFTAKE']
            df_supply = df_with_data[df_with_data['Logistics_Type_Normalized'] == 'SUPPLY']
            # Use our new intelligent Abuja detection instead of manual "Is Abuja" flag
            df_abuja_supply = df_supply[df_supply['Movement Category'] == 'Abuja Internal Supply']

            # FIX: Apply filtering to ensure accurate per-unit calculations in overall metrics
            df_offtake_with_birds = df_offtake[df_offtake['Number of Birds'] > 0]
            df_supply_with_birds = df_supply[df_supply['Number of Birds'] > 0]
            df_abuja_supply_with_birds = df_abuja_supply[df_abuja_supply['Number of Birds'] > 0]
            df_with_data_and_birds = df_with_data[df_with_data['Number of Birds'] > 0]

            df_offtake_with_weight = df_offtake[(df_offtake['Total Weight (kg)'] > 0) & (df_offtake['Grand Total Cost'] > 0)]
            df_supply_with_weight = df_supply[(df_supply['Total Weight (kg)'] > 0) & (df_supply['Grand Total Cost'] > 0)]
            df_abuja_supply_with_weight = df_abuja_supply[(df_abuja_supply['Total Weight (kg)'] > 0) & (df_abuja_supply['Grand Total Cost'] > 0)]
            df_with_data_and_weight = df_with_data[(df_with_data['Total Weight (kg)'] > 0) & (df_with_data['Grand Total Cost'] > 0)]

            metrics = {
                # Traditional bird-based metrics (whole chicken focus) - FIXED to use allocated costs for purchase/supply cost per bird
                'avg_purchase_cost_per_bird': self._safe_division((df_offtake_with_birds['Whole_Chicken_Cost_per_Bird'] * df_offtake_with_birds['Number of Birds']).sum(), df_offtake_with_birds['Number of Birds'].sum()),
                'avg_supply_cost_per_bird': self._safe_division((df_supply_with_birds['Whole_Chicken_Cost_per_Bird'] * df_supply_with_birds['Number of Birds']).sum(), df_supply_with_birds['Number of Birds'].sum()),
                'avg_abuja_supply_cost_per_bird': self._safe_division(df_abuja_supply_with_birds['Grand Total Cost'].sum(), df_abuja_supply_with_birds['Number of Birds'].sum()),
                'total_birds_moved': df['Number of Birds'].sum(),
                'avg_grand_total_per_bird': self._safe_division(df_with_data_and_birds['Grand Total Cost'].sum(), df_with_data_and_birds['Number of Birds'].sum()),

                # Traditional weight-based metrics - FIXED to use allocated costs for purchase/supply cost per kg
                # Calculate weighted average of all product costs per kg for each record
                'avg_purchase_cost_per_kg': self._calculate_weighted_avg_cost_per_kg(df_offtake_with_weight),
                'avg_supply_cost_per_kg': self._calculate_weighted_avg_cost_per_kg(df_supply_with_weight),
                'avg_abuja_supply_cost_per_kg': self._safe_division(df_abuja_supply_with_weight['Grand Total Cost'].sum(), df_abuja_supply_with_weight['Total Weight (kg)'].sum()),
                'total_weight_moved': df['Total Weight (kg)'].sum(),
                'avg_grand_total_per_kg': self._safe_division(df_with_data_and_weight['Grand Total Cost'].sum(), df_with_data_and_weight['Total Weight (kg)'].sum()),

                # NEW: Product-specific metrics - FIXED to use weighted averages
                'total_crates_moved': df['Number of Crates'].sum(),
                'avg_egg_cost_per_crate': self._calculate_weighted_avg_cost_per_crate(df),

                # Product-specific cost per kg metrics - FIXED to use weighted averages
                'avg_gizzard_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Gizzard'),
                'avg_whole_chicken_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Whole Chicken'),
                'avg_laps_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Laps'),
                'avg_breast_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Breast'),
                'avg_fillet_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Fillet'),
                'avg_wings_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Wings'),
                'avg_bone_cost_per_kg': self._calculate_weighted_avg_for_product(df, 'Bone'),

                # Whole chicken specific (bird count correlation) - FIXED to use weighted average
                'avg_whole_chicken_cost_per_bird': self._calculate_weighted_avg_cost_per_bird(df),

                # Product weight totals
                'total_gizzard_weight': df['Gizzard Weight'].sum(),
                'total_whole_chicken_weight': df['Whole Chicken Weight'].sum(),
                'total_laps_weight': df['Laps Weight'].sum(),
                'total_breast_weight': df['Breast Weight'].sum(),
                'total_fillet_weight': df['Fillet Weight'].sum(),
                'total_wings_weight': df['Wings Weight'].sum(),
                'total_bone_weight': df['Bone Weight'].sum(),

                # General metrics
                'avg_fuel_cost': df[df['Fuel Cost'] > 0]['Fuel Cost'].mean() if len(df[df['Fuel Cost'] > 0]) > 0 else 0,
                'third_party_percentage': (len(df[df['Transportation Mode'] == 'Third Party']) / len(df) * 100) if len(df) > 0 else 0,
                'current_running_balance': self.get_current_running_balance(df),
            }

            # NEW: Benchmark performance metrics
            benchmark_metrics = self.calculate_benchmark_performance(df)
            metrics.update(benchmark_metrics)

            return metrics

        except Exception as e:
            print(f"Error calculating overall metrics: {e}")
            return {}

    def calculate_benchmark_performance(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate benchmark performance metrics."""
        try:
            # Filter rows with applicable benchmarks and costs
            df_with_benchmarks = df[(df['Applicable_Benchmark'] != '') & (df['Grand Total Cost'] > 0)]

            if len(df_with_benchmarks) == 0:
                return {}

            benchmark_metrics = {}

            # Overall compliance rate
            total_with_benchmarks = len(df_with_benchmarks)
            within_benchmark = len(df_with_benchmarks[df_with_benchmarks['Benchmark_Status'] == 'Within'])
            near_benchmark = len(df_with_benchmarks[df_with_benchmarks['Benchmark_Status'] == 'Near'])
            exceeded_benchmark = len(df_with_benchmarks[df_with_benchmarks['Benchmark_Status'] == 'Exceeded'])

            benchmark_metrics['overall_compliance_rate'] = (within_benchmark / total_with_benchmarks * 100) if total_with_benchmarks > 0 else 0
            benchmark_metrics['near_benchmark_rate'] = (near_benchmark / total_with_benchmarks * 100) if total_with_benchmarks > 0 else 0
            benchmark_metrics['violation_rate'] = (exceeded_benchmark / total_with_benchmarks * 100) if total_with_benchmarks > 0 else 0

            # Specific benchmark performance
            for benchmark_type in ['offtake_cost_per_kg', 'supply_cost_per_kg', 'kaduna_to_abuja_supply_per_kg', 'abuja_internal_supply_per_kg']:
                benchmark_data = df_with_benchmarks[df_with_benchmarks['Applicable_Benchmark'] == benchmark_type]

                if len(benchmark_data) > 0:
                    within_count = len(benchmark_data[benchmark_data['Benchmark_Status'] == 'Within'])
                    compliance_rate = (within_count / len(benchmark_data) * 100)

                    # Average overage for this benchmark type
                    violations = benchmark_data[benchmark_data['Benchmark_Status'] == 'Exceeded']
                    avg_overage = violations['Benchmark_Variance_Percent'].mean() if len(violations) > 0 else 0

                    benchmark_metrics[f'{benchmark_type}_compliance_rate'] = compliance_rate
                    benchmark_metrics[f'{benchmark_type}_avg_overage'] = avg_overage
                    benchmark_metrics[f'{benchmark_type}_violation_count'] = len(violations)

            # Overall average overage
            violations_all = df_with_benchmarks[df_with_benchmarks['Benchmark_Status'] == 'Exceeded']
            benchmark_metrics['overall_avg_overage'] = violations_all['Benchmark_Variance_Percent'].mean() if len(violations_all) > 0 else 0

            return benchmark_metrics

        except Exception as e:
            print(f"Error calculating benchmark performance: {e}")
            return {}

    def get_current_violations(self, df: pd.DataFrame) -> list:
        """Get current month's benchmark violations sorted by severity."""
        try:
            # Get current month data
            current_month = datetime.now().strftime('%Y-%m')
            current_month_data = df[df['Month_Year'] == current_month]

            # Filter violations
            violations = current_month_data[current_month_data['Benchmark_Status'] == 'Exceeded']

            if len(violations) == 0:
                return []

            # Format violations
            violation_list = []
            for _, row in violations.iterrows():
                # FIX: Use allocated cost per kg instead of raw cost for accurate benchmark comparison
                actual_cost_per_kg = self._get_allocated_cost_per_kg_for_row(row)

                violation_list.append({
                    'date': row['Date'].strftime('%Y-%m-%d') if pd.notna(row['Date']) else '',
                    'route': f"{row['From']} → {row['To']}",
                    'actual_cost': actual_cost_per_kg,
                    'benchmark': row['Benchmark_Value'],
                    'overage_percent': row['Benchmark_Variance_Percent']
                })

            # Sort by overage percentage (most severe first)
            violation_list.sort(key=lambda x: x['overage_percent'], reverse=True)

            return violation_list

        except Exception as e:
            print(f"Error getting current violations: {e}")
            return []

    def _safe_division(self, numerator: float, denominator: float) -> float:
        """Safely divide two numbers, returning 0 if denominator is 0."""
        return numerator / denominator if denominator > 0 else 0

    def _safe_avg(self, series) -> float:
        """Safely calculate average, returning 0 if series is empty."""
        return series.mean() if len(series) > 0 else 0

    def _calculate_weighted_avg_cost_per_kg(self, df) -> float:
        """Calculate weighted average cost per kg using allocated costs for all products."""
        if len(df) == 0:
            return 0

        # Get all product types with weights
        products = ['Gizzard', 'Whole Chicken', 'Laps', 'Breast', 'Fillet', 'Wings', 'Bone']

        total_allocated_cost = 0
        total_weight = 0

        for _, row in df.iterrows():
            for product in products:
                weight_col = f'{product} Weight'
                cost_per_kg_col = f'{product}_Cost_per_kg'

                if weight_col in row and cost_per_kg_col in row:
                    weight = row[weight_col] if pd.notna(row[weight_col]) else 0
                    cost_per_kg = row[cost_per_kg_col] if pd.notna(row[cost_per_kg_col]) else 0

                    if weight > 0 and cost_per_kg > 0:
                        total_allocated_cost += weight * cost_per_kg
                        total_weight += weight

        return total_allocated_cost / total_weight if total_weight > 0 else 0

    def _calculate_weighted_avg_for_product(self, df, product_name) -> float:
        """Calculate weighted average cost per kg for a specific product."""
        if len(df) == 0:
            return 0

        weight_col = f'{product_name} Weight'
        cost_per_kg_col = f'{product_name.replace(" ", "_")}_Cost_per_kg'

        # Filter records that have this product
        product_data = df[(df[weight_col] > 0) & (df[cost_per_kg_col] > 0)]

        if len(product_data) == 0:
            return 0

        # Calculate weighted average: sum(weight × cost_per_kg) / sum(weight)
        total_cost = (product_data[weight_col] * product_data[cost_per_kg_col]).sum()
        total_weight = product_data[weight_col].sum()

        return total_cost / total_weight if total_weight > 0 else 0

    def _calculate_weighted_avg_cost_per_bird(self, df) -> float:
        """Calculate weighted average cost per bird for whole chicken."""
        if len(df) == 0:
            return 0

        # Filter records that have birds and cost per bird
        bird_data = df[(df['Number of Birds'] > 0) & (df['Whole_Chicken_Cost_per_Bird'] > 0)]

        if len(bird_data) == 0:
            return 0

        # Calculate weighted average: sum(birds × cost_per_bird) / sum(birds)
        total_cost = (bird_data['Number of Birds'] * bird_data['Whole_Chicken_Cost_per_Bird']).sum()
        total_birds = bird_data['Number of Birds'].sum()

        return total_cost / total_birds if total_birds > 0 else 0

    def _calculate_weighted_avg_cost_per_crate(self, df) -> float:
        """Calculate weighted average cost per crate for eggs."""
        if len(df) == 0:
            return 0

        # Filter records that have crates and cost per crate
        crate_data = df[(df['Number of Crates'] > 0) & (df['Egg_Cost_per_Crate'] > 0)]

        if len(crate_data) == 0:
            return 0

        # Calculate weighted average: sum(crates × cost_per_crate) / sum(crates)
        total_cost = (crate_data['Number of Crates'] * crate_data['Egg_Cost_per_Crate']).sum()
        total_crates = crate_data['Number of Crates'].sum()

        return total_cost / total_crates if total_crates > 0 else 0

    def _get_allocated_cost_per_kg_for_row(self, row) -> float:
        """Calculate allocated cost per kg for a single row using weighted average of all products."""
        if row['Total Weight (kg)'] <= 0:
            return 0

        # Get all product types with weights
        products = ['Gizzard', 'Whole_Chicken', 'Laps', 'Breast', 'Fillet', 'Wings', 'Bone']

        total_allocated_cost = 0
        total_weight = 0

        for product in products:
            weight_col = f'{product.replace(" ", "_")} Weight'
            cost_per_kg_col = f'{product.replace(" ", "_")}_Cost_per_kg'

            if weight_col in row and cost_per_kg_col in row:
                weight = row[weight_col] if pd.notna(row[weight_col]) else 0
                cost_per_kg = row[cost_per_kg_col] if pd.notna(row[cost_per_kg_col]) else 0

                if weight > 0 and cost_per_kg > 0:
                    total_allocated_cost += weight * cost_per_kg
                    total_weight += weight

        return total_allocated_cost / total_weight if total_weight > 0 else 0

    def get_applicable_benchmarks(self, row) -> list:
        """Determine which benchmarks apply to a given logistics row."""
        benchmarks = []

        # Get movement category if available
        category = row.get('Movement Category', '')

        # Route-specific benchmarks (most specific first)
        if (self.normalize_location(row['From']) == 'KADUNA' and
            self.normalize_location(row['To']) == 'ABUJA' and
            row['Logistics_Type_Normalized'] == 'SUPPLY'):
            benchmarks.append(('kaduna_to_abuja_supply_per_kg', self.cost_benchmarks['kaduna_to_abuja_supply_per_kg']))

        elif row['Movement Category'] == 'Abuja Internal Supply':
            benchmarks.append(('abuja_internal_supply_per_kg', self.cost_benchmarks['abuja_internal_supply_per_kg']))

        # AUTOMATIC SUPPLY CATEGORY DETECTION (works for any supply location)
        elif ('Supply' in category or 'Internal' in category):
            # This is a supply category - use supply benchmark
            benchmarks.append(('supply_cost_per_kg', self.cost_benchmarks['supply_cost_per_kg']))

        # General operation benchmarks
        elif row['Logistics_Type_Normalized'] == 'OFFTAKE':
            benchmarks.append(('offtake_cost_per_kg', self.cost_benchmarks['offtake_cost_per_kg']))

        elif row['Logistics_Type_Normalized'] == 'SUPPLY':
            benchmarks.append(('supply_cost_per_kg', self.cost_benchmarks['supply_cost_per_kg']))

        return benchmarks

    def calculate_benchmark_violations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add benchmark analysis columns to the dataframe."""
        # Initialize benchmark columns
        df['Applicable_Benchmark'] = ''
        df['Benchmark_Value'] = 0.0
        df['Benchmark_Variance_Percent'] = 0.0
        df['Benchmark_Status'] = ''
        df['Is_Violation'] = False

        for idx, row in df.iterrows():
            if row['Grand Total Cost'] <= 0 or row['Total Weight (kg)'] <= 0:
                continue

            # Calculate actual allocated cost per kg (not raw transportation cost)
            actual_cost_per_kg = self._get_allocated_cost_per_kg_for_row(row)

            # Get applicable benchmarks
            applicable_benchmarks = self.get_applicable_benchmarks(row)

            if applicable_benchmarks:
                # Use the first (most specific) applicable benchmark
                benchmark_name, benchmark_value = applicable_benchmarks[0]

                # Calculate variance
                variance_percent = ((actual_cost_per_kg - benchmark_value) / benchmark_value) * 100

                # Determine status
                if variance_percent <= 5:
                    status = 'Within'
                elif variance_percent <= 20:
                    status = 'Near'
                else:
                    status = 'Exceeded'

                # Update dataframe
                df.loc[idx, 'Applicable_Benchmark'] = benchmark_name
                df.loc[idx, 'Benchmark_Value'] = benchmark_value
                df.loc[idx, 'Benchmark_Variance_Percent'] = variance_percent
                df.loc[idx, 'Benchmark_Status'] = status
                df.loc[idx, 'Is_Violation'] = variance_percent > 20  # Violation if >20% over

        return df

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

    def _get_category_benchmark_info(self, cat_data: pd.DataFrame) -> dict:
        """Get benchmark information for a category of data."""
        if len(cat_data) == 0:
            return {'display': '', 'status': ''}

        # Get the most common benchmark for this category
        benchmarks = cat_data[cat_data['Applicable_Benchmark'] != '']['Applicable_Benchmark'].value_counts()
        if len(benchmarks) == 0:
            return {'display': '', 'status': ''}

        most_common_benchmark = benchmarks.index[0]
        benchmark_value = cat_data[cat_data['Applicable_Benchmark'] == most_common_benchmark]['Benchmark_Value'].iloc[0]

        # Calculate status based on AGGREGATED costs (not individual transactions)
        status = ''
        aggregated_cost = 0

        # Determine which aggregated cost to use based on benchmark type
        if most_common_benchmark == 'offtake_cost_per_kg':
            # Use aggregated purchase/offtake cost
            offtake_data = cat_data[cat_data['Logistics_Type_Normalized'] == 'OFFTAKE']
            if len(offtake_data) > 0:
                offtake_with_weight_and_cost = offtake_data[(offtake_data['Total Weight (kg)'] > 0) & (offtake_data['Grand Total Cost'] > 0)]
                aggregated_cost = (offtake_with_weight_and_cost['Grand Total Cost'].sum() / offtake_with_weight_and_cost['Total Weight (kg)'].sum()) if offtake_with_weight_and_cost['Total Weight (kg)'].sum() > 0 else 0
        elif most_common_benchmark in ['supply_cost_per_kg', 'kaduna_to_abuja_supply_per_kg', 'abuja_internal_supply_per_kg']:
            # Use aggregated supply cost - AUTOMATIC LOGIC for supply categories
            # First check if this is a supply category (contains "Supply" or "Internal")
            category = cat_data['Movement Category'].iloc[0] if len(cat_data) > 0 else ''
            if ('Supply' in category or 'Internal' in category):
                # This entire category represents supply operations - use same logic as monthly breakdown
                supply_with_weight = cat_data[(cat_data['Total Weight (kg)'] > 0) & (cat_data['Grand Total Cost'] > 0)]
                if len(supply_with_weight) > 0:
                    total_cost = 0
                    total_weight = 0
                    for _, row in supply_with_weight.iterrows():
                        weight = row['Total Weight (kg)']
                        cost = row['Grand Total Cost'] if row['Grand Total Cost'] > 0 else row.get('Logistics Cost', 0)
                        total_cost += cost
                        total_weight += weight
                    aggregated_cost = total_cost / total_weight if total_weight > 0 else 0
                else:
                    aggregated_cost = 0
            else:
                # Traditional approach for mixed categories
                supply_data = cat_data[cat_data['Logistics_Type_Normalized'] == 'SUPPLY']
                if len(supply_data) > 0:
                    supply_with_weight_and_cost = supply_data[(supply_data['Total Weight (kg)'] > 0) & (supply_data['Grand Total Cost'] > 0)]
                    aggregated_cost = (supply_with_weight_and_cost['Grand Total Cost'].sum() / supply_with_weight_and_cost['Total Weight (kg)'].sum()) if supply_with_weight_and_cost['Total Weight (kg)'].sum() > 0 else 0
        else:
            # Fallback to overall category average
            cat_with_weight_and_cost = cat_data[(cat_data['Total Weight (kg)'] > 0) & (cat_data['Grand Total Cost'] > 0)]
            aggregated_cost = (cat_with_weight_and_cost['Grand Total Cost'].sum() / cat_with_weight_and_cost['Total Weight (kg)'].sum()) if cat_with_weight_and_cost['Total Weight (kg)'].sum() > 0 else 0

        # Calculate status based on aggregated cost vs benchmark
        if aggregated_cost > 0 and benchmark_value > 0:
            variance_percent = ((aggregated_cost - benchmark_value) / benchmark_value) * 100

            if variance_percent <= 5:
                status = 'Within'
            elif variance_percent <= 20:
                status = 'Near'
            else:
                status = 'Exceeded'

        return {
            'display': f'₦{benchmark_value:.0f}/kg',
            'status': status
        }

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
                        total_crates = cat_data['Number of Crates'].sum()
                        total_weight = cat_data['Total Weight (kg)'].sum()
                        total_logistics_cost = cat_data['Logistics Cost'].sum()
                        total_grand_cost = cat_data['Grand Total Cost'].sum()

                        # Calculate averages using AGGREGATE METHOD (industry standard) with filtering
                        # FIX: Use allocated costs instead of raw Grand Total Cost for accurate per-unit calculations (follows metrics explainer)
                        cat_bird_data = cat_data[cat_data['Whole_Chicken_Cost_per_Bird'] > 0]
                        cat_total_allocated_bird_costs = (cat_bird_data['Whole_Chicken_Cost_per_Bird'] * cat_bird_data['Number of Birds']).sum() if len(cat_bird_data) > 0 else 0
                        cat_total_bird_count = cat_bird_data['Number of Birds'].sum() if len(cat_bird_data) > 0 else 0
                        avg_cost_per_bird = cat_total_allocated_bird_costs / cat_total_bird_count if cat_total_bird_count > 0 else 0

                        cat_crate_data = cat_data[cat_data['Egg_Cost_per_Crate'] > 0]
                        cat_total_allocated_crate_costs = (cat_crate_data['Egg_Cost_per_Crate'] * cat_crate_data['Number of Crates']).sum() if len(cat_crate_data) > 0 else 0
                        cat_total_crate_count = cat_crate_data['Number of Crates'].sum() if len(cat_crate_data) > 0 else 0
                        avg_cost_per_crate = cat_total_allocated_crate_costs / cat_total_crate_count if cat_total_crate_count > 0 else 0

                        cat_weight_data = cat_data[(cat_data['Total Weight (kg)'] > 0) & (cat_data['Grand Total Cost'] > 0)]
                        cat_total_weight_costs = cat_weight_data['Grand Total Cost'].sum() if len(cat_weight_data) > 0 else 0
                        cat_total_weight_count = cat_weight_data['Total Weight (kg)'].sum() if len(cat_weight_data) > 0 else 0
                        avg_cost_per_kg = cat_total_weight_costs / cat_total_weight_count if cat_total_weight_count > 0 else 0

                        # Product-specific averages using AGGREGATE METHOD (GAAP compliant)
                        # Calculate total cost and weight for each product type, then divide
                        gizzard_data = cat_data[cat_data['Gizzard_Cost_per_kg'] > 0]
                        avg_gizzard_cost_per_kg = (gizzard_data['Gizzard_Cost_per_kg'] * gizzard_data['Gizzard Weight']).sum() / gizzard_data['Gizzard Weight'].sum() if gizzard_data['Gizzard Weight'].sum() > 0 else 0

                        chicken_data = cat_data[cat_data['Whole_Chicken_Cost_per_kg'] > 0]
                        avg_chicken_cost_per_kg = (chicken_data['Whole_Chicken_Cost_per_kg'] * chicken_data['Whole Chicken Weight']).sum() / chicken_data['Whole Chicken Weight'].sum() if chicken_data['Whole Chicken Weight'].sum() > 0 else 0
                        avg_chicken_cost_per_bird = cat_data[cat_data['Whole_Chicken_Cost_per_Bird'] > 0]['Whole_Chicken_Cost_per_Bird'].mean() if len(cat_data[cat_data['Whole_Chicken_Cost_per_Bird'] > 0]) > 0 else 0

                        # Additional product-specific averages using AGGREGATE METHOD
                        laps_data = cat_data[cat_data['Laps_Cost_per_kg'] > 0]
                        avg_laps_cost_per_kg = (laps_data['Laps_Cost_per_kg'] * laps_data['Laps Weight']).sum() / laps_data['Laps Weight'].sum() if laps_data['Laps Weight'].sum() > 0 else 0

                        breast_data = cat_data[cat_data['Breast_Cost_per_kg'] > 0]
                        avg_breast_cost_per_kg = (breast_data['Breast_Cost_per_kg'] * breast_data['Breast Weight']).sum() / breast_data['Breast Weight'].sum() if breast_data['Breast Weight'].sum() > 0 else 0

                        fillet_data = cat_data[cat_data['Fillet_Cost_per_kg'] > 0]
                        avg_fillet_cost_per_kg = (fillet_data['Fillet_Cost_per_kg'] * fillet_data['Fillet Weight']).sum() / fillet_data['Fillet Weight'].sum() if fillet_data['Fillet Weight'].sum() > 0 else 0

                        wings_data = cat_data[cat_data['Wings_Cost_per_kg'] > 0]
                        avg_wings_cost_per_kg = (wings_data['Wings_Cost_per_kg'] * wings_data['Wings Weight']).sum() / wings_data['Wings Weight'].sum() if wings_data['Wings Weight'].sum() > 0 else 0

                        bone_data = cat_data[cat_data['Bone_Cost_per_kg'] > 0]
                        avg_bone_cost_per_kg = (bone_data['Bone_Cost_per_kg'] * bone_data['Bone Weight']).sum() / bone_data['Bone Weight'].sum() if bone_data['Bone Weight'].sum() > 0 else 0

                        # Operational metrics
                        avg_fuel_cost = cat_data[cat_data['Fuel Cost'] > 0]['Fuel Cost'].mean() if len(cat_data[cat_data['Fuel Cost'] > 0]) > 0 else 0
                        third_party_percentage = (len(cat_data[cat_data['Transportation Mode'] == 'Third Party']) / len(cat_data) * 100) if len(cat_data) > 0 else 0

                        # Purchase vs Supply cost comparison using AGGREGATE METHOD (GAAP compliant)
                        # FIX: Use allocated costs for true purchase/supply cost per kg (not raw transportation costs) - CATEGORY LEVEL

                        # For OFFTAKE categories, calculate purchase cost
                        offtake_data = cat_data[cat_data['Logistics_Type_Normalized'] == 'OFFTAKE']
                        offtake_with_weight = offtake_data[(offtake_data['Total Weight (kg)'] > 0) & (offtake_data['Grand Total Cost'] > 0)]
                        avg_purchase_cost_per_kg = self._calculate_weighted_avg_cost_per_kg(offtake_with_weight)

                        # For SUPPLY categories, the logic is different:
                        # If the category name contains "Supply" or "Internal", it IS a supply category
                        # so use the category's own data to calculate supply cost
                        if ('Supply' in category or 'Internal' in category):
                            # This entire category represents supply operations
                            # AUTOMATIC ROBUST LOGIC: Works for ANY supply location
                            supply_with_weight = cat_data[(cat_data['Total Weight (kg)'] > 0) & (cat_data['Grand Total Cost'] > 0)]
                            if len(supply_with_weight) > 0:
                                total_cost = 0
                                total_weight = 0

                                # Only use ACTUAL costs - no estimation
                                for _, row in supply_with_weight.iterrows():
                                    weight = row['Total Weight (kg)']
                                    # Try Grand Total Cost first, then Logistics Cost as fallback
                                    cost = row['Grand Total Cost'] if row['Grand Total Cost'] > 0 else row.get('Logistics Cost', 0)

                                    # NO ESTIMATION - only use actual costs
                                    total_cost += cost
                                    total_weight += weight

                                avg_supply_cost_per_kg = total_cost / total_weight if total_weight > 0 else 0
                            else:
                                avg_supply_cost_per_kg = 0
                        else:
                            # For mixed categories, look for supply records within the category
                            supply_data = cat_data[cat_data['Logistics_Type_Normalized'] == 'SUPPLY']
                            supply_with_weight = supply_data[(supply_data['Total Weight (kg)'] > 0) & (supply_data['Grand Total Cost'] > 0)]
                            avg_supply_cost_per_kg = self._calculate_weighted_avg_cost_per_kg(supply_with_weight)

                            # If no supply data found for this month/category, check if this category typically has supplies
                            if avg_supply_cost_per_kg == 0:
                                category_all_months = df[df['Movement Category'] == category]
                                category_supplies = category_all_months[
                                    (category_all_months['Logistics_Type_Normalized'] == 'SUPPLY') &
                                    (category_all_months['Total Weight (kg)'] > 0) &
                                    (category_all_months['Grand Total Cost'] > 0)
                                ]
                                if len(category_supplies) > 0:
                                    # Use most recent supply cost for this category
                                    recent_supplies = category_supplies.sort_values('Month_Year').tail(5)  # Last 5 transactions
                                    avg_supply_cost_per_kg = self._calculate_weighted_avg_cost_per_kg(recent_supplies)

                        # Benchmark analysis for this category
                        benchmark_info = self._get_category_benchmark_info(cat_data)
                        benchmark_display = benchmark_info.get('display', '')
                        benchmark_status = benchmark_info.get('status', '')

                        monthly_data.append({
                            'Month': month,
                            'Category': category,
                            'Trips': total_trips,
                            'Total Birds': total_birds,
                            'Total Crates': total_crates,
                            'Total Weight (kg)': total_weight,
                            'Total Logistics Cost': total_logistics_cost,
                            'Total Grand Cost': total_grand_cost,
                            'Avg Cost per Bird': avg_cost_per_bird if pd.notna(avg_cost_per_bird) else 0,
                            'Avg Cost per Crate': avg_cost_per_crate if pd.notna(avg_cost_per_crate) else 0,

                            # Product-specific cost per kg metrics
                            'Avg Gizzard Cost per kg': avg_gizzard_cost_per_kg if pd.notna(avg_gizzard_cost_per_kg) else 0,
                            'Gizzard_Percent_Change': 0,  # Will be calculated later
                            'Avg Chicken Cost per kg': avg_chicken_cost_per_kg if pd.notna(avg_chicken_cost_per_kg) else 0,
                            'Chicken_Percent_Change': 0,  # Will be calculated later
                            'Avg Laps Cost per kg': avg_laps_cost_per_kg if pd.notna(avg_laps_cost_per_kg) else 0,
                            'Laps_Percent_Change': 0,  # Will be calculated later
                            'Avg Breast Cost per kg': avg_breast_cost_per_kg if pd.notna(avg_breast_cost_per_kg) else 0,
                            'Breast_Percent_Change': 0,  # Will be calculated later
                            'Avg Fillet Cost per kg': avg_fillet_cost_per_kg if pd.notna(avg_fillet_cost_per_kg) else 0,
                            'Fillet_Percent_Change': 0,  # Will be calculated later
                            'Avg Wings Cost per kg': avg_wings_cost_per_kg if pd.notna(avg_wings_cost_per_kg) else 0,
                            'Wings_Percent_Change': 0,  # Will be calculated later
                            'Avg Bone Cost per kg': avg_bone_cost_per_kg if pd.notna(avg_bone_cost_per_kg) else 0,
                            'Bone_Percent_Change': 0,  # Will be calculated later

                            # Operational metrics
                            'Avg Fuel Cost': avg_fuel_cost if pd.notna(avg_fuel_cost) else 0,
                            'Third Party %': third_party_percentage if pd.notna(third_party_percentage) else 0,

                            # Purchase vs Supply comparison
                            'Avg Purchase Cost per kg': avg_purchase_cost_per_kg if pd.notna(avg_purchase_cost_per_kg) else 0,
                            'Avg Supply Cost per kg': avg_supply_cost_per_kg if pd.notna(avg_supply_cost_per_kg) else 0,

                            'Benchmark_Display': benchmark_display,
                            'Benchmark_Status': benchmark_status,
                            'Is_Month_Total': False
                        })
                
                # Add MONTH TOTAL row after each month's categories
                if not month_data.empty:
                    month_total_trips = len(month_data)
                    month_total_birds = month_data['Number of Birds'].sum()
                    month_total_crates = month_data['Number of Crates'].sum()
                    month_total_weight = month_data['Total Weight (kg)'].sum()
                    month_total_logistics_cost = month_data['Logistics Cost'].sum()
                    month_total_grand_cost = month_data['Grand Total Cost'].sum()

                    # Calculate MONTHLY AGGREGATE AVERAGES across all categories

                    # FIX: Use allocated costs instead of raw Grand Total Cost for accurate per-unit calculations (follows metrics explainer)
                    month_bird_data = month_data[month_data['Whole_Chicken_Cost_per_Bird'] > 0]
                    month_total_allocated_bird_costs = (month_bird_data['Whole_Chicken_Cost_per_Bird'] * month_bird_data['Number of Birds']).sum() if len(month_bird_data) > 0 else 0
                    month_total_bird_count = month_bird_data['Number of Birds'].sum() if len(month_bird_data) > 0 else 0
                    month_avg_cost_per_bird = month_total_allocated_bird_costs / month_total_bird_count if month_total_bird_count > 0 else 0

                    # FIX: Use allocated costs instead of raw Grand Total Cost for accurate per-unit calculations (follows metrics explainer)
                    month_crate_data = month_data[month_data['Egg_Cost_per_Crate'] > 0]
                    month_total_allocated_crate_costs = (month_crate_data['Egg_Cost_per_Crate'] * month_crate_data['Number of Crates']).sum() if len(month_crate_data) > 0 else 0
                    month_total_crate_count = month_crate_data['Number of Crates'].sum() if len(month_crate_data) > 0 else 0
                    month_avg_cost_per_crate = month_total_allocated_crate_costs / month_total_crate_count if month_total_crate_count > 0 else 0

                    # FIX: Only include costs from trips that actually transported weight for Cost/kg calculation
                    month_weight_data = month_data[(month_data['Total Weight (kg)'] > 0) & (month_data['Grand Total Cost'] > 0)]
                    month_total_weight_costs = month_weight_data['Grand Total Cost'].sum() if len(month_weight_data) > 0 else 0
                    month_total_weight_count = month_weight_data['Total Weight (kg)'].sum() if len(month_weight_data) > 0 else 0
                    month_avg_cost_per_kg = month_total_weight_costs / month_total_weight_count if month_total_weight_count > 0 else 0

                    # Monthly product-specific averages using AGGREGATE METHOD (GAAP compliant)
                    month_gizzard_data = month_data[month_data['Gizzard_Cost_per_kg'] > 0]
                    month_avg_gizzard_cost_per_kg = (month_gizzard_data['Gizzard_Cost_per_kg'] * month_gizzard_data['Gizzard Weight']).sum() / month_gizzard_data['Gizzard Weight'].sum() if month_gizzard_data['Gizzard Weight'].sum() > 0 else 0

                    month_chicken_data = month_data[month_data['Whole_Chicken_Cost_per_kg'] > 0]
                    month_avg_chicken_cost_per_kg = (month_chicken_data['Whole_Chicken_Cost_per_kg'] * month_chicken_data['Whole Chicken Weight']).sum() / month_chicken_data['Whole Chicken Weight'].sum() if month_chicken_data['Whole Chicken Weight'].sum() > 0 else 0
                    month_avg_chicken_cost_per_bird = month_data[month_data['Whole_Chicken_Cost_per_Bird'] > 0]['Whole_Chicken_Cost_per_Bird'].mean() if len(month_data[month_data['Whole_Chicken_Cost_per_Bird'] > 0]) > 0 else 0

                    # Additional monthly product-specific averages using AGGREGATE METHOD
                    month_laps_data = month_data[month_data['Laps_Cost_per_kg'] > 0]
                    month_avg_laps_cost_per_kg = (month_laps_data['Laps_Cost_per_kg'] * month_laps_data['Laps Weight']).sum() / month_laps_data['Laps Weight'].sum() if month_laps_data['Laps Weight'].sum() > 0 else 0

                    month_breast_data = month_data[month_data['Breast_Cost_per_kg'] > 0]
                    month_avg_breast_cost_per_kg = (month_breast_data['Breast_Cost_per_kg'] * month_breast_data['Breast Weight']).sum() / month_breast_data['Breast Weight'].sum() if month_breast_data['Breast Weight'].sum() > 0 else 0

                    month_fillet_data = month_data[month_data['Fillet_Cost_per_kg'] > 0]
                    month_avg_fillet_cost_per_kg = (month_fillet_data['Fillet_Cost_per_kg'] * month_fillet_data['Fillet Weight']).sum() / month_fillet_data['Fillet Weight'].sum() if month_fillet_data['Fillet Weight'].sum() > 0 else 0

                    month_wings_data = month_data[month_data['Wings_Cost_per_kg'] > 0]
                    month_avg_wings_cost_per_kg = (month_wings_data['Wings_Cost_per_kg'] * month_wings_data['Wings Weight']).sum() / month_wings_data['Wings Weight'].sum() if month_wings_data['Wings Weight'].sum() > 0 else 0

                    month_bone_data = month_data[month_data['Bone_Cost_per_kg'] > 0]
                    month_avg_bone_cost_per_kg = (month_bone_data['Bone_Cost_per_kg'] * month_bone_data['Bone Weight']).sum() / month_bone_data['Bone Weight'].sum() if month_bone_data['Bone Weight'].sum() > 0 else 0

                    # Monthly operational metrics
                    month_avg_fuel_cost = month_data[month_data['Fuel Cost'] > 0]['Fuel Cost'].mean() if len(month_data[month_data['Fuel Cost'] > 0]) > 0 else 0
                    month_third_party_percentage = (len(month_data[month_data['Transportation Mode'] == 'Third Party']) / len(month_data) * 100) if len(month_data) > 0 else 0

                    # Monthly Purchase vs Supply comparison using AGGREGATE METHOD (GAAP compliant)
                    # FIX: Use allocated costs for true purchase/supply cost per kg (not raw transportation costs)

                    # Calculate overall monthly purchase cost from offtake data
                    month_offtake_data = month_data[month_data['Logistics_Type_Normalized'] == 'OFFTAKE']
                    month_offtake_with_weight = month_offtake_data[(month_offtake_data['Total Weight (kg)'] > 0) & (month_offtake_data['Grand Total Cost'] > 0)]
                    month_avg_purchase_cost_per_kg = self._calculate_weighted_avg_cost_per_kg(month_offtake_with_weight)

                    # Calculate overall monthly supply cost from all supply data (including supply categories)
                    month_supply_data = month_data[month_data['Logistics_Type_Normalized'] == 'SUPPLY']
                    month_supply_with_weight = month_supply_data[(month_supply_data['Total Weight (kg)'] > 0) & (month_supply_data['Grand Total Cost'] > 0)]
                    month_avg_supply_cost_per_kg = self._calculate_weighted_avg_cost_per_kg(month_supply_with_weight)

                    monthly_data.append({
                        'Month': month,
                        'Category': 'MONTH SUMMARY',
                        'Trips': month_total_trips,
                        'Total Birds': month_total_birds,
                        'Total Crates': month_total_crates,
                        'Total Weight (kg)': month_total_weight,
                        'Total Logistics Cost': month_total_logistics_cost,
                        'Total Grand Cost': month_total_grand_cost,
                        'Avg Cost per Bird': month_avg_cost_per_bird,
                        'Avg Cost per Crate': month_avg_cost_per_crate,

                        # Product-specific cost per kg metrics
                        'Avg Gizzard Cost per kg': month_avg_gizzard_cost_per_kg,
                        'Gizzard_Percent_Change': 0,  # N/A for totals
                        'Avg Chicken Cost per kg': month_avg_chicken_cost_per_kg,
                        'Chicken_Percent_Change': 0,  # N/A for totals
                        'Avg Laps Cost per kg': month_avg_laps_cost_per_kg,
                        'Laps_Percent_Change': 0,  # N/A for totals
                        'Avg Breast Cost per kg': month_avg_breast_cost_per_kg,
                        'Breast_Percent_Change': 0,  # N/A for totals
                        'Avg Fillet Cost per kg': month_avg_fillet_cost_per_kg,
                        'Fillet_Percent_Change': 0,  # N/A for totals
                        'Avg Wings Cost per kg': month_avg_wings_cost_per_kg,
                        'Wings_Percent_Change': 0,  # N/A for totals
                        'Avg Bone Cost per kg': month_avg_bone_cost_per_kg,
                        'Bone_Percent_Change': 0,  # N/A for totals

                        # Operational metrics
                        'Avg Fuel Cost': month_avg_fuel_cost,
                        'Third Party %': month_third_party_percentage,

                        # Purchase vs Supply comparison
                        'Avg Purchase Cost per kg': month_avg_purchase_cost_per_kg,
                        'Avg Supply Cost per kg': month_avg_supply_cost_per_kg,

                        'Benchmark_Display': '',  # N/A for totals
                        'Benchmark_Status': '',  # N/A for totals
                        'Is_Month_Total': True
                    })
            
            # Convert to DataFrame for easier manipulation
            monthly_df = pd.DataFrame(monthly_data)
            
            # Calculate month-over-month percentage changes for product-specific costs (exclude totals)
            if not monthly_df.empty:
                # Sort by category and month for proper comparison
                monthly_df = monthly_df.sort_values(['Category', 'Month'])

                # Calculate product-specific percentage changes for each category (skip month totals)
                for category in monthly_df[monthly_df['Is_Month_Total'] == False]['Category'].unique():
                    cat_mask = (monthly_df['Category'] == category) & (monthly_df['Is_Month_Total'] == False)
                    cat_data = monthly_df[cat_mask].copy().sort_values('Month')

                    if len(cat_data) > 1:
                        # Calculate month-over-month changes for each product
                        for i in range(1, len(cat_data)):
                            current_idx = cat_data.index[i]
                            previous_idx = cat_data.index[i-1]

                            # Gizzard cost/kg % change
                            prev_gizzard = cat_data.loc[previous_idx, 'Avg Gizzard Cost per kg']
                            curr_gizzard = cat_data.loc[current_idx, 'Avg Gizzard Cost per kg']
                            if prev_gizzard > 0 and curr_gizzard > 0:
                                gizzard_change = ((curr_gizzard - prev_gizzard) / prev_gizzard) * 100
                                monthly_df.loc[current_idx, 'Gizzard_Percent_Change'] = gizzard_change

                            # Chicken cost/kg % change
                            prev_chicken = cat_data.loc[previous_idx, 'Avg Chicken Cost per kg']
                            curr_chicken = cat_data.loc[current_idx, 'Avg Chicken Cost per kg']
                            if prev_chicken > 0 and curr_chicken > 0:
                                chicken_change = ((curr_chicken - prev_chicken) / prev_chicken) * 100
                                monthly_df.loc[current_idx, 'Chicken_Percent_Change'] = chicken_change

                            # Laps cost/kg % change
                            prev_laps = cat_data.loc[previous_idx, 'Avg Laps Cost per kg']
                            curr_laps = cat_data.loc[current_idx, 'Avg Laps Cost per kg']
                            if prev_laps > 0 and curr_laps > 0:
                                laps_change = ((curr_laps - prev_laps) / prev_laps) * 100
                                monthly_df.loc[current_idx, 'Laps_Percent_Change'] = laps_change

                            # Breast cost/kg % change
                            prev_breast = cat_data.loc[previous_idx, 'Avg Breast Cost per kg']
                            curr_breast = cat_data.loc[current_idx, 'Avg Breast Cost per kg']
                            if prev_breast > 0 and curr_breast > 0:
                                breast_change = ((curr_breast - prev_breast) / prev_breast) * 100
                                monthly_df.loc[current_idx, 'Breast_Percent_Change'] = breast_change

                            # Fillet cost/kg % change
                            prev_fillet = cat_data.loc[previous_idx, 'Avg Fillet Cost per kg']
                            curr_fillet = cat_data.loc[current_idx, 'Avg Fillet Cost per kg']
                            if prev_fillet > 0 and curr_fillet > 0:
                                fillet_change = ((curr_fillet - prev_fillet) / prev_fillet) * 100
                                monthly_df.loc[current_idx, 'Fillet_Percent_Change'] = fillet_change

                            # Wings cost/kg % change
                            prev_wings = cat_data.loc[previous_idx, 'Avg Wings Cost per kg']
                            curr_wings = cat_data.loc[current_idx, 'Avg Wings Cost per kg']
                            if prev_wings > 0 and curr_wings > 0:
                                wings_change = ((curr_wings - prev_wings) / prev_wings) * 100
                                monthly_df.loc[current_idx, 'Wings_Percent_Change'] = wings_change

                            # Bone cost/kg % change
                            prev_bone = cat_data.loc[previous_idx, 'Avg Bone Cost per kg']
                            curr_bone = cat_data.loc[current_idx, 'Avg Bone Cost per kg']
                            if prev_bone > 0 and curr_bone > 0:
                                bone_change = ((curr_bone - prev_bone) / prev_bone) * 100
                                monthly_df.loc[current_idx, 'Bone_Percent_Change'] = bone_change
                
                # Sort by month first, then put MONTH SUMMARY at the end of each month
                monthly_df['Sort_Order'] = monthly_df.apply(lambda x: (x['Month'], 1 if x['Is_Month_Total'] else 0, x['Category']), axis=1)
                monthly_df = monthly_df.sort_values('Sort_Order')
                monthly_df = monthly_df.drop('Sort_Order', axis=1)
            
            return monthly_df
            
        except Exception as e:
            print(f"Error calculating monthly breakdown: {e}")
            return pd.DataFrame()
    
    
    def format_cell_range(self, range_name: str, bg_color: dict, text_color: dict = None,
                         bold: bool = False, italic: bool = False, font_size: int = 10, format_type: str = None, sheet=None):
        """Apply formatting to a cell range - now collects requests for batch processing."""
        if text_color is None:
            text_color = {'red': 0, 'green': 0, 'blue': 0}  # Black
        
        format_dict = {
            "backgroundColor": bg_color,
            "textFormat": {
                "foregroundColor": text_color,
                "fontSize": font_size,
                "bold": bold,
                "italic": italic
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
        """Process all queued formatting requests efficiently using batchUpdate."""
        if not hasattr(self, '_format_queue') or not self._format_queue:
            return

        try:
            # Group by sheet
            sheet_requests = {}
            for req in self._format_queue:
                target_sheet = req['sheet']
                sheet_id = target_sheet.id
                if sheet_id not in sheet_requests:
                    sheet_requests[sheet_id] = []
                sheet_requests[sheet_id].append(req)

            # Process each sheet's requests
            for sheet_id, requests in sheet_requests.items():
                if sheet and requests[0]['sheet'] != sheet:
                    continue

                # Build batchUpdate requests - process in optimal batches (50-100 operations per batch)
                batch_size = 50
                for i in range(0, len(requests), batch_size):
                    batch = requests[i:i + batch_size]

                    # Convert formatting requests to batchUpdate format
                    batch_requests = []
                    for req in batch:
                        range_parts = req['range'].split('!')
                        if len(range_parts) == 2:
                            range_notation = range_parts[1]
                        else:
                            range_notation = req['range']

                        # Parse range (e.g., "A1:B2" or "A1")
                        if ':' in range_notation:
                            start_cell, end_cell = range_notation.split(':')
                        else:
                            start_cell = end_cell = range_notation

                        # Convert A1 notation to grid coordinates
                        start_col, start_row = self._a1_to_grid_range(start_cell)
                        end_col, end_row = self._a1_to_grid_range(end_cell)

                        batch_requests.append({
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": start_row,
                                    "endRowIndex": end_row + 1,
                                    "startColumnIndex": start_col,
                                    "endColumnIndex": end_col + 1
                                },
                                "cell": {
                                    "userEnteredFormat": req['format']
                                },
                                "fields": "userEnteredFormat"
                            }
                        })

                    # Execute batch update
                    if batch_requests:
                        self.execute_with_retry(
                            self.spreadsheet.batch_update,
                            {"requests": batch_requests}
                        )

            # Clear the queue
            self._format_queue = []

        except Exception as e:
            print(f"⚠ Warning: Some formatting could not be applied: {e}")
            self._format_queue = []

    def _a1_to_grid_range(self, a1_notation):
        """Convert A1 notation to zero-based grid coordinates."""
        col_str = ""
        row_str = ""

        for char in a1_notation:
            if char.isalpha():
                col_str += char
            else:
                row_str += char

        # Convert column letters to number (A=0, B=1, etc.)
        col = 0
        for i, char in enumerate(reversed(col_str.upper())):
            col += (ord(char) - ord('A') + 1) * (26 ** i)
        col -= 1  # Convert to zero-based

        # Convert row to zero-based
        row = int(row_str) - 1 if row_str else 0

        return col, row
    
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

    def column_letter_to_index(self, column_letter):
        """Convert column letter(s) to zero-based index."""
        index = 0
        for char in column_letter:
            index = index * 26 + (ord(char) - ord('A') + 1)
        return index - 1

    def auto_resize_columns(self, sheet, start_col='A', end_col='K'):
        """Auto-resize columns for better display."""
        try:
            # Auto-resize columns using Google Sheets API
            requests = [{
                "autoResizeDimensions": {
                    "dimensions": {
                        "sheetId": sheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": self.column_letter_to_index(start_col),
                        "endIndex": self.column_letter_to_index(end_col) + 1
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
            # Clear formatting from a large range to handle cases where previous data extended further
            try:
                body = {
                    "requests": [
                        {
                            "updateCells": {
                                "range": {
                                    "sheetId": self.dashboard_sheet.id
                                    # No row/column indexes = entire sheet (unbounded)
                                },
                                "fields": "userEnteredFormat"
                            }
                        }
                    ]
                }
                self.execute_with_retry(self.spreadsheet.batch_update, body)
            except Exception:
                # Fallback: clear row by row if batch update fails
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
            
            # KPI Data with explanations (with proper comma formatting) - ENHANCED with product-specific metrics
            kpi_data = [
                # Traditional metrics
                ['Average Purchase Cost per Bird', f"₦{overall_metrics.get('avg_purchase_cost_per_bird', 0):,.2f}", "Total offtake costs ÷ total offtake birds (offtake only)"],
                ['Average Supply Cost per Bird', f"₦{overall_metrics.get('avg_supply_cost_per_bird', 0):,.2f}", "Total supply costs ÷ total supply birds (supply only)"],
                ['Total Birds Moved', f"{overall_metrics.get('total_birds_moved', 0):,.0f}", "Sum of all birds transported (offtake + supply)"],

                # NEW: Product-specific bird metrics
                ['Whole Chicken Cost per Bird', f"₦{overall_metrics.get('avg_whole_chicken_cost_per_bird', 0):,.2f}", "Average logistics cost per bird for whole chicken shipments (offtake + supply)"],

                # NEW: Crate-based metrics
                ['Total Crates Moved', f"{overall_metrics.get('total_crates_moved', 0):,.0f}", "Sum of all egg crates transported (offtake + supply)"],
                ['Average Egg Cost per Crate', f"₦{overall_metrics.get('avg_egg_cost_per_crate', 0):,.2f}", "Average logistics cost per crate for egg shipments (offtake + supply)"],

                # Traditional weight metrics
                ['Average Purchase Cost per kg', f"₦{overall_metrics.get('avg_purchase_cost_per_kg', 0):,.2f}", "Total offtake costs ÷ total offtake weight (offtake only)"],
                ['Average Supply Cost per kg', f"₦{overall_metrics.get('avg_supply_cost_per_kg', 0):,.2f}", "Total supply costs ÷ total supply weight (supply only)"],
                ['Total Weight Moved (kg)', f"{overall_metrics.get('total_weight_moved', 0):,.1f}", "Sum of all weight transported (offtake + supply)"],

                # NEW: Product-specific cost per kg metrics
                ['Gizzard Cost per kg', f"₦{overall_metrics.get('avg_gizzard_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for gizzard shipments (offtake + supply)"],
                ['Whole Chicken Cost per kg', f"₦{overall_metrics.get('avg_whole_chicken_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for whole chicken shipments (offtake + supply)"],
                ['Laps Cost per kg', f"₦{overall_metrics.get('avg_laps_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for laps shipments (offtake + supply)"],
                ['Breast Cost per kg', f"₦{overall_metrics.get('avg_breast_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for breast shipments (offtake + supply)"],
                ['Fillet Cost per kg', f"₦{overall_metrics.get('avg_fillet_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for fillet shipments (offtake + supply)"],
                ['Wings Cost per kg', f"₦{overall_metrics.get('avg_wings_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for wings shipments (offtake + supply)"],
                ['Bone Cost per kg', f"₦{overall_metrics.get('avg_bone_cost_per_kg', 0):,.2f}", "Average logistics cost per kg for bone shipments (offtake + supply)"],

                # General metrics
                ['Average Fuel Cost', f"₦{overall_metrics.get('avg_fuel_cost', 0):,.2f}", "Average fuel cost per trip across all shipments (offtake + supply)"],
                ['Third Party Transportation Usage', f"{overall_metrics.get('third_party_percentage', 0):.1f}%", "Third party trips ÷ total trips × 100 (offtake + supply)"],
                ['Current Available Balance', f"₦{overall_metrics.get('current_running_balance', 0):,.2f}", "Running total of all cash flows (fuel costs excluded as paid separately)"],
            ]
            
            kpi_start_row = current_row
            self.execute_with_retry(
                self.dashboard_sheet.update,
                kpi_data,
                f'A{current_row}:C{current_row + len(kpi_data) - 1}'
            )
            
            # Apply italic formatting to all descriptions in column C
            self.format_cell_range(f'C{kpi_start_row}:C{kpi_start_row + len(kpi_data) - 1}',
                                 {'red': 1, 'green': 1, 'blue': 1},  # White background
                                 {'red': 0.3, 'green': 0.3, 'blue': 0.3},  # Dark gray text
                                 italic=True, font_size=10)

            # Add alternating row colors for better readability
            for i, row_data in enumerate(kpi_data):
                row = kpi_start_row + i

                # Apply alternating background colors to all columns
                if i % 2 == 0:
                    # Even rows - very light gray background
                    self.format_cell_range(f'A{row}:C{row}', {'red': 0.97, 'green': 0.97, 'blue': 0.97})
                else:
                    # Odd rows - white background
                    self.format_cell_range(f'A{row}:C{row}', {'red': 1.0, 'green': 1.0, 'blue': 1.0})

                # Re-apply italic formatting to description column (Column C) to preserve it
                self.format_cell_range(f'C{row}',
                                     {'red': 1, 'green': 1, 'blue': 1} if i % 2 != 0 else {'red': 0.97, 'green': 0.97, 'blue': 0.97},
                                     {'red': 0.3, 'green': 0.3, 'blue': 0.3},
                                     italic=True, font_size=10)

                # Special coloring for current available balance value
                if row_data[0] == 'Current Available Balance':
                    running_balance_value = overall_metrics.get('current_running_balance', 0)
                    if running_balance_value > 0:
                        self.format_cell_range(f'B{row}', self.colors['positive'], bold=True)
                    elif running_balance_value < 0:
                        self.format_cell_range(f'B{row}', self.colors['negative'], bold=True)
            
            current_row += len(kpi_data) + 2

            # BENCHMARK PERFORMANCE SECTION
            if overall_metrics.get('overall_compliance_rate') is not None:
                benchmark_section_data = [
                    ['BENCHMARK PERFORMANCE SUMMARY', '', ''],
                    ['Benchmark Type', 'Compliance Rate', 'Avg Budget Overrun']
                ]

                self.execute_with_retry(
                    self.dashboard_sheet.update,
                    benchmark_section_data,
                    f'A{current_row}:C{current_row + 1}'
                )

                # Format benchmark section headers
                self.format_cell_range(f'A{current_row}:C{current_row}',
                                     self.colors['subheader'], bold=True, font_size=12)
                self.format_cell_range(f'A{current_row + 1}:C{current_row + 1}',
                                     self.colors['primary'], {'red': 1, 'green': 1, 'blue': 1}, bold=True)
                current_row += 2

                # Benchmark performance data
                benchmark_data = [
                    ['Overall Performance', f"{overall_metrics.get('overall_compliance_rate', 0):.1f}%", f"+{overall_metrics.get('overall_avg_overage', 0):.1f}%"],
                    ['Purchase Operations (₦40/kg)', f"{overall_metrics.get('offtake_cost_per_kg_compliance_rate', 0):.1f}%", f"+{overall_metrics.get('offtake_cost_per_kg_avg_overage', 0):.1f}%"],
                    ['Delivery Operations (₦80/kg)', f"{overall_metrics.get('supply_cost_per_kg_compliance_rate', 0):.1f}%", f"+{overall_metrics.get('supply_cost_per_kg_avg_overage', 0):.1f}%"],
                    ['Kaduna→Abuja Route (₦50/kg)', f"{overall_metrics.get('kaduna_to_abuja_supply_per_kg_compliance_rate', 0):.1f}%", f"+{overall_metrics.get('kaduna_to_abuja_supply_per_kg_avg_overage', 0):.1f}%"],
                    ['Abuja Internal Routes (₦30/kg)', f"{overall_metrics.get('abuja_internal_supply_per_kg_compliance_rate', 0):.1f}%", f"+{overall_metrics.get('abuja_internal_supply_per_kg_avg_overage', 0):.1f}%"]
                ]

                benchmark_start_row = current_row
                self.execute_with_retry(
                    self.dashboard_sheet.update,
                    benchmark_data,
                    f'A{current_row}:C{current_row + len(benchmark_data) - 1}'
                )

                # Format benchmark performance data with colors and alternating rows
                for i, row_data in enumerate(benchmark_data):
                    row = benchmark_start_row + i

                    # Apply alternating background colors to all columns first
                    if i % 2 == 0:
                        # Even rows - very light gray background
                        self.format_cell_range(f'A{row}:C{row}', {'red': 0.97, 'green': 0.97, 'blue': 0.97})
                    else:
                        # Odd rows - white background
                        self.format_cell_range(f'A{row}:C{row}', {'red': 1.0, 'green': 1.0, 'blue': 1.0})

                    # Color compliance rates: green >80%, yellow 60-80%, red <60%
                    if i == 0:  # Overall performance
                        compliance_rate = overall_metrics.get('overall_compliance_rate', 0)
                    else:
                        key_map = ['', 'offtake_cost_per_kg_compliance_rate', 'supply_cost_per_kg_compliance_rate',
                                  'kaduna_to_abuja_supply_per_kg_compliance_rate', 'abuja_internal_supply_per_kg_compliance_rate']
                        compliance_rate = overall_metrics.get(key_map[i], 0)

                    if compliance_rate >= 80:
                        self.format_cell_range(f'B{row}', self.colors['positive'], bold=True)
                    elif compliance_rate >= 60:
                        self.format_cell_range(f'B{row}', {'red': 1.0, 'green': 0.97, 'blue': 0.88}, bold=True)
                    else:
                        self.format_cell_range(f'B{row}', self.colors['negative'], bold=True)

                current_row += len(benchmark_data) + 2

            # BENCHMARK VIOLATIONS ALERT SECTION
            violations = self.get_current_violations(df)
            if violations:
                violation_section_data = [
                    ['⚠️ BENCHMARK VIOLATIONS (Current Month)', '', '', ''],
                    ['Date', 'Route', 'Actual ₦/kg', 'vs Benchmark']
                ]

                self.execute_with_retry(
                    self.dashboard_sheet.update,
                    violation_section_data,
                    f'A{current_row}:D{current_row + 1}'
                )

                # Format violation section headers
                self.format_cell_range(f'A{current_row}:D{current_row}',
                                     self.colors['negative'], bold=True, font_size=12)
                self.format_cell_range(f'A{current_row + 1}:D{current_row + 1}',
                                     self.colors['primary'], {'red': 1, 'green': 1, 'blue': 1}, bold=True)
                current_row += 2

                # Violation data (limit to top 10 most severe)
                violation_data = []
                for violation in violations[:10]:  # Show top 10 violations
                    violation_data.append([
                        violation['date'],
                        violation['route'],
                        f"₦{violation['actual_cost']:.2f}",
                        f"{violation['overage_percent']:+.1f}% over ₦{violation['benchmark']:.0f}"
                    ])

                if violation_data:
                    self.execute_with_retry(
                        self.dashboard_sheet.update,
                        violation_data,
                        f'A{current_row}:D{current_row + len(violation_data) - 1}'
                    )

                    # Format violation data with red background for severe violations
                    for i, row_data in enumerate(violation_data):
                        row = current_row + i
                        self.format_cell_range(f'A{row}:D{row}',
                                             {'red': 0.98, 'green': 0.95, 'blue': 0.95})  # Light red

                current_row += len(violation_data) + 2 if violation_data else 2

            # Monthly Breakdown Section - combine headers in one update
            monthly_breakdown = self.calculate_monthly_breakdown(df)
            if not monthly_breakdown.empty:
                monthly_section_data = [
                    ['MONTHLY BREAKDOWN BY MOVEMENT CATEGORY & PRODUCTS', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', ''],
                    ['Month', 'Category', 'Trips', 'Birds', 'Crates', 'Total Weight', 'Total Cost', 'Avg Fuel Cost',
                     'Cost/Bird', 'Cost/Crate', 'Gizzard ₦/kg', 'Gizzard Cost % MoM', 'Chicken ₦/kg', 'Chicken Cost % MoM',
                     'Laps ₦/kg', 'Laps Cost % MoM', 'Breast ₦/kg', 'Breast Cost % MoM', 'Fillet ₦/kg', 'Fillet Cost % MoM',
                     'Wings ₦/kg', 'Wings Cost % MoM', 'Bone ₦/kg', 'Bone Cost % MoM', '3rd Party %',
                     'Purchase ₦/kg', 'Supply ₦/kg', 'Benchmark', 'Status']
                ]
                
                self.execute_with_retry(
                    self.dashboard_sheet.update,
                    monthly_section_data,
                    f'A{current_row}:AC{current_row + 1}'
                )

                # Format only section header and column headers
                self.format_cell_range(f'A{current_row}:AC{current_row}',
                                     self.colors['subheader'], bold=True, font_size=12)
                self.format_cell_range(f'A{current_row + 1}:AC{current_row + 1}',
                                     self.colors['primary'], {'red': 1, 'green': 1, 'blue': 1}, bold=True)
                current_row += 2
                
                # Monthly breakdown data with color separation between months only
                monthly_start_row = current_row
                monthly_data = []
                
                for _, row in monthly_breakdown.iterrows():
                    # Format product-specific percentage changes
                    gizzard_pct = f"{row['Gizzard_Percent_Change']:+.1f}%" if row['Gizzard_Percent_Change'] != 0 else "-"
                    chicken_pct = f"{row['Chicken_Percent_Change']:+.1f}%" if row['Chicken_Percent_Change'] != 0 else "-"
                    laps_pct = f"{row['Laps_Percent_Change']:+.1f}%" if row['Laps_Percent_Change'] != 0 else "-"
                    breast_pct = f"{row['Breast_Percent_Change']:+.1f}%" if row['Breast_Percent_Change'] != 0 else "-"
                    fillet_pct = f"{row['Fillet_Percent_Change']:+.1f}%" if row['Fillet_Percent_Change'] != 0 else "-"
                    wings_pct = f"{row['Wings_Percent_Change']:+.1f}%" if row['Wings_Percent_Change'] != 0 else "-"
                    bone_pct = f"{row['Bone_Percent_Change']:+.1f}%" if row['Bone_Percent_Change'] != 0 else "-"

                    monthly_data.append([
                        row['Month'],
                        row['Category'],
                        int(row['Trips']),
                        f"{row['Total Birds']:,.0f}",
                        f"{row['Total Crates']:,.0f}",
                        f"{row['Total Weight (kg)']:,.1f}",
                        f"₦{row['Total Grand Cost']:,.2f}",
                        f"₦{row['Avg Fuel Cost']:,.2f}",
                        f"₦{row['Avg Cost per Bird']:,.2f}",
                        f"₦{row['Avg Cost per Crate']:,.2f}",
                        f"₦{row['Avg Gizzard Cost per kg']:,.2f}",
                        gizzard_pct,
                        f"₦{row['Avg Chicken Cost per kg']:,.2f}",
                        chicken_pct,
                        f"₦{row['Avg Laps Cost per kg']:,.2f}",
                        laps_pct,
                        f"₦{row['Avg Breast Cost per kg']:,.2f}",
                        breast_pct,
                        f"₦{row['Avg Fillet Cost per kg']:,.2f}",
                        fillet_pct,
                        f"₦{row['Avg Wings Cost per kg']:,.2f}",
                        wings_pct,
                        f"₦{row['Avg Bone Cost per kg']:,.2f}",
                        bone_pct,
                        f"{row['Third Party %']:,.1f}%",
                        f"₦{row['Avg Purchase Cost per kg']:,.2f}" if row['Avg Purchase Cost per kg'] > 0 else "N/A",
                        f"₦{row['Avg Supply Cost per kg']:,.2f}" if row['Avg Supply Cost per kg'] > 0 else "N/A",
                        row['Benchmark_Display'],
                        row['Benchmark_Status']
                    ])
                
                if monthly_data:
                    self.execute_with_retry(
                        self.dashboard_sheet.update,
                        monthly_data,
                        f'A{current_row}:AC{current_row + len(monthly_data) - 1}'
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
                        
                        # Special formatting for MONTH SUMMARY rows
                        if row_data_dict.get('Is_Month_Total', False) or 'MONTH SUMMARY' in str(row_data[1]):
                            # Bold formatting with darker background for monthly totals
                            self.format_cell_range(f'A{row_num}:AC{row_num}',
                                                 {'red': 0.85, 'green': 0.92, 'blue': 0.95},  # Darker blue for totals
                                                 bold=True,
                                                 sheet=self.dashboard_sheet)
                        else:
                            # Apply regular month-based coloring to category rows
                            self.format_cell_range(f'A{row_num}:AC{row_num}',
                                                 month_colors[month_color_index],
                                                 sheet=self.dashboard_sheet)
                        
                        # Color product-specific percentage changes and benchmark status
                        if not (row_data_dict.get('Is_Month_Total', False) or 'MONTH SUMMARY' in str(row_data[1])):
                            # Color Gizzard % change (column L)
                            if row_data_dict['Gizzard_Percent_Change'] > 0:
                                self.format_cell_range(f'L{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict['Gizzard_Percent_Change'] < 0:
                                self.format_cell_range(f'L{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Color Chicken % change (column N)
                            if row_data_dict['Chicken_Percent_Change'] > 0:
                                self.format_cell_range(f'N{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict['Chicken_Percent_Change'] < 0:
                                self.format_cell_range(f'N{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Color additional product-specific percentage changes
                            # Laps % change (column P)
                            if row_data_dict.get('Laps_Percent_Change', 0) > 0:
                                self.format_cell_range(f'P{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict.get('Laps_Percent_Change', 0) < 0:
                                self.format_cell_range(f'P{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Breast % change (column R)
                            if row_data_dict.get('Breast_Percent_Change', 0) > 0:
                                self.format_cell_range(f'R{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict.get('Breast_Percent_Change', 0) < 0:
                                self.format_cell_range(f'R{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Fillet % change (column T)
                            if row_data_dict.get('Fillet_Percent_Change', 0) > 0:
                                self.format_cell_range(f'T{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict.get('Fillet_Percent_Change', 0) < 0:
                                self.format_cell_range(f'T{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Wings % change (column V)
                            if row_data_dict.get('Wings_Percent_Change', 0) > 0:
                                self.format_cell_range(f'V{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict.get('Wings_Percent_Change', 0) < 0:
                                self.format_cell_range(f'V{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Bone % change (column X)
                            if row_data_dict.get('Bone_Percent_Change', 0) > 0:
                                self.format_cell_range(f'X{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red for cost increase
                            elif row_data_dict.get('Bone_Percent_Change', 0) < 0:
                                self.format_cell_range(f'X{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green for cost decrease

                            # Color benchmark status (column AC - last column)
                            benchmark_status = row_data_dict.get('Benchmark_Status', '')
                            if benchmark_status == 'Within':
                                self.format_cell_range(f'AC{row_num}', {'red': 0.85, 'green': 0.95, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Green
                            elif benchmark_status == 'Near':
                                self.format_cell_range(f'AC{row_num}', {'red': 1.0, 'green': 0.97, 'blue': 0.88}, sheet=self.dashboard_sheet)  # Yellow
                            elif benchmark_status == 'Exceeded':
                                self.format_cell_range(f'AC{row_num}', {'red': 0.98, 'green': 0.85, 'blue': 0.85}, sheet=self.dashboard_sheet)  # Red
            
            # Apply all formatting in batch and auto-resize columns
            print("🎨 Applying final formatting and auto-sizing...")
            self.flush_formatting_queue(self.dashboard_sheet)
            self.auto_resize_columns(self.dashboard_sheet, 'A', 'AC')
                    
        except Exception as e:
            print(f"✗ Error updating main dashboard: {e}")
    
    
    def update_cash_flow_sheet(self, cash_flow_timeline: pd.DataFrame):
        """Update the cash flow timeline sheet - optimized."""
        try:
            # Clear existing content AND formatting completely with retry
            self.execute_with_retry(self.cash_flow_sheet.clear)
            
            # Additional comprehensive clear to ensure no residual formatting remains
            # Clear formatting from a large range to handle cases where previous data extended further
            try:
                body = {
                    "requests": [
                        {
                            "updateCells": {
                                "range": {
                                    "sheetId": self.cash_flow_sheet.id
                                    # No row/column indexes = entire sheet (unbounded)
                                },
                                "fields": "userEnteredFormat"
                            }
                        }
                    ]
                }
                self.execute_with_retry(self.spreadsheet.batch_update, body)
            except Exception:
                # Fallback: clear row by row if batch update fails
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
    # Configuration - use environment variables for security (works with GitHub Actions secrets)
    # Try GOOGLE_SERVICE_ACCOUNT (JSON content) first, then fall back to GOOGLE_SERVICE_ACCOUNT_FILE (file path)
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    credentials_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
    spreadsheet_id = os.getenv('SPREADSHEET_ID')

    # For local development, you can set these environment variables:
    # export GOOGLE_SERVICE_ACCOUNT_FILE="/path/to/your/service_account.json"
    # export SPREADSHEET_ID="your_spreadsheet_id_here"
    # For CI/CD, GOOGLE_SERVICE_ACCOUNT contains the JSON content directly

    if not service_account_json and not credentials_path:
        raise ValueError("Please set either GOOGLE_SERVICE_ACCOUNT or GOOGLE_SERVICE_ACCOUNT_FILE environment variable")

    if not spreadsheet_id:
        raise ValueError("Please set SPREADSHEET_ID environment variable")

    # Handle JSON content vs file path
    if service_account_json:
        # Write JSON content to temporary file for gspread
        import tempfile
        import json

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
            json.dump(json.loads(service_account_json), temp_file, indent=2)
            temp_credentials_path = temp_file.name

        try:
            # Create updater instance with temporary file
            updater = LogisticsDashboardUpdater(temp_credentials_path, spreadsheet_id)

            # Run the update
            success = updater.run_update()
        finally:
            # Clean up temporary file
            os.unlink(temp_credentials_path)
    else:
        # Create updater instance with file path
        updater = LogisticsDashboardUpdater(credentials_path, spreadsheet_id)

        # Run the update
        success = updater.run_update()
    
    if success:
        print(f"\n✨ Results written to 2 sheets:")
        print(f"   📊 '{updater.new_sheet_name}' - KPIs & Monthly Breakdown")
        print(f"   💰 '{updater.cash_flow_sheet_name}' - Running Balance Timeline")
    else:
        print("\n❌ Update failed. Please check the error messages above.")