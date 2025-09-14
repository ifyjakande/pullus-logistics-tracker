#!/usr/bin/env python3
"""
Metrics Explainer Sheet Creator
Creates a comprehensive documentation sheet explaining all cost calculation methodologies
used in the Pullus Logistics Dashboard.

This is a standalone script that runs once to create and populate the explainer sheet.
"""

import gspread
from google.oauth2.service_account import Credentials
import time
import os
import json

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

class MetricsExplainerCreator:
    def __init__(self):
        """Initialize the metrics explainer creator."""
        self.spreadsheet = None
        self.explainer_sheet = None

        # Use environment variables for security (same as main dashboard updater)
        self.spreadsheet_id = os.getenv('SPREADSHEET_ID')
        self.credentials_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')

        if not self.spreadsheet_id:
            raise ValueError("Please set SPREADSHEET_ID environment variable")

        if not self.credentials_path:
            raise ValueError("Please set GOOGLE_SERVICE_ACCOUNT_FILE environment variable")

        # Formatting queue for batch processing
        self.formatting_queue = []

        # Colors for formatting
        self.colors = {
            'header': {'red': 0.2, 'green': 0.4, 'blue': 0.8},      # Blue
            'subheader': {'red': 0.3, 'green': 0.6, 'blue': 0.9},   # Light blue
            'formula': {'red': 0.95, 'green': 0.95, 'blue': 0.85},  # Light yellow
            'example': {'red': 0.85, 'green': 0.95, 'blue': 0.85},  # Light green
            'warning': {'red': 0.98, 'green': 0.85, 'blue': 0.85},  # Light red
            'info': {'red': 0.9, 'green': 0.9, 'blue': 0.95}       # Light gray
        }

    def connect(self):
        """Connect to Google Sheets using service account credentials."""
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]

            # Handle both file path and JSON string from environment (same as main script)
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
            elif self.credentials_path and os.path.isfile(self.credentials_path):
                # Use local file
                creds = Credentials.from_service_account_file(
                    self.credentials_path,
                    scopes=scopes
                )
            else:
                raise ValueError("No valid service account configuration found")

            # Create client and open spreadsheet by ID
            client = gspread.authorize(creds)
            self.spreadsheet = client.open_by_key(self.spreadsheet_id)

            print("✓ Successfully connected to Google Sheets")
            return True

        except Exception as e:
            print(f"❌ Error connecting to Google Sheets: {e}")
            return False

    def create_explainer_sheet(self):
        """Create the Metrics Explainer sheet."""
        try:
            # Check if sheet already exists
            try:
                existing_sheet = self.spreadsheet.worksheet("Metrics Explainer")
                print("⚠ Metrics Explainer sheet already exists. Deleting old version...")
                self.spreadsheet.del_worksheet(existing_sheet)
                time.sleep(1)
            except gspread.WorksheetNotFound:
                pass

            # Create new sheet with more rows to accommodate all content
            self.explainer_sheet = self.spreadsheet.add_worksheet(
                title="Metrics Explainer",
                rows=300,
                cols=10
            )

            print("✓ Created new 'Metrics Explainer' sheet")
            return True

        except Exception as e:
            print(f"❌ Error creating explainer sheet: {e}")
            return False

    def format_cell_range(self, range_notation, background_color, bold=False, text_color=None, font_size=10, italic=False):
        """Queue a format request for batch processing - enhanced version."""
        if text_color is None:
            text_color = {'red': 0, 'green': 0, 'blue': 0}  # Black default

        format_dict = {
            "backgroundColor": background_color,
            "textFormat": {
                "foregroundColor": text_color,
                "fontSize": font_size,
                "bold": bold,
                "italic": italic
            },
            "horizontalAlignment": "LEFT"  # Better for documentation text
        }

        # Add to formatting queue for batch processing
        self.formatting_queue.append({
            'range': range_notation,
            'format': format_dict
        })

    def process_formatting_queue(self):
        """Process all queued formatting requests using efficient batch operations."""
        if not self.formatting_queue:
            return

        print(f"🎨 Processing {len(self.formatting_queue)} formatting requests using batch operations...")

        try:
            # Prepare batch update requests
            batch_requests = []

            for format_request in self.formatting_queue:
                # Convert A1 notation to GridRange for batch update
                range_parts = format_request['range'].split(':')
                start_cell = range_parts[0] if range_parts else format_request['range']
                end_cell = range_parts[1] if len(range_parts) > 1 else start_cell

                # Parse cell references (simple A1 notation parser)
                start_col, start_row = self._parse_cell_reference(start_cell)
                end_col, end_row = self._parse_cell_reference(end_cell)

                batch_request = {
                    "repeatCell": {
                        "range": {
                            "sheetId": self.explainer_sheet.id,
                            "startRowIndex": start_row - 1,  # 0-indexed
                            "endRowIndex": end_row,
                            "startColumnIndex": start_col - 1,  # 0-indexed
                            "endColumnIndex": end_col
                        },
                        "cell": {
                            "userEnteredFormat": format_request['format']
                        },
                        "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
                    }
                }
                batch_requests.append(batch_request)

            # Execute batch update in chunks to avoid API limits
            chunk_size = 20  # Conservative chunk size
            for i in range(0, len(batch_requests), chunk_size):
                chunk = batch_requests[i:i + chunk_size]

                print(f"   Processing chunk {i//chunk_size + 1}/{(len(batch_requests) + chunk_size - 1)//chunk_size}...")

                # Use the spreadsheet's batch_update method
                self.explainer_sheet.spreadsheet.batch_update({
                    "requests": chunk
                })

                # Small delay between chunks
                if i + chunk_size < len(batch_requests):
                    time.sleep(0.5)

            print("✓ All formatting applied successfully using batch operations")

        except Exception as e:
            print(f"❌ Error in batch formatting: {e}")
            print("⚠ Falling back to individual formatting requests...")
            # Fallback to individual requests if batch fails
            self._fallback_individual_formatting()

        # Clear the queue
        self.formatting_queue.clear()

    def _parse_cell_reference(self, cell_ref):
        """Parse cell reference like 'A1' or 'AC25' into column and row numbers."""
        import re
        match = re.match(r'([A-Z]+)(\d+)', cell_ref)
        if not match:
            raise ValueError(f"Invalid cell reference: {cell_ref}")

        col_letters, row_num = match.groups()

        # Convert column letters to number (A=1, B=2, ..., Z=26, AA=27, etc.)
        col_num = 0
        for char in col_letters:
            col_num = col_num * 26 + (ord(char) - ord('A') + 1)

        return col_num, int(row_num)

    def _fallback_individual_formatting(self):
        """Fallback method using individual format calls if batch fails."""
        for format_request in self.formatting_queue:
            try:
                self.explainer_sheet.format(format_request['range'], format_request['format'])
                time.sleep(0.1)  # Small delay between requests
            except Exception as e:
                print(f"   Warning: Could not format {format_request['range']}: {e}")

    def populate_explainer_content(self):
        """Populate the explainer sheet with comprehensive documentation."""
        try:
            current_row = 1

            # Title and Introduction
            intro_data = [
                ["PULLUS LOGISTICS DASHBOARD - METRICS CALCULATION GUIDE", "", "", "", "", "", "", "", "", ""],
                ["", "", "", "", "", "", "", "", "", ""],
                ["This document explains how every metric in the Pullus Logistics Dashboard is calculated.", "", "", "", "", "", "", "", "", ""],
                ["", "", "", "", "", "", "", "", "", ""],
            ]

            self.explainer_sheet.update(intro_data, f'A{current_row}:J{current_row + len(intro_data) - 1}')

            # Format title
            self.format_cell_range(f'A{current_row}:J{current_row}', self.colors['header'], bold=True,
                                 text_color={'red': 1, 'green': 1, 'blue': 1})

            current_row += len(intro_data)

            # Table of Contents
            toc_data = [
                ["TABLE OF CONTENTS", "", "", "", "", "", "", "", "", ""],
                ["1. Cost Allocation Methodology Overview", "", "", "", "", "", "", "", "", ""],
                ["2. Bird Cost Calculations", "", "", "", "", "", "", "", "", ""],
                ["3. Crate Cost Calculations (Eggs)", "", "", "", "", "", "", "", "", ""],
                ["4. Product-Specific Cost per kg", "", "", "", "", "", "", "", "", ""],
                ["5. Shipment Scenarios & Examples", "", "", "", "", "", "", "", "", ""],
                ["6. Benchmark Calculations", "", "", "", "", "", "", "", "", ""],
                ["7. Aggregation Methods (GAAP Compliant)", "", "", "", "", "", "", "", "", ""],
                ["8. Common Questions & Edge Cases", "", "", "", "", "", "", "", "", ""],
                ["", "", "", "", "", "", "", "", "", ""],
            ]

            self.explainer_sheet.update(toc_data, f'A{current_row}:J{current_row + len(toc_data) - 1}')

            # Format TOC header
            self.format_cell_range(f'A{current_row}:J{current_row}', self.colors['subheader'], bold=True)

            current_row += len(toc_data)

            # Section 1: Cost Allocation Methodology Overview
            current_row = self.add_section_1_overview(current_row)

            # Section 2: Bird Cost Calculations
            current_row = self.add_section_2_bird_costs(current_row)

            # Section 3: Crate Cost Calculations
            current_row = self.add_section_3_crate_costs(current_row)

            # Section 4: Product-Specific Costs
            current_row = self.add_section_4_product_costs(current_row)

            # Section 5: Shipment Scenarios
            current_row = self.add_section_5_scenarios(current_row)

            # Section 6: Benchmark Calculations
            current_row = self.add_section_6_benchmarks(current_row)

            # Section 7: Aggregation Methods
            current_row = self.add_section_7_aggregation(current_row)

            # Section 8: Common Questions
            current_row = self.add_section_8_faq(current_row)

            print("✓ Successfully populated explainer sheet content")
            return True

        except Exception as e:
            print(f"❌ Error populating explainer content: {e}")
            return False

    def add_section_1_overview(self, start_row):
        """Add Section 1: Cost Allocation Methodology Overview."""
        overview_data = [
            ["1. COST ALLOCATION METHODOLOGY OVERVIEW", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["The Pullus Logistics Dashboard uses intelligent cost allocation to distribute", "", "", "", "", "", "", "", "", ""],
            ["total transportation costs across different products and units transported.", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["KEY PRINCIPLES:", "", "", "", "", "", "", "", "", ""],
            ["• Cost Sharing: Costs distributed based on weight or space usage", "", "", "", "", "", "", "", "", ""],
            ["• Space Equivalency: 1 crate = 15kg equivalent space for mixed shipments", "", "", "", "", "", "", "", "", ""],
            ["• Total Cost: Uses Logistics + Fuel + Miscellaneous costs", "", "", "", "", "", "", "", "", ""],
            ["• Industry Standard: Weighted average method for accurate cost calculations", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["COST COMPONENTS:", "", "", "", "", "", "", "", "", ""],
            ["Total Cost = Logistics Cost + Fuel Cost + Miscellaneous Cost", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(overview_data, f'A{start_row}:J{start_row + len(overview_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format key principles
        self.format_cell_range(f'A{start_row + 5}:J{start_row + 5}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 11}:J{start_row + 11}', self.colors['subheader'], bold=True)

        # Format formula
        self.format_cell_range(f'A{start_row + 12}:J{start_row + 12}', self.colors['formula'], bold=True)

        return start_row + len(overview_data)

    def add_section_2_bird_costs(self, start_row):
        """Add Section 2: Bird Cost Calculations."""
        bird_data = [
            ["2. BIRD COST CALCULATIONS", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Bird costs are calculated differently based on shipment composition:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO A: Pure Whole Chicken Shipment", "", "", "", "", "", "", "", "", ""],
            ["Formula: Whole_Chicken_Cost_per_Bird = Total_Cost / Number_of_Birds", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO B: Mixed Weight Products + Birds", "", "", "", "", "", "", "", "", ""],
            ["1. Calculate total product weight from all products in shipment", "", "", "", "", "", "", "", "", ""],
            ["2. Allocate cost proportionally by weight:", "", "", "", "", "", "", "", "", ""],
            ["   Whole_Chicken_Allocated_Cost = (Whole_Chicken_Weight ÷ Total_Product_Weight) × Total_Cost", "", "", "", "", "", "", "", "", ""],
            ["3. Formula: Whole_Chicken_Cost_per_Bird = Allocated_Whole_Chicken_Cost ÷ Number_of_Birds", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO C: Mixed Shipment (Products + Crates + Birds)", "", "", "", "", "", "", "", "", ""],
            ["1. Convert crates to space equivalent (1 crate = 15kg space)", "", "", "", "", "", "", "", "", ""],
            ["2. Calculate total equivalent weight = Product_Weight + (Crates × 15)", "", "", "", "", "", "", "", "", ""],
            ["3. Allocate remaining cost (after eggs) to weight-based products", "", "", "", "", "", "", "", "", ""],
            ["4. Proportionally distribute to whole chicken based on weight", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["EXAMPLE A (Pure Whole Chicken):", "", "", "", "", "", "", "", "", ""],
            ["Shipment: 100 birds, 500kg whole chicken only, ₦50,000 total cost", "", "", "", "", "", "", "", "", ""],
            ["Results:", "", "", "", "", "", "", "", "", ""],
            ["- Whole_Chicken_Cost_per_Bird = ₦50,000 ÷ 100 = ₦500 per bird", "", "", "", "", "", "", "", "", ""],
            ["- Whole_Chicken_Cost_per_kg = ₦50,000 ÷ 500kg = ₦100 per kg", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["EXAMPLE B (Mixed Products + Birds):", "", "", "", "", "", "", "", "", ""],
            ["Shipment: 100 birds, 300kg whole chicken + 200kg gizzard, ₦50,000 total cost", "", "", "", "", "", "", "", "", ""],
            ["Calculation:", "", "", "", "", "", "", "", "", ""],
            ["- Total product weight: 300kg + 200kg = 500kg", "", "", "", "", "", "", "", "", ""],
            ["- Whole chicken allocation: (300 ÷ 500) × ₦50,000 = ₦30,000", "", "", "", "", "", "", "", "", ""],
            ["- Cost per bird: ₦30,000 ÷ 100 = ₦300 per bird", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(bird_data, f'A{start_row}:J{start_row + len(bird_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format scenario headers
        self.format_cell_range(f'A{start_row + 4}:J{start_row + 4}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 7}:J{start_row + 7}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 12}:J{start_row + 12}', self.colors['subheader'], bold=True)

        # Format formulas
        self.format_cell_range(f'A{start_row + 5}:J{start_row + 5}', self.colors['formula'])
        self.format_cell_range(f'A{start_row + 10}:J{start_row + 10}', self.colors['formula'])

        # Format example
        self.format_cell_range(f'A{start_row + 17}:J{start_row + 21}', self.colors['example'])

        return start_row + len(bird_data)

    def add_section_3_crate_costs(self, start_row):
        """Add Section 3: Crate Cost Calculations."""
        crate_data = [
            ["3. CRATE COST CALCULATIONS (EGGS)", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Egg crate costs are calculated based on shipment type:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO A: Pure Egg Shipment (Crates Only)", "", "", "", "", "", "", "", "", ""],
            ["Formula: Egg_Cost_per_Crate = Total_Cost / Number_of_Crates", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO B: Mixed Shipment (Crates + Weight Products)", "", "", "", "", "", "", "", "", ""],
            ["1. Calculate space allocation:", "", "", "", "", "", "", "", "", ""],
            ["   - Crate_Equivalent_Weight = Number_of_Crates × 15kg", "", "", "", "", "", "", "", "", ""],
            ["   - Total_Equivalent_Weight = Product_Weight + Crate_Equivalent_Weight", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["2. Allocate cost to eggs:", "", "", "", "", "", "", "", "", ""],
            ["   - Egg_Allocated_Cost = (Crate_Equivalent_Weight ÷ Total_Equivalent_Weight) × Total_Cost", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["3. Calculate per crate cost:", "", "", "", "", "", "", "", "", ""],
            ["   - Egg_Cost_per_Crate = Egg_Allocated_Cost ÷ Number_of_Crates", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["EXAMPLE:", "", "", "", "", "", "", "", "", ""],
            ["Shipment: 10 crates + 300kg products, ₦30,000 total cost", "", "", "", "", "", "", "", "", ""],
            ["Calculation:", "", "", "", "", "", "", "", "", ""],
            ["- Crate equivalent: 10 × 15 = 150kg", "", "", "", "", "", "", "", "", ""],
            ["- Total equivalent: 300kg + 150kg = 450kg", "", "", "", "", "", "", "", "", ""],
            ["- Egg allocation: (150 ÷ 450) × ₦30,000 = ₦10,000", "", "", "", "", "", "", "", "", ""],
            ["- Per crate: ₦10,000 ÷ 10 = ₦1,000 per crate", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(crate_data, f'A{start_row}:J{start_row + len(crate_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format scenario headers
        self.format_cell_range(f'A{start_row + 4}:J{start_row + 4}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 7}:J{start_row + 7}', self.colors['subheader'], bold=True)

        # Format formulas
        self.format_cell_range(f'A{start_row + 5}:J{start_row + 5}', self.colors['formula'])
        self.format_cell_range(f'A{start_row + 13}:J{start_row + 13}', self.colors['formula'])
        self.format_cell_range(f'A{start_row + 16}:J{start_row + 16}', self.colors['formula'])

        # Format example
        self.format_cell_range(f'A{start_row + 18}:J{start_row + 24}', self.colors['example'])

        return start_row + len(crate_data)

    def add_section_4_product_costs(self, start_row):
        """Add Section 4: Product-Specific Cost per kg."""
        product_data = [
            ["4. PRODUCT-SPECIFIC COST PER KG", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["All product types use the same cost sharing approach:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["PRODUCT TYPES:", "", "", "", "", "", "", "", "", ""],
            ["• Gizzard", "• Whole Chicken", "• Laps", "• Breast", "", "", "", "", "", ""],
            ["• Fillet", "• Wings", "• Bone", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["CALCULATION METHOD:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 1: Calculate Total Product Weight", "", "", "", "", "", "", "", "", ""],
            ["Total_Product_Weight = Sum of all product weights in shipment", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 2: Determine Available Cost for Weight Products", "", "", "", "", "", "", "", "", ""],
            ["If mixed shipment (crates + products):", "", "", "", "", "", "", "", "", ""],
            ["  Available_Cost = Total_Cost - Egg_Allocated_Cost", "", "", "", "", "", "", "", "", ""],
            ["If pure product shipment:", "", "", "", "", "", "", "", "", ""],
            ["  Available_Cost = Total_Cost", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 3: Allocate Cost to Each Product", "", "", "", "", "", "", "", "", ""],
            ["Product_Allocated_Cost = (Product_Weight ÷ Total_Product_Weight) × Available_Cost", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 4: Calculate Cost per kg", "", "", "", "", "", "", "", "", ""],
            ["Product_Cost_per_kg = Product_Allocated_Cost ÷ Product_Weight", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["EXAMPLE:", "", "", "", "", "", "", "", "", ""],
            ["Shipment: 200kg Gizzard + 300kg Whole Chicken, ₦40,000 total cost", "", "", "", "", "", "", "", "", ""],
            ["Calculation:", "", "", "", "", "", "", "", "", ""],
            ["- Total product weight: 200kg + 300kg = 500kg", "", "", "", "", "", "", "", "", ""],
            ["- Gizzard allocation: (200 ÷ 500) × ₦40,000 = ₦16,000", "", "", "", "", "", "", "", "", ""],
            ["- Gizzard cost per kg: ₦16,000 ÷ 200kg = ₦80/kg", "", "", "", "", "", "", "", "", ""],
            ["- Whole Chicken allocation: (300 ÷ 500) × ₦40,000 = ₦24,000", "", "", "", "", "", "", "", "", ""],
            ["- Whole Chicken cost per kg: ₦24,000 ÷ 300kg = ₦80/kg", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(product_data, f'A{start_row}:J{start_row + len(product_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format subsection headers
        self.format_cell_range(f'A{start_row + 4}:J{start_row + 4}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 8}:J{start_row + 8}', self.colors['subheader'], bold=True)

        # Format step headers
        for step_row in [start_row + 10, start_row + 13, start_row + 19, start_row + 22]:
            self.format_cell_range(f'A{step_row}:J{step_row}', self.colors['info'], bold=True)

        # Format formulas
        self.format_cell_range(f'A{start_row + 20}:J{start_row + 20}', self.colors['formula'])
        self.format_cell_range(f'A{start_row + 23}:J{start_row + 23}', self.colors['formula'])

        # Format example
        self.format_cell_range(f'A{start_row + 25}:J{start_row + 32}', self.colors['example'])

        return start_row + len(product_data)

    def add_section_5_scenarios(self, start_row):
        """Add Section 5: Shipment Scenarios & Examples."""
        scenario_data = [
            ["5. SHIPMENT SCENARIOS & EXAMPLES", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["The system handles three main shipment scenarios:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO 1: Pure Weight-Based Products", "", "", "", "", "", "", "", "", ""],
            ["Characteristics: Has product weights, no crates, may have birds", "", "", "", "", "", "", "", "", ""],
            ["Example: 300kg Gizzard + 200kg Whole Chicken + 100 birds", "", "", "", "", "", "", "", "", ""],
            ["Processing:", "", "", "", "", "", "", "", "", ""],
            ["- All cost allocated proportionally by weight", "", "", "", "", "", "", "", "", ""],
            ["- Bird cost calculated from whole chicken allocation", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO 2: Pure Crate Shipment (Eggs Only)", "", "", "", "", "", "", "", "", ""],
            ["Characteristics: Has crates, no product weights, no birds", "", "", "", "", "", "", "", "", ""],
            ["Example: 50 egg crates", "", "", "", "", "", "", "", "", ""],
            ["Processing:", "", "", "", "", "", "", "", "", ""],
            ["- All cost allocated to eggs", "", "", "", "", "", "", "", "", ""],
            ["- Cost per crate = Total cost ÷ Number of crates", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SCENARIO 3: Mixed Shipment", "", "", "", "", "", "", "", "", ""],
            ["Characteristics: Has both crates and product weights", "", "", "", "", "", "", "", "", ""],
            ["Example: 20 crates + 400kg products", "", "", "", "", "", "", "", "", ""],
            ["Processing:", "", "", "", "", "", "", "", "", ""],
            ["- Convert crates to space equivalent (20 × 15kg = 300kg)", "", "", "", "", "", "", "", "", ""],
            ["- Total equivalent weight = 400kg + 300kg = 700kg", "", "", "", "", "", "", "", "", ""],
            ["- Allocate cost: Eggs get 300/700, Products get 400/700", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["EDGE CASES:", "", "", "", "", "", "", "", "", ""],
            ["• Zero costs: No allocation performed", "", "", "", "", "", "", "", "", ""],
            ["• Zero weights/crates: Respective calculations skipped", "", "", "", "", "", "", "", "", ""],
            ["• Mixed products + birds: Bird cost from whole chicken allocation only", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(scenario_data, f'A{start_row}:J{start_row + len(scenario_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format scenario headers
        for scenario_row in [start_row + 4, start_row + 11, start_row + 18]:
            self.format_cell_range(f'A{scenario_row}:J{scenario_row}', self.colors['subheader'], bold=True)

        # Format edge cases
        self.format_cell_range(f'A{start_row + 26}:J{start_row + 26}', self.colors['warning'], bold=True)

        return start_row + len(scenario_data)

    def add_section_6_benchmarks(self, start_row):
        """Add Section 6: Benchmark Calculations."""
        benchmark_data = [
            ["6. BENCHMARK CALCULATIONS", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Benchmark analysis compares actual costs against predefined targets:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["BENCHMARK VALUES:", "", "", "", "", "", "", "", "", ""],
            ["• Offtake Operations: ₦40/kg", "", "", "", "", "", "", "", "", ""],
            ["• Supply Operations: ₦80/kg", "", "", "", "", "", "", "", "", ""],
            ["• Kaduna → Abuja Supply: ₦50/kg", "", "", "", "", "", "", "", "", ""],
            ["• Abuja Internal Supply: ₦30/kg", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["CALCULATION METHOD:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 1: Calculate Actual Cost per kg", "", "", "", "", "", "", "", "", ""],
            ["Individual Transaction: Actual_Cost = Total_Cost ÷ Total_Weight", "", "", "", "", "", "", "", "", ""],
            ["Aggregated Monthly: Actual_Cost = Sum_of_Costs ÷ Sum_of_Weights", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 2: Calculate Budget Performance Percentage", "", "", "", "", "", "", "", "", ""],
            ["Budget_Performance% = ((Actual_Cost - Budget_Target) ÷ Budget_Target) × 100", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["STEP 3: Determine Status", "", "", "", "", "", "", "", "", ""],
            ["• Within Budget: Performance ≤ 5% over target", "", "", "", "", "", "", "", "", ""],
            ["• Near Budget: Performance > 5% and ≤ 20% over target", "", "", "", "", "", "", "", "", ""],
            ["• Over Budget: Performance > 20% over target", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["AVERAGE OVERAGE CALCULATION:", "", "", "", "", "", "", "", "", ""],
            ["Shows how much we typically overspend on severely over-budget trips", "", "", "", "", "", "", "", "", ""],
            ["Only counts trips that exceeded budget by more than 20%", "", "", "", "", "", "", "", "", ""],
            ["Average Overage = Average of all overspend percentages above 20%", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["EXAMPLE:", "", "", "", "", "", "", "", "", ""],
            ["Actual cost: ₦90/kg, Benchmark: ₦80/kg", "", "", "", "", "", "", "", "", ""],
            ["Budget Performance: ((90-80) ÷ 80) × 100 = 12.5% over budget", "", "", "", "", "", "", "", "", ""],
            ["Status: Near Budget (because 12.5% > 5% but ≤ 20%)", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(benchmark_data, f'A{start_row}:J{start_row + len(benchmark_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format subsection headers
        self.format_cell_range(f'A{start_row + 4}:J{start_row + 4}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 10}:J{start_row + 10}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 24}:J{start_row + 24}', self.colors['subheader'], bold=True)

        # Format step headers
        for step_row in [start_row + 12, start_row + 16, start_row + 19]:
            self.format_cell_range(f'A{step_row}:J{step_row}', self.colors['info'], bold=True)

        # Format formulas
        self.format_cell_range(f'A{start_row + 17}:J{start_row + 17}', self.colors['formula'])
        self.format_cell_range(f'A{start_row + 26}:J{start_row + 26}', self.colors['formula'])

        # Format example
        self.format_cell_range(f'A{start_row + 28}:J{start_row + 31}', self.colors['example'])

        return start_row + len(benchmark_data)

    def add_section_7_aggregation(self, start_row):
        """Add Section 7: Aggregation Methods."""
        aggregation_data = [
            ["7. AGGREGATION METHODS (GAAP COMPLIANT)", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["The dashboard uses industry-standard calculation methods for accurate reporting:", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["WEIGHTED AVERAGE METHOD (Recommended):", "", "", "", "", "", "", "", "", ""],
            ["Formula: Total_Cost ÷ Total_Weight", "", "", "", "", "", "", "", "", ""],
            ["Example: ₦500,000 total cost ÷ 5,000kg total weight = ₦100/kg", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Benefits:", "", "", "", "", "", "", "", "", ""],
            ["• More accurate for business decisions", "", "", "", "", "", "", "", "", ""],
            ["• Accounts for different shipment sizes properly", "", "", "", "", "", "", "", "", ""],
            ["• GAAP compliant for financial reporting", "", "", "", "", "", "", "", "", ""],
            ["• Eliminates bias from small/large shipment outliers", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["SIMPLE AVERAGE METHOD (For Reference):", "", "", "", "", "", "", "", "", ""],
            ["Formula: Simple average of all individual cost per kg values", "", "", "", "", "", "", "", "", ""],
            ["Example: (₦90/kg + ₦110/kg + ₦95/kg) ÷ 3 = ₦98.33/kg", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Issues with Simple Average:", "", "", "", "", "", "", "", "", ""],
            ["• Can be skewed by outlier transactions", "", "", "", "", "", "", "", "", ""],
            ["• Doesn't weight by transaction size", "", "", "", "", "", "", "", "", ""],
            ["• May not represent true business cost", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["DASHBOARD USAGE:", "", "", "", "", "", "", "", "", ""],
            ["• Purchase/Supply Cost per kg: Uses AGGREGATE method", "", "", "", "", "", "", "", "", ""],
            ["• Overall KPIs: Uses AGGREGATE method", "", "", "", "", "", "", "", "", ""],
            ["• Benchmark Status: Based on AGGREGATED costs (not individual)", "", "", "", "", "", "", "", "", ""],
            ["• Product-specific costs: Individual allocation, then simple average", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(aggregation_data, f'A{start_row}:J{start_row + len(aggregation_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format method headers
        self.format_cell_range(f'A{start_row + 4}:J{start_row + 4}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 14}:J{start_row + 14}', self.colors['subheader'], bold=True)
        self.format_cell_range(f'A{start_row + 22}:J{start_row + 22}', self.colors['subheader'], bold=True)

        # Format formulas
        self.format_cell_range(f'A{start_row + 5}:J{start_row + 6}', self.colors['formula'])
        self.format_cell_range(f'A{start_row + 15}:J{start_row + 16}', self.colors['formula'])

        # Format warnings
        self.format_cell_range(f'A{start_row + 18}:J{start_row + 18}', self.colors['warning'], bold=True)

        return start_row + len(aggregation_data)

    def add_section_8_faq(self, start_row):
        """Add Section 8: Common Questions & FAQ."""
        faq_data = [
            ["8. COMMON QUESTIONS & EDGE CASES", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Q1: Why do Total Birds/Weight count both offtake and supply?", "", "", "", "", "", "", "", "", ""],
            ["A1: This reflects total operational activity. Each leg represents real", "", "", "", "", "", "", "", "", ""],
            ["    logistics work and cost. Same birds may be counted twice to show", "", "", "", "", "", "", "", "", ""],
            ["    total transportation workload, not just inventory movement.", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Q2: What happens with zero weights or costs?", "", "", "", "", "", "", "", "", ""],
            ["A2: Zero values are handled gracefully:", "", "", "", "", "", "", "", "", ""],
            ["    • Zero costs: No allocation performed, costs remain 0", "", "", "", "", "", "", "", "", ""],
            ["    • Zero weights: Product skipped in allocation", "", "", "", "", "", "", "", "", ""],
            ["    • Zero crates: Crate calculations skipped", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Q3: How are mixed shipments with birds handled?", "", "", "", "", "", "", "", "", ""],
            ["A3: Birds are only counted if whole chicken weight exists:", "", "", "", "", "", "", "", "", ""],
            ["    • System allocates cost to whole chicken by weight", "", "", "", "", "", "", "", "", ""],
            ["    • Then calculates bird cost: Allocated Cost ÷ Number of Birds", "", "", "", "", "", "", "", "", ""],
            ["    • Other products don't contribute to bird cost calculations", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Q4: Why use 15kg equivalent for crates?", "", "", "", "", "", "", "", "", ""],
            ["A4: This represents space/volume equivalency in transportation:", "", "", "", "", "", "", "", "", ""],
            ["    • 1 egg crate occupies similar space as 15kg of products", "", "", "", "", "", "", "", "", ""],
            ["    • Allows fair cost allocation in mixed shipments", "", "", "", "", "", "", "", "", ""],
            ["    • Can be adjusted if operational data suggests different ratio", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Q5: How is benchmark status determined for aggregated data?", "", "", "", "", "", "", "", "", ""],
            ["A5: Uses AGGREGATED costs, not worst individual transaction:", "", "", "", "", "", "", "", "", ""],
            ["    • Calculates monthly average cost for category", "", "", "", "", "", "", "", "", ""],
            ["    • Compares average against benchmark", "", "", "", "", "", "", "", "", ""],
            ["    • More representative of overall performance", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
            ["Q6: What's the difference between Total Cost and Logistics Cost?", "", "", "", "", "", "", "", "", ""],
            ["A6: Total Cost is comprehensive:", "", "", "", "", "", "", "", "", ""],
            ["    • Total Cost = Logistics + Fuel + Miscellaneous costs", "", "", "", "", "", "", "", "", ""],
            ["    • Logistics Cost = Just the transportation service fee", "", "", "", "", "", "", "", "", ""],
            ["    • All calculations use Total Cost for accuracy", "", "", "", "", "", "", "", "", ""],
            ["", "", "", "", "", "", "", "", "", ""],
        ]

        self.explainer_sheet.update(faq_data, f'A{start_row}:J{start_row + len(faq_data) - 1}')

        # Format section header
        self.format_cell_range(f'A{start_row}:J{start_row}', self.colors['header'], bold=True,
                             text_color={'red': 1, 'green': 1, 'blue': 1})

        # Format questions with better organization
        question_rows = [start_row + 2, start_row + 8, start_row + 14, start_row + 20, start_row + 26, start_row + 32]
        answer_rows = [start_row + 3, start_row + 9, start_row + 15, start_row + 21, start_row + 27, start_row + 33]

        # Format questions with blue background
        for q_row in question_rows:
            if q_row < start_row + len(faq_data):
                self.format_cell_range(f'A{q_row}:J{q_row}', self.colors['subheader'], bold=True)

        # Format answers with light background
        for a_row in answer_rows:
            if a_row < start_row + len(faq_data):
                self.format_cell_range(f'A{a_row}:J{a_row}', self.colors['info'], bold=False)

        # Format bullet point rows for better readability
        for i in range(start_row + 1, start_row + len(faq_data)):
            # Skip questions and answer headers already formatted
            if i not in question_rows and i not in answer_rows:
                # Check if it's a bullet point or sub-answer row
                if i - start_row in [4, 5, 6, 10, 11, 12, 16, 17, 18, 22, 23, 24, 28, 29, 30, 34, 35, 36]:
                    self.format_cell_range(f'A{i}:J{i}', self.colors['example'], bold=False)

        return start_row + len(faq_data)

    def run(self):
        """Main execution method."""
        print("🚀 Starting Metrics Explainer Sheet Creation...")
        print("=" * 60)

        # Step 1: Connect to Google Sheets
        if not self.connect():
            return False

        # Step 2: Create the explainer sheet
        if not self.create_explainer_sheet():
            return False

        # Step 3: Populate content
        if not self.populate_explainer_content():
            return False

        # Step 4: Process all formatting in controlled batches
        self.process_formatting_queue()

        print("=" * 60)
        print("🎉 Metrics Explainer Sheet created successfully!")
        print("📊 Sheet contains comprehensive documentation of all cost calculations")
        print("📈 Access it in your 'Pullus Logistics Dashboard' spreadsheet")

        return True

if __name__ == "__main__":
    creator = MetricsExplainerCreator()
    success = creator.run()

    if success:
        print("\n✅ Process completed successfully!")
    else:
        print("\n❌ Process failed. Check error messages above.")