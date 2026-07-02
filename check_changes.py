#!/usr/bin/env python3
"""
Check if source data worksheet has been modified since last update.
Uses content hash of the source worksheet instead of Drive API timestamp.
"""

import json
import os
import sys
import time
import hashlib
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import gspread

def load_env_file():
    """Load environment variables from .env file if it exists."""
    env_file = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(env_file):
        with open(env_file, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip().strip('"\'')
                    os.environ[key] = value

def get_credentials():
    """Get Google API credentials from environment."""
    # Load .env file first
    load_env_file()

    service_account_info = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    if not service_account_info:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT environment variable not set")

    try:
        # Check if it's a file path or JSON content
        if service_account_info.startswith('/') or service_account_info.endswith('.json'):
            # It's a file path - for local development
            credentials = Credentials.from_service_account_file(
                service_account_info,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets.readonly',
                    'https://www.googleapis.com/auth/drive.metadata.readonly'
                ]
            )
            print(f"🔑 Using service account file: {service_account_info}")
        else:
            # It's JSON content - for GitHub Actions
            credentials_dict = json.loads(service_account_info)
            credentials = Credentials.from_service_account_info(
                credentials_dict,
                scopes=[
                    'https://www.googleapis.com/auth/spreadsheets.readonly',
                    'https://www.googleapis.com/auth/drive.metadata.readonly'
                ]
            )
            print("🔑 Using service account from environment variable")

        return credentials

    except json.JSONDecodeError as e:
        print(f"❌ Error parsing service account JSON: {e}")
        sys.exit(1)
    except FileNotFoundError as e:
        print(f"❌ Service account file not found: {service_account_info}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error creating credentials: {e}")
        sys.exit(1)

def get_source_data_hash(spreadsheet_id, credentials, source_worksheet_name="Logistics Data"):
    """Get content hash of the source data worksheet."""
    max_retries = 4
    try:
        # Retry transient API errors (rate limits / server errors) with backoff
        for attempt in range(max_retries):
            try:
                # Use gspread for easier worksheet access
                gc = gspread.authorize(credentials)
                spreadsheet = gc.open_by_key(spreadsheet_id)

                # Get the source worksheet
                source_worksheet = spreadsheet.worksheet(source_worksheet_name)

                # Get all values from the source worksheet
                all_values = source_worksheet.get_all_values()
                break
            except gspread.exceptions.APIError as e:
                status = e.response.status_code
                if (status == 429 or status >= 500) and attempt < max_retries - 1:
                    wait_time = 5 * (2 ** attempt)
                    print(f"Transient API error {status} (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise

        # Create hash of the content
        content_str = str(all_values)
        content_hash = hashlib.md5(content_str.encode('utf-8')).hexdigest()

        print(f"📊 Source worksheet: {source_worksheet_name}")
        print(f"📝 Rows with data: {len([row for row in all_values if any(cell.strip() for cell in row)])}")
        print(f"🔗 Content hash: {content_hash}")

        return content_hash

    except gspread.WorksheetNotFound:
        print(f"❌ Source worksheet '{source_worksheet_name}' not found")
        print("Available worksheets:")
        try:
            gc = gspread.authorize(credentials)
            spreadsheet = gc.open_by_key(spreadsheet_id)
            for ws in spreadsheet.worksheets():
                print(f"  - {ws.title}")
        except Exception:
            pass
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error getting source data hash: {e}")
        sys.exit(1)

def load_last_hash():
    """Load the last processed content hash from file."""
    hash_file = 'last_source_hash.json'
    try:
        if os.path.exists(hash_file):
            with open(hash_file, 'r') as f:
                data = json.load(f)
                return data.get('content_hash')
        return None
    except Exception as e:
        print(f"⚠️  Warning: Could not load last hash: {e}")
        return None

def save_hash(content_hash):
    """Save the current content hash to file."""
    hash_file = 'last_source_hash.json'
    try:
        data = {
            'content_hash': content_hash,
            'updated_at': hashlib.md5(str(content_hash).encode()).hexdigest()  # Simple timestamp alternative
        }
        with open(hash_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved new content hash: {content_hash}")
    except Exception as e:
        print(f"❌ Error saving hash: {e}")
        sys.exit(1)

def main():
    """Main function to check for changes in source data."""
    try:
        # Load .env file first
        load_env_file()

        # Get environment variables
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        if not spreadsheet_id:
            print("❌ SPREADSHEET_ID environment variable not set")
            sys.exit(1)

        # Allow customization of source worksheet name
        source_worksheet_name = os.getenv('SOURCE_WORKSHEET_NAME', 'Logistics Data')

        print(f"🔍 Checking for changes in source worksheet '{source_worksheet_name}'...")

        # Get credentials
        credentials = get_credentials()

        # Get current source data hash
        current_hash = get_source_data_hash(spreadsheet_id, credentials, source_worksheet_name)

        # Load last processed hash
        last_hash = load_last_hash()
        print(f"📅 Last processed hash: {last_hash or 'Never'}")

        # Compare hashes
        if current_hash != last_hash:
            print("✅ Source data changes detected! Update needed.")
            save_hash(current_hash)
            print("NEEDS_UPDATE=true")
            return True
        else:
            print("⏭️  No changes in source data detected. Skipping update.")
            print("NEEDS_UPDATE=false")
            return False

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()