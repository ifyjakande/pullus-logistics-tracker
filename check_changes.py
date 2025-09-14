#!/usr/bin/env python3
"""
Check if Google Sheet has been modified since last update.
Uses Google Drive API to check sheet's last modified timestamp.
"""

import json
import os
import sys
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

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
        # Parse JSON from environment variable
        credentials_dict = json.loads(service_account_info)

        # Create credentials
        credentials = Credentials.from_service_account_info(
            credentials_dict,
            scopes=[
                'https://www.googleapis.com/auth/spreadsheets.readonly',
                'https://www.googleapis.com/auth/drive.metadata.readonly'
            ]
        )
        return credentials

    except json.JSONDecodeError as e:
        print(f"❌ Error parsing service account JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error creating credentials: {e}")
        sys.exit(1)

def get_sheet_last_modified(spreadsheet_id, credentials):
    """Get the last modified timestamp of a Google Sheet."""
    try:
        # Build Drive API service
        drive_service = build('drive', 'v3', credentials=credentials)

        # Get file metadata
        file_info = drive_service.files().get(
            fileId=spreadsheet_id,
            fields='modifiedTime,name'
        ).execute()

        return file_info['modifiedTime'], file_info.get('name', 'Unknown')

    except Exception as e:
        print(f"❌ Error getting sheet metadata: {e}")
        sys.exit(1)

def load_last_timestamp():
    """Load the last processed timestamp from file."""
    timestamp_file = 'last_update.json'
    try:
        if os.path.exists(timestamp_file):
            with open(timestamp_file, 'r') as f:
                data = json.load(f)
                return data.get('last_modified')
        return None
    except Exception as e:
        print(f"⚠️  Warning: Could not load last timestamp: {e}")
        return None

def save_timestamp(timestamp):
    """Save the current timestamp to file."""
    timestamp_file = 'last_update.json'
    try:
        data = {
            'last_modified': timestamp,
            'updated_at': timestamp
        }
        with open(timestamp_file, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved new timestamp: {timestamp}")
    except Exception as e:
        print(f"❌ Error saving timestamp: {e}")
        sys.exit(1)

def main():
    """Main function to check for changes."""
    try:
        # Get environment variables
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        if not spreadsheet_id:
            print("❌ SPREADSHEET_ID environment variable not set")
            sys.exit(1)

        print("🔍 Checking for changes in Google Sheet...")

        # Get credentials
        credentials = get_credentials()

        # Get current sheet modification time
        current_timestamp, sheet_name = get_sheet_last_modified(spreadsheet_id, credentials)
        print(f"📊 Sheet: {sheet_name}")
        print(f"🕒 Current modified time: {current_timestamp}")

        # Load last processed timestamp
        last_timestamp = load_last_timestamp()
        print(f"📅 Last processed time: {last_timestamp or 'Never'}")

        # Compare timestamps
        if current_timestamp != last_timestamp:
            print("✅ Changes detected! Update needed.")
            save_timestamp(current_timestamp)
            print("NEEDS_UPDATE=true")
            return True
        else:
            print("⏭️  No changes detected. Skipping update.")
            print("NEEDS_UPDATE=false")
            return False

    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()