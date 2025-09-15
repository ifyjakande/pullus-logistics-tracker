# Smart Google Sheets Workflow Optimization Guide

This guide documents how to implement timestamp-based change detection for Google Sheets workflows to reduce unnecessary execution by ~80%.

## 🎯 Problem Statement

Standard workflows that run on a schedule (e.g., every 30 minutes) execute regardless of whether the source data has changed, wasting resources and GitHub Actions minutes.

## 💡 Solution Overview

Implement a two-stage workflow:
1. **Stage 1**: Lightweight change detection (~15 seconds)
2. **Stage 2**: Heavy processing (only when changes detected)

## 🏗️ Implementation Steps

### 1. Create Change Detection Script (`check_changes.py`)

```python
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
                    value = value.strip().strip('"\'')
                    os.environ[key] = value

def get_credentials():
    """Get Google API credentials from environment."""
    load_env_file()

    service_account_info = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    if not service_account_info:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT environment variable not set")

    try:
        credentials_dict = json.loads(service_account_info)
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
        drive_service = build('drive', 'v3', credentials=credentials)
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
        spreadsheet_id = os.getenv('SPREADSHEET_ID')
        if not spreadsheet_id:
            print("❌ SPREADSHEET_ID environment variable not set")
            sys.exit(1)

        print("🔍 Checking for changes in Google Sheet...")

        credentials = get_credentials()
        current_timestamp, sheet_name = get_sheet_last_modified(spreadsheet_id, credentials)
        print(f"📊 Sheet: {sheet_name}")
        print(f"🕒 Current modified time: {current_timestamp}")

        last_timestamp = load_last_timestamp()
        print(f"📅 Last processed time: {last_timestamp or 'Never'}")

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
```

### 2. Update Main Script for Dual Credential Support

Add this to your main processing script:

```python
if __name__ == "__main__":
    # Try GOOGLE_SERVICE_ACCOUNT (JSON content) first, then fall back to file path
    service_account_json = os.getenv('GOOGLE_SERVICE_ACCOUNT')
    credentials_path = os.getenv('GOOGLE_SERVICE_ACCOUNT_FILE')
    spreadsheet_id = os.getenv('SPREADSHEET_ID')

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
            updater = YourUpdaterClass(temp_credentials_path, spreadsheet_id)
            success = updater.run_update()
        finally:
            os.unlink(temp_credentials_path)
    else:
        updater = YourUpdaterClass(credentials_path, spreadsheet_id)
        success = updater.run_update()
```

### 3. Configure GitHub Actions Workflow

```yaml
name: Update Dashboard

on:
  schedule:
    - cron: '*/5 * * * *'  # Every 5 minutes
  workflow_dispatch:

permissions:
  contents: write

jobs:
  check-changes:
    runs-on: ubuntu-latest
    outputs:
      needs_update: ${{ steps.check.outputs.needs_update }}

    steps:
    - name: Checkout repository
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install google-auth google-api-python-client

    - name: Check for changes
      id: check
      env:
        GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
        SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
        CI: "true"
      run: |
        python check_changes.py > check_output.txt 2>&1
        if grep -q "NEEDS_UPDATE=true" check_output.txt; then
          echo "needs_update=true" >> $GITHUB_OUTPUT
        else
          echo "needs_update=false" >> $GITHUB_OUTPUT
        fi
        cat check_output.txt

    - name: Commit timestamp file
      if: steps.check.outputs.needs_update == 'true'
      run: |
        git config --local user.email "action@github.com"
        git config --local user.name "GitHub Action"
        git add -f last_update.json
        git diff --staged --quiet || git commit -m "Update last processed timestamp [skip ci]"
        git push

  update-dashboard:
    needs: check-changes
    if: always() && needs.check-changes.result == 'success' && needs.check-changes.outputs.needs_update == 'true'
    runs-on: ubuntu-latest

    steps:
    - name: Checkout repository
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install gspread google-auth pandas  # Add your dependencies

    - name: Update dashboard
      env:
        GOOGLE_SERVICE_ACCOUNT: ${{ secrets.GOOGLE_SERVICE_ACCOUNT }}
        SPREADSHEET_ID: ${{ secrets.SPREADSHEET_ID }}
      run: |
        python your_main_script.py
```

### 4. Update .gitignore

```gitignore
# Service account credentials - never commit these!
*.json
your-service-account-*.json

# Exception: Allow timestamp tracking file
!last_update.json

# Python cache
__pycache__/
*.py[cod]
*$py.class

# Environment files
.env
.env.local
.env.*.local

# IDE files
.vscode/
.idea/
*.swp
*.swo

# OS files
.DS_Store
Thumbs.db

# Logs
*.log
```

## 🔧 Required GitHub Secrets

Set these in your repository settings → Secrets and variables → Actions:

1. **GOOGLE_SERVICE_ACCOUNT**: Full service account JSON content
2. **SPREADSHEET_ID**: Your Google Sheet ID from the URL

## 🚨 Critical Implementation Notes

### Conditional Execution Fix
The most critical fix for GitHub Actions conditional execution:

```yaml
if: always() && needs.check-changes.result == 'success' && needs.check-changes.outputs.needs_update == 'true'
```

Without `always()`, the job won't run even when conditions are met.

### Gitignore Exception
Essential for timestamp tracking:

```gitignore
# Block all JSON files but allow the timestamp file
*.json
!last_update.json
```

### Forced Git Add
Use `git add -f` to add the timestamp file despite gitignore:

```bash
git add -f last_update.json
```

### Google API Scopes
Required for both scripts:

```python
scopes=[
    'https://www.googleapis.com/auth/spreadsheets.readonly',  # For reading sheet data
    'https://www.googleapis.com/auth/drive.metadata.readonly'  # For checking modification time
]
```

## 📊 Expected Results

- **No Changes**: Workflow completes in ~15 seconds (check-changes job only)
- **With Changes**: Full workflow runs (check-changes + update-dashboard)
- **Resource Savings**: ~80% reduction in unnecessary workflow executions
- **Cost Savings**: Significant reduction in GitHub Actions minutes usage

## 🔄 Testing the Implementation

1. **Initial Setup**: Run workflow manually to create initial timestamp
2. **No Changes**: Run again - should skip update stage
3. **With Changes**: Modify sheet, run workflow - should execute both stages
4. **Verify Logs**: Check GitHub Actions logs for proper conditional execution

## 🎯 Key Success Factors

1. **Proper Conditional Logic**: Use `always()` function correctly
2. **Dual Credential Support**: Handle both local dev and CI/CD environments
3. **Timestamp Persistence**: Ensure `last_update.json` can be committed
4. **Error Handling**: Robust error handling in change detection
5. **API Permissions**: Correct Google API scopes for both Drive and Sheets

## 🔗 Adaptable to Other Projects

This pattern works for any Google Sheets-based automation:

1. **Replace** `your_main_script.py` with your processing script
2. **Update** dependencies in workflow YAML
3. **Modify** credential handling if using different libraries
4. **Adjust** schedule frequency as needed
5. **Customize** timestamp file location if required

## 📈 Performance Impact

- **Before**: 288 workflow runs per day (every 5 min) = ~576 minutes daily
- **After**: ~288 lightweight checks + ~10 actual updates = ~92 minutes daily
- **Savings**: ~484 minutes daily, ~14,520 minutes monthly

This implementation provides massive resource savings while maintaining the same functionality and responsiveness to changes.