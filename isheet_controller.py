from __future__ import print_function
import os.path
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Define the scopes
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# Function to generate credentials
def gen_cred():
    creds = None
    # Check if token.json exists and load credentials from it
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If no valid credentials are available, prompt the user to log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return creds

# Function to clear data in the specified spreadsheet and range
def clear_sheet(creds, spreadsheet_id, range_name):
    try:
        service = build('sheets', 'v4', credentials=creds)
        clear_request = service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id, range=range_name
        )
        clear_request.execute()
        print(f"Data in range {range_name} cleared.")
    except HttpError as error:
        print(f"An error occurred: {error}")

# Function to paste new data into the specified spreadsheet and range
def paste_data(creds, spreadsheet_id, range_name, value_input_option, new_data):
    try:
        service = build('sheets', 'v4', credentials=creds)
        body = {'values': new_data}
        update_request = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id, range=range_name,
            valueInputOption=value_input_option, body=body
        )
        update_request.execute()
        print(f"New data pasted to range {range_name}.")
    except HttpError as error:
        print(f"An error occurred: {error}")

# Function to update the first spreadsheet
def sheet_update(new_data, sheet_name):
    spreadsheet_id = "1fkSAe0FNvO01xV-giOdEU3RQFCNNNml4qJ7pKC8ySe8"
    range_name = f"{sheet_name}!A1:Z99999999"
    value_input_option = "RAW"

    creds = gen_cred()

    # Clear existing data
    clear_sheet(creds, spreadsheet_id, range_name)

    # Paste new data
    paste_data(creds, spreadsheet_id, range_name, value_input_option, new_data)

# Function to update the second spreadsheet
def sheet_update2(new_data, sheet_name):
    spreadsheet_id_2 = "1-oIObLpWJ3qUDRx2-tDgYVM-OUJBdtbxnbEOVILTWfU"
    range_name = f"{sheet_name}!A1:Z999999"
    value_input_option = "RAW"

    creds = gen_cred()

    # Clear existing data
    clear_sheet(creds, spreadsheet_id_2, range_name)

    # Paste new data
    paste_data(creds, spreadsheet_id_2, range_name, value_input_option, new_data)



# Function to update the second spreadsheet
def sheet_update3(new_data, sheet_name):
    spreadsheet_id = "10OJCcwvgFA3VDJ4xmzEJ9a1jIWgOTNhnparpxLKvdGo"
    range_name = f"{sheet_name}!A1:Z999999"
    value_input_option = "RAW"

    creds = gen_cred()

    # Clear existing data
    clear_sheet(creds, spreadsheet_id, range_name)

    # Paste new data
    paste_data(creds, spreadsheet_id, range_name, value_input_option, new_data)



