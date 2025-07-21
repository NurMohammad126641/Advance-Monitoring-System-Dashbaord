import os
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from sheet_read import read_sheet_data
from isheet_controller import sheet_update2

# Load environment variables from the .env file
load_dotenv()

def fetch_data(query, db_params, dbname):
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host']
        )
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def extract_and_calculate(sheet_name):
    # Define the spreadsheet ID and range
    spreadsheet_id = "1fkSAe0FNvO01xV-giOdEU3RQFCNNNml4qJ7pKC8ySe8"
    range_name = f"{sheet_name}!A1:E999999"  # Assuming 'day' is in column B (adjust range as necessary)

    # Read data from the specified sheet tab
    data = read_sheet_data(spreadsheet_id, range_name)
    user_ids = []
    detailed_results = []

    print("Calculated Results:")
    for row in data[1:]:  # Skip the header row
        if len(row) > 3 and row[2] == "User profile not found":
            day = row[0]  # Extract 'day' from the second column
            hour = row[1]  # Extract 'hour' from the first column
            previous_merchant_id = row[3]

            # Extract the part from the 6th character, taking the next 9 characters
            extracted_part = previous_merchant_id[6:15]  # 6th to 14th digit (0-indexed)
            if extracted_part.isdigit():
                # Convert to an integer and subtract 124445555
                result = int(extracted_part) - 124445555
                user_ids.append(result)
                detailed_results.append((day, hour, previous_merchant_id, "User profile not found"))
                print(f"Day: {day}, Hour: {hour}, Receiver Wallet No: {previous_merchant_id}, Extracted: {extracted_part}, Result: {result}")
            else:
                print(f"Day: {day}, Hour: {hour}, Receiver Wallet No: {previous_merchant_id}, Extracted: {extracted_part}, Error: Invalid number extraction")

    if user_ids:
        # Prepare the SQL query
        query = f"""
        SELECT user_id,
               biz_name, 
               wallet_no, 
               merchant_id as current_merchant_id
        FROM profile 
        WHERE profile.user_id IN ({','.join(map(str, user_ids))}) 
              ORDER BY user_id
        """

        # Define database parameters and database name
        db_params = {
            "user": os.getenv("TP_PG_USR"),
            "password": os.getenv("TP_PG_PWD"),
            "host": os.getenv("TP_HOST")
        }
        dbname = "backend_db"

        # Fetch the data from the database
        query_result = fetch_data(query, db_params, dbname)
        if query_result:
            # Create a dictionary from the query result using user_id as the key
            result_dict = {row[0]: row[1:] for row in query_result}

            # Create a list to store the aligned results
            aligned_results = []
            for user_id in user_ids:
                # Find the corresponding row in the result_dict, or use placeholders if not found
                biz_name, wallet_no, current_merchant_id = result_dict.get(user_id, ("", "", ""))
                aligned_results.append((user_id, biz_name, wallet_no, current_merchant_id))

            # Convert aligned results to a DataFrame
            df_query_result = pd.DataFrame(aligned_results, columns=["user_id", "biz_name", "wallet_no", "current_merchant_id"])

            # Create a DataFrame for the extracted results, including the 'day' and 'hour' columns
            df_extracted = pd.DataFrame(detailed_results, columns=["day", "hour", "previous_merchant_id", "status"])

            # Combine both DataFrames by aligning rows (horizontal concatenation)
            df_final = pd.concat([df_extracted, df_query_result], axis=1)

            # Replace NaN values with empty strings
            df_final = df_final.fillna("")

            # Prepare data to send to Google Sheets
            headers_mapping = {
                "inactive_SQR_attempt_users_list": ["day", "hour", "previous_merchant_id", "status", "user_id", "biz_name", "wallet_no", "current_merchant_id"]
            }

            # Prepare the data with headers
            sheet_name = "inactive_SQR_attempt_users_list"
            headers = headers_mapping.get(sheet_name)
            data_to_update = [headers] + df_final.values.tolist()

            # Use the sheet_update function to send data to Google Sheets
            sheet_update2(data_to_update, sheet_name)
            print(f"Results written to Google Sheets tab '{sheet_name}'.")
        else:
            print("No results found or an error occurred while fetching data.")
    else:
        print("No valid user IDs found for the query.")

def run_sqr_user_extraction():
    """Wrapper function to execute the main logic."""
    extract_and_calculate("sqr_current_day_unique_failed_sheet")

run_sqr_user_extraction()
