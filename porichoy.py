import psycopg2
from isheet_controller import sheet_update2  # Ensure this module is correctly set up to handle Google Sheets updates
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

# Queries
queries = {

    "sqr_current_day_unique_failed": """
       SELECT
    id,
    create_date,
    update_date,
    request_id,
    -- Extract the XML part first
    (regexp_matches(response::json->>'xml', '<field id="32" value="([^"]+)"'))[1] AS field_id_32_value,
    (regexp_matches(response::json->>'xml', '<field id="39" value="([^"]+)"'))[1] AS field_id_39_value
FROM request_log rl
WHERE 1=1
  AND request ILIKE '%hex%'
  and create_date >= '2025-01-01'
  and create_date <= '2025-03-01'
ORDER BY id desc;
    """
}

# Function to fetch data from the database
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

# Function to handle each query and update the corresponding Google Sheet
def process_query(service_name, query, db_params, dbname, headers, sheet_name):
    result = fetch_data(query, db_params, dbname)
    data = [headers]

    if result:
        for row in result:
            converted_row = [
                x.strftime('%Y-%m-%d') if isinstance(x, datetime.date) else float(x) if isinstance(x, Decimal) else x
                for x in row
            ]
            if len(converted_row) == len(headers):
                data.append(converted_row)

        sheet_update2(data, sheet_name)
        print(f"Data for {service_name} updated successfully in sheet '{sheet_name}'.")
    else:
        print(f"No data returned for {service_name}")

# Main function to initialize parameters and process each query
def porichoy():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        # "sqr_monthly_unique_failed": "tallypay_issuer",
        "sqr_current_day_unique_failed": "tallypay_issuer"
    }

    sheet_mapping = {
        # "sqr_monthly_unique_failed": "sqr_monthly_unique_failed_sheet",
        "sqr_current_day_unique_failed": "sqr_current_day_unique_failed_sheet"
    }

    headers_mapping = {
        # "sqr_monthly_unique_failed": ["day", "status", "Receiver Wallet No", "wallet_count"],
        "sqr_current_day_unique_failed": ["id", "create_date", "update_date", "request_id","field_id_32_value","field_id_39_value"]
    }

    # Use threading to parallelize the process
    with ThreadPoolExecutor(max_workers=7) as executor:
        future_to_service = {
            executor.submit(
                process_query,
                service_name,
                query,
                db_params,
                db_mapping[service_name],
                headers_mapping[service_name],
                sheet_mapping[service_name]
            ): service_name
            for service_name, query in queries.items()
        }

        for future in as_completed(future_to_service):
            service_name = future_to_service[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing {service_name}: {e}")

if __name__ == "__main__":
    porichoy()
