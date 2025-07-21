import psycopg2
from isheet_controller import sheet_update
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

# Queries
queries = {
    # SQR form opened today
    "sqr_form_opened_today": """
        SELECT DISTINCT(id),
               TO_CHAR(created_at, 'HH24') AS hour,
               'SQR_FORM_OPENED' AS event_name,
               tallykhata_user_id,
               COUNT(*) OVER (PARTITION BY id, tallykhata_user_id) AS transaction_count
        FROM sync_appevent
        WHERE event_name = 'event_sqr_rtlr_form_open'
        AND created_at::date = CURRENT_DATE
        ORDER BY id DESC;
    """,

    # SQR approved today count
    "sqr_approved_today_count": """
        SELECT DISTINCT(user_id),
               to_char(create_date, 'HH24') AS hour,
               new_value AS new_user_type,
               old_value AS old_user_type,
               updated_by 
        FROM audit_log
        WHERE old_value = 'CUSTOMER'
        AND new_value IN ('MERCHANT', 'MICRO_MERCHANT')
        AND create_date::date = CURRENT_DATE;
    """,

    # SQR approved monthly count
    "sqr_approved_monthly_count": """
        SELECT DISTINCT(user_id),
               to_char(create_date, 'YYYY-MM-DD') AS day,
               new_value AS new_user_type,
               old_value AS old_user_type,
               updated_by 
        FROM audit_log
        WHERE old_value = 'CUSTOMER'
        AND new_value IN ('MERCHANT', 'MICRO_MERCHANT')
        AND create_date >= NOW() - interval '1 month';
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

        sheet_update(data, sheet_name)
        print(f"Data for {service_name} updated successfully in sheet '{sheet_name}'.")
    else:
        print(f"No data returned for {service_name}")

def sqr_reg():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_params_2 = {
        "user": os.getenv("TP_PG_USR_2"),
        "password": os.getenv("TP_PG_PWD_2"),
        "host": os.getenv("TP_HOST_2")
    }

    db_mapping = {
        "sqr_form_opened_today": "tallykhata_v2_live",
        "sqr_approved_today_count": "backend_db",
        "sqr_approved_monthly_count": "backend_db"
    }

    sheet_mapping = {
        "sqr_form_opened_today": "sqr_form_opened_today_sheet",
        "sqr_approved_today_count": "sqr_approved_today_count_sheet",
        "sqr_approved_monthly_count": "sqr_approved_monthly_count_sheet"
    }

    headers_mapping = {
        "sqr_form_opened_today": ["id", "hour", "event_name", "tallykhata_user_id", "transaction_count"],
        "sqr_approved_today_count": ["user_id", "hour", "new_user_type", "old_user_type", "updated_by"],
        "sqr_approved_monthly_count": ["user_id", "day", "new_user_type", "old_user_type", "updated_by"]
    }

    # Use threading to parallelize the process
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_service = {
            executor.submit(
                process_query,
                service_name,
                query,
                db_params_2 if service_name == "sqr_form_opened_today" else db_params,
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

# if __name__ == "__main__":
#     sqr_reg()
