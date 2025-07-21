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
    # Existing queries
    "today_data_backup_count": """
            SELECT 
    TO_CHAR(created_at, 'HH24') AS hr,
    event_name,
    message,
    DATE(created_at) AS created_at,
    COUNT(*) AS total_backup_count
FROM 
    eventapp_event
WHERE 
    message = 'data-backup-done'
    AND DATE(created_at) = CURRENT_DATE
GROUP BY 
    hr, event_name, message, DATE(created_at)
ORDER BY 
    hr;
    """,

    # New queries
    "today_data_backup_error": """
       SELECT 
    TO_CHAR(created_at, 'HH24') AS hr,
    event_name,
    message,
    details ,
    DATE(created_at) AS created_at,
    COUNT(*) AS total_d2s_error_count
FROM 
    eventapp_event ee
WHERE 
   "level" = 'ERROR'
   and (ee.event_name = 'v6_device_to_server_sync' or ee.event_name = 'v5_device_to_server_sync' or ee.event_name = 'device_to_server_sync_v4')
    AND DATE(created_at) = CURRENT_DATE
GROUP BY 
    hr, event_name, message, details, DATE(created_at)
ORDER BY 
    hr;
    """,

    "today_s2d_count": """
                      SELECT 
    TO_CHAR(created_at, 'HH24') AS hr,
    CASE 
        WHEN message LIKE '%request%' THEN 'request received'
        WHEN message LIKE '%response%' THEN 'response generated'
    END AS message,
    COUNT(*) AS total_s2d_count
FROM eventapp_event
WHERE 
    event_name = 'sync_app_event'
    AND "level" = 'INFO'
    AND DATE(created_at) = CURRENT_DATE
GROUP BY 
    TO_CHAR(created_at, 'HH24'), 
    CASE 
        WHEN message LIKE '%request%' THEN 'request received'
        WHEN message LIKE '%response%' THEN 'response generated'
    END
ORDER BY 
    hr, message;
    """,

    "today_s2d_error": """  
 SELECT 
    TO_CHAR(created_at, 'HH24') AS hr,
    "level" ,
    event_name,
    message,
    DATE(created_at) AS created_at,
    COUNT(*) AS total_s2d_error_count
FROM 
    eventapp_event
WHERE 
    event_name = 'sync_app_event'
    and level= 'ERROR'
    AND DATE(created_at) = CURRENT_DATE
GROUP BY 
    hr, "level", event_name, message, DATE(created_at)
ORDER BY 
    hr;
 """,


    "tk_login_monitor": """
    SELECT 
    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS date_time,
    "level" ,
    event_name,
    message,
    details 
FROM 
    eventapp_event
WHERE 
    event_name = 'user-login-api-v2'
ORDER BY id desc
limit 50;
   """,


    "tk_login_error": """
    SELECT 
    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS date_time,
    "level" ,
    event_name,
    message,
    details 
FROM 
    eventapp_event
WHERE 
    event_name = 'user-login-api-v2'
    and "level" != 'INFO'
ORDER BY id desc
limit 50;
  """,


    "wallet_login_monitor": """
    SELECT 
    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS date_time,
    "level" ,
    event_name,
    message,
    details,
    user_id 
FROM 
    eventapp_event
WHERE 
    event_name = 'wallet-lookup-api-v2'
ORDER BY id desc
limit 50;
    """,



    "wallet_login_error": """
    SELECT 
    TO_CHAR(created_at, 'YYYY-MM-DD HH24:MI:SS') AS date_time,
    "level" ,
    event_name,
    message,
    details,
    user_id 
FROM 
    eventapp_event
WHERE 
    event_name = 'wallet-lookup-api-v2'
    and eventapp_event."level" != 'INFO'
ORDER BY id desc
limit 50;
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
def tallykhata_log():
    db_params = {
        "user": os.getenv("TP_PG_USR_2"),
        "password": os.getenv("TP_PG_PWD_2"),
        "host": os.getenv("TP_HOST_2")
    }

    db_mapping = {
        "today_data_backup_count": "tallykhata_log",
        "today_data_backup_error": "tallykhata_log",
        "today_s2d_count": "tallykhata_log",
        "today_s2d_error": "tallykhata_log",
        "tk_login_monitor": "tallykhata_log",
        "tk_login_error": "tallykhata_log",
        "wallet_login_monitor": "tallykhata_log",
        "wallet_login_error": "tallykhata_log"

    }

    sheet_mapping = {
        "today_data_backup_count": "today_data_backup_count_sheet",
        "today_data_backup_error": "today_data_backup_error_sheet",
        "today_s2d_count": "today_s2d_count_sheet",
        "today_s2d_error": "today_s2d_error_sheet",
        "tk_login_monitor": "tk_login_monitor_sheet",
        "tk_login_error": "tk_login_error_sheet",
        "wallet_login_monitor": "wallet_login_monitor_sheet",
        "wallet_login_error": "wallet_login_error_sheet"
    }

    headers_mapping = {
        "today_data_backup_count": ["hr","event_name","message","created_at","total_backup_count"],
        "today_data_backup_error": ["hr","event_name","message", "details","created_at","total_d2s_error_count"],
        "today_s2d_count": ["hr","message","total_s2d_count"],
        "today_s2d_error": ["hr","level","event_name", "message","created_at","total_s2d_error_count"],
        "tk_login_monitor": ["date_time","level","event_name", "message","details"],
        "tk_login_error": ["date_time","level","event_name", "message","details"],
        "wallet_login_monitor": ["date_time","level","event_name", "message","details","user_id"],
        "wallet_login_error": ["date_time","level","event_name", "message","details","user_id"]
    }

    # Use threading to parallelize the process
    with ThreadPoolExecutor(max_workers=5) as executor:
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
    tallykhata_log()
