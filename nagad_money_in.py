import psycopg2
from isheet_controller import sheet_update
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

# Queries for Nagad transactions
queries = {
    "nagad_in_current_day_txn_count": """
        SELECT 
            to_char(nt.create_date, 'HH24') AS hour,
            create_date,
            amount,
            nagad_status,
            nagad_status_code,
            CASE 
                WHEN nagad_status = 'Ready' THEN 'READY'
                WHEN nagad_status = 'Success' THEN 'SUCCESS'
                WHEN nagad_status = 'Failed' THEN 'FAILED'
                WHEN nagad_status = 'Aborted' THEN 'ABORTED'
                WHEN nagad_status = 'Cancelled' THEN 'CANCELLED'
                WHEN nagad_status = 'Refunded' THEN 'REFUNDED'
                WHEN nagad_status = 'TransactionInProgress' THEN 'IN_PROGRESS'
                ELSE status
            END AS status,
            tp_transaction_number,
            wallet
        FROM 
            nobopay_payment_gw.public.nagad_txn nt
        WHERE 
            create_date::date = CURRENT_DATE
        ORDER BY 
            id DESC;
    """,

    "nagad_in_last_month_txn": """
    SELECT 
    TO_CHAR(nt.create_date, 'YYYY-MM-DD') AS day, -- Extract the day from create_date
    ROUND(SUM(amount), 2) AS total_amount,       -- Sum the amounts for the day
    nagad_status,                                -- Include nagad_status
    nagad_status_code,                           -- Include nagad_status_code
    CASE 
        WHEN nagad_status = 'Ready' THEN 'READY'
        WHEN nagad_status = 'Success' THEN 'SUCCESS'
        WHEN nagad_status = 'Failed' THEN 'FAILED'
        WHEN nagad_status = 'Aborted' THEN 'ABORTED'
        WHEN nagad_status = 'Cancelled' THEN 'CANCELLED'
        WHEN nagad_status = 'Refunded' THEN 'REFUNDED'
        WHEN nagad_status = 'TransactionInProgress' THEN 'IN_PROGRESS'
        ELSE status
    END AS status,                               -- Categorize statuses
    COUNT(*) AS txn_count                        -- Count transactions
FROM 
    nobopay_payment_gw.public.nagad_txn nt
WHERE 
    create_date >= NOW() - INTERVAL '1 month'    -- Filter for the last month
GROUP BY 
    TO_CHAR(nt.create_date, 'YYYY-MM-DD'), 
    nagad_status, 
    nagad_status_code,
    CASE 
        WHEN nagad_status = 'Ready' THEN 'READY'
        WHEN nagad_status = 'Success' THEN 'SUCCESS'
        WHEN nagad_status = 'Failed' THEN 'FAILED'
        WHEN nagad_status = 'Aborted' THEN 'ABORTED'
        WHEN nagad_status = 'Cancelled' THEN 'CANCELLED'
        WHEN nagad_status = 'Refunded' THEN 'REFUNDED'
        WHEN nagad_status = 'TransactionInProgress' THEN 'IN_PROGRESS'
        ELSE status
    END
ORDER BY 
    day DESC, 
    txn_count DESC;
    """,
    "nagad_in_failed_current_day_txn": """
        SELECT 
            to_char(nt.create_date, 'HH24') AS hour,
            create_date,
            amount,
            nagad_status,
            nagad_status_code,
            CASE 
                WHEN nagad_status = 'Ready' THEN 'READY'
                WHEN nagad_status = 'Success' THEN 'SUCCESS'
                WHEN nagad_status = 'Failed' THEN 'FAILED'
                WHEN nagad_status = 'Aborted' THEN 'ABORTED'
                WHEN nagad_status = 'Cancelled' THEN 'CANCELLED'
                WHEN nagad_status = 'Refunded' THEN 'REFUNDED'
                WHEN nagad_status = 'TransactionInProgress' THEN 'IN_PROGRESS'
                ELSE status
            END AS status,
            tp_transaction_number,
            wallet
        FROM 
            nobopay_payment_gw.public.nagad_txn nt
        WHERE 
            create_date::date = CURRENT_DATE
            AND nagad_status = 'Failed'
        ORDER BY 
            id DESC;
    """,


    "nagad_in_failed_monthly_txn": """
         SELECT 
    to_char(nt.create_date, 'YYYY-MM-DD') AS day,
    nagad_status,
    nagad_status_code,
    CASE 
        WHEN nagad_status = 'Ready' THEN 'READY'
        WHEN nagad_status = 'Success' THEN 'SUCCESS'
        WHEN nagad_status = 'Failed' THEN 'FAILED'
        WHEN nagad_status = 'Aborted' THEN 'ABORTED'
        WHEN nagad_status = 'Cancelled' THEN 'CANCELLED'
        WHEN nagad_status = 'Refunded' THEN 'REFUNDED'
        WHEN nagad_status = 'TransactionInProgress' THEN 'IN_PROGRESS'
        ELSE nagad_status
    END AS status,
    COUNT(*) AS status_code_count,
    SUM(amount) AS total_amount
FROM 
    nobopay_payment_gw.public.nagad_txn nt
WHERE 
    create_date >= NOW() - INTERVAL '1 month'
    AND nagad_status = 'Failed'
GROUP BY 
    day, nagad_status, nagad_status_code
ORDER BY 
    day DESC, status_code_count DESC;
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

def nagad_money_in():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "nagad_in_current_day_txn_count": "nobopay_payment_gw",
        "nagad_in_last_month_txn": "nobopay_payment_gw",
        "nagad_in_failed_current_day_txn": "nobopay_payment_gw",
        "nagad_in_failed_monthly_txn": "nobopay_payment_gw"
    }

    sheet_mapping = {
        "nagad_in_current_day_txn_count": "nagad_in_current_day_txn_count_sheet",
        "nagad_in_last_month_txn": "nagad_in_monthly_txn_sheet",
        "nagad_in_failed_current_day_txn": "nagad_in_failed_current_day_txn_sheet",
        "nagad_in_failed_monthly_txn": "nagad_in_failed_monthly_txn_sheet"
    }

    headers_mapping = {
        "nagad_in_current_day_txn_count": ["hour", "create_date", "amount", "nagad_status", "nagad_status_code", "status", "tp_transaction_number", "wallet"],
        "nagad_in_last_month_txn": ["day", "amount", "nagad_status", "nagad_status_code", "status", "txn_count"],
        "nagad_in_failed_current_day_txn": ["hour", "create_date", "amount", "nagad_status", "nagad_status_code", "status", "tp_transaction_number", "wallet"],
        "nagad_in_failed_monthly_txn": ["day", "nagad_status", "nagad_status_code", "status", "status_code_count", "total_amount"]
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
    nagad_money_in()
