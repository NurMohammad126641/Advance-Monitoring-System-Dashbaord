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
    # New queries
    "not_success_reasons_monthly": """
        SELECT DISTINCT ON (bti.request_id)
        to_char(bti.create_date, 'YYYY-MM-DD') as day, bti.status, bti.amount,
        bti.bank_swift_code, bti.channel,
        COALESCE(rl.response::text, 'response: NULL') AS response
        FROM tp_bank_service.public.bank_transaction_info bti
        INNER JOIN tp_bank_service.public.request_log rl
        ON bti.request_id = rl.request_id
        WHERE bti.status in ('REVERSE', 'UNKNOWN', 'REVERSIBLE')
        AND rl.request ILIKE '%https://mfsdev.mutualtrustbank.com%'
        AND bti.create_date >= CURRENT_DATE - INTERVAL '30 days'
        ORDER BY bti.request_id DESC;
    """,

    "not_success_today": """
        SELECT DISTINCT ON (bti.request_id)
        to_char(bti.create_date, 'HH24') as hour, bti.status, bti.amount,
        bti.bank_swift_code, bti.channel,
        COALESCE(rl.response::text, 'response: NULL') AS response
        FROM tp_bank_service.public.bank_transaction_info bti
        INNER JOIN tp_bank_service.public.request_log rl
        ON bti.request_id = rl.request_id
        WHERE bti.status in ('REVERSE', 'UNKNOWN', 'REVERSIBLE')
        AND rl.request ILIKE '%https://mfsdev.mutualtrustbank.com%'
        AND bti.create_date::date = CURRENT_DATE
        ORDER BY bti.request_id DESC;
    """,

    "current_day_npsb": """
        SELECT to_char(create_date, 'HH24') as hour, status, amount,
        bank_swift_code, COUNT(*) as transaction_count
        FROM bank_transaction_info
        WHERE channel in ('NPSB', 'MTB')
        AND create_date::date = CURRENT_DATE
        AND create_date <= NOW() - interval '5 minutes'
        GROUP BY hour, status, amount, bank_swift_code
        ORDER BY hour;
    """,

    "monthly_count": """
     SELECT 
        to_char(create_date, 'YYYY-MM-DD') AS day, 
        status, 
        SUM(amount) AS total_amount, 
        bank_swift_code, 
        COUNT(*) AS transaction_count
    FROM bank_transaction_info
    WHERE channel IN ('NPSB', 'MTB') 
        AND create_date >= NOW() - INTERVAL '1 month'
    GROUP BY day, status, bank_swift_code
    ORDER BY day, bank_swift_code, status;
    """,


    "RC_85_TIMEOUT": """
         SELECT 
    to_char(create_date, 'YYYY-MM-DD') AS day,
    to_char(create_date, 'HH24') as hour,
    request_id as RRN,
    'RC_85(TIMEOUT)' as response 
    FROM tallypay_issuer.public.request_log
    WHERE request_id IS NOT NULL
      AND request IS NOT NULL
      AND response IS NULL
      AND create_date > '2025-04-01';
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
def NPSB():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "not_success_reasons_monthly": "tp_bank_service",
        "not_success_today": "tp_bank_service",
        "current_day_npsb": "tp_bank_service",
        "monthly_count": "tp_bank_service",
        "RC_85_TIMEOUT": "tallypay_issuer"
    }

    sheet_mapping = {
        "not_success_reasons_monthly": "npsb_not_success_reasons_monthly_sheet",
        "not_success_today": "npsb_not_success_today_sheet",
        "current_day_npsb": "current_day_status_npsb_sheet",
        "monthly_count": "npsb_monthly_status_sheet",
        "RC_85_TIMEOUT": "RC_85"
    }

    headers_mapping = {
        "not_success_reasons_monthly": ["day", "status", "amount", "bank_swift_code", "channel", "response"],
        "not_success_today": ["hour", "status", "amount", "bank_swift_code", "channel", "response"],
        "current_day_npsb": ["hour", "status", "amount", "bank_swift_code", "transaction_count"],
        "monthly_count": ["day", "status", "total_amount", "bank_swift_code", "transaction_count"],
        "RC_85_TIMEOUT": ["day", "hour", "RRN", "response"]
    }

    # Use threading to parallelize the process
    with ThreadPoolExecutor(max_workers=10) as executor:
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
    NPSB()
