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
    "robi_airtel_balance": """
        SELECT 
        rl_robi.request AS request_robi,
        rl_airtel.request AS request_airtel,
        SUBSTRING(
            rl_robi.response,
            POSITION('Your new balance is ' IN rl_robi.response) + LENGTH('Your new balance is '),
            POSITION(' TAKA' IN SUBSTRING(rl_robi.response, POSITION('Your new balance is ' IN rl_robi.response) + LENGTH('Your new balance is '))) - 1
        ) AS new_balance_robi,
        SUBSTRING(
            rl_airtel.response,
            POSITION('Your new balance is ' IN rl_airtel.response) + LENGTH('Your new balance is '),
            POSITION(' TAKA' IN SUBSTRING(rl_airtel.response, POSITION('Your new balance is ' IN rl_airtel.response) + LENGTH('Your new balance is '))) - 1
        ) AS new_balance_airtel
    FROM 
        request_log rl_robi
    LEFT JOIN 
        request_log rl_airtel ON rl_airtel.request_id = (
            SELECT request_id
            FROM top_up_info
            WHERE vendor_name = 'AIRTEL'
            AND status = 'SUCCESS'
            ORDER BY id DESC
            LIMIT 1
        )
    WHERE 
        rl_robi.request_id = (
            SELECT request_id
            FROM top_up_info
            WHERE vendor_name = 'ROBI'
            AND status = 'SUCCESS'
            ORDER BY id DESC
            LIMIT 1
        )
    AND rl_robi.request LIKE '%https://elapi.robi.com.bd/pretups/C2SReceiver?%'
    AND rl_airtel.request LIKE '%https://elapi.robi.com.bd/pretups/C2SReceiver?%';
    """,

    # New queries
    "nagad_balance": """
                   SELECT 
        r1.request,
        to_char(
            to_timestamp(
                substring(r1.request FROM 'requestDateTime=([0-9]+)')::text, 
                'YYYYMMDDHH24MISS'
            ), 
            'YYYY/MM/DD HH24:MI:SS'
        ) AS formatted_request_date_time,
        to_char((r1.response::json ->> 'merchantBalance')::numeric, 'FM999,999,999') AS merchant_balance_nagad
    FROM 
        request_log r1
    WHERE 
        1=1
        AND r1.request ILIKE '%https://api.mynagad.com/api/dfs/recharge%'
        AND r1.response ~ '^{.*}$'
    ORDER BY 
        r1.id DESC
    LIMIT 1;
    """,

    "paystation_balance": """
                  SELECT 
        request, 
        response,
        CASE 
            WHEN response IS NOT NULL AND response ~ '^{.*}$' THEN  -- Basic regex check for JSON format
                COALESCE(
                    NULLIF(
                        to_char((response::jsonb->>'new_balance')::numeric, 'FM999,999,999'), -- Format the number with commas
                        ''
                    ), 
                    'Invalid or non-JSON response'
                )
            ELSE 
                'Invalid or non-JSON response'
        END AS new_balance_paystation
    FROM 
        request_log rl 
    WHERE 
        1=1
        AND request_id = (
            SELECT request_id 
            FROM top_up_info a 
            WHERE 
                vendor_name = 'PAYSTATION' 
                AND status = 'SUCCESS' 
                AND number_of_try = '0'
            ORDER BY id DESC 
            LIMIT 1
        )
        AND request ILIKE '%http://api.shl.com.bd:8282/recharge %';
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
def all_balance():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "robi_airtel_balance": "topup_service",
        "nagad_balance": "tallypay_to_fi_integration",
        "paystation_balance": "topup_service"
    }

    sheet_mapping = {
        "robi_airtel_balance": "robi_airtel_balance_sheet",
        "nagad_balance": "nagad_balance_sheet",
        "paystation_balance": "paystation_balance_sheet"
    }

    headers_mapping = {
        "robi_airtel_balance": ["request_robi","request_airtel","new_balance_robi","new_balance_airtel"],
        "nagad_balance": ["request", "formatted_request_date_time", "merchant_balance_nagad"],
        "paystation_balance": ["request", "response", "new_balance_paystation"]
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
    all_balance()
