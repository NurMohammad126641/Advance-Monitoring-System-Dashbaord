import psycopg2
from isheet_controller import sheet_update
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

# Queries for Number2
queries = {
    "recharge_current_day_txn_count": """
            SELECT 
        to_char(create_date, 'HH24') AS hour,
        CONCAT(status, '  (', mobile_operator, ')') AS status_operator,
        COUNT(*) AS transaction_count
    FROM 
        topup_service.public.top_up_info tui
    WHERE 
        create_date::date = CURRENT_DATE
        AND create_date <= NOW() - interval '5 minutes'
    GROUP BY 
        1, 2
    ORDER BY 
        1;
    """,

    "recharge_monthly_txn_count": """
        SELECT 
            to_char(create_date, 'YYYY-MM-DD') as day,
            CONCAT(status, '  (', mobile_operator, ')') AS status_operator,
    COUNT(*) AS transaction_count
        FROM 
            topup_service.public.top_up_info tui
        WHERE 
            create_date >= NOW() - interval '1 month'
        GROUP BY 
            1, 2
        ORDER BY 
            1;
    """,

    "recharge_current_day_txn_amount": """
            select
        to_char(create_date,
        'HH24') as hour,
        CONCAT(status, '  (', mobile_operator, ')') AS status_operator,
    COUNT(*) AS transaction_count,
        SUM(amount) as total_amount
    from
        topup_service.public.top_up_info tui
    where 1=1
        and create_date::date = 'now()'
        and create_date <= NOW() - interval '5 minutes'
    group by
        1,
        2
    order by
        1;
    """,

    "recharge_monthly_txn_amount": """
        SELECT 
            to_char(create_date, 'YYYY-MM-DD') as day,
            CONCAT(status, '  (', mobile_operator, ')') AS status_operator,
            COUNT(*) AS transaction_count,
            SUM(amount) as total_amount
        FROM 
            topup_service.public.top_up_info tui
        WHERE 
            create_date >= NOW() - interval '1 month'
            AND status = 'SUCCESS'
        GROUP BY 
            1, 2
        ORDER BY 
            1;
    """,

    "recharge_current_day_failed_reversed_txn": """
        SELECT 
            to_char(create_date, 'HH24') as hour,
            status,
            mobile_operator,
            COUNT(*) as transaction_count
        FROM 
            topup_service.public.top_up_info tui
        WHERE 
            create_date::date = CURRENT_DATE
            AND status NOT IN ('SUCCESS')
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            1, 2, 3
        ORDER BY 
            1;
    """,

    "recharge_current_day_failed_reversed_amount": """
        SELECT 
            to_char(create_date, 'HH24') as hour,
            status,
            mobile_operator,
            SUM(amount) as total_amount
        FROM 
            topup_service.public.top_up_info tui
        WHERE 
            create_date::date = CURRENT_DATE
            AND status NOT IN ('SUCCESS')
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            1, 2, 3
        ORDER BY 
            1;
    """,

    "recharge_current_day_not_success_breakdown": """
        SELECT 
            to_char(create_date, 'HH24') as hour,
            status,
            mobile_operator,
            description ,
            COUNT(*) as transaction_count
        FROM 
            topup_service.public.top_up_info tui
        WHERE 
            create_date::date = CURRENT_DATE
            AND status NOT IN ('SUCCESS')
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            1, 2, 3, 4
        ORDER BY 
            1;
    """,

    "recharge_monthly_not_success_breakdown": """
        SELECT 
            to_char(create_date, 'YYYY-MM-DD') as day,
            status,
            mobile_operator,
            description ,
            COUNT(*) as transaction_count
        FROM 
            topup_service.public.top_up_info tui
        WHERE 
            create_date >= NOW() - interval '1 month'
            AND status NOT IN ('SUCCESS')
        GROUP BY 
            1, 2, 3, 4
        ORDER BY 
            1;
    """,

    "vendor": """
              WITH LatestVendors AS (
        SELECT mobile_operator, vendor_name, 
               ROW_NUMBER() OVER (PARTITION BY mobile_operator ORDER BY id DESC) AS rn
        FROM top_up_info tui
        WHERE mobile_operator IN ('GP', 'BL', 'AIRTEL', 'ROBI', 'TT')
    )
    select
        MAX(CASE WHEN mobile_operator = 'AIRTEL' THEN vendor_name END) AS Airtel,
        MAX(CASE WHEN mobile_operator = 'GP' THEN vendor_name END) AS GrameenPhone,
        MAX(CASE WHEN mobile_operator = 'BL' THEN vendor_name END) AS Banglalink,
        MAX(CASE WHEN mobile_operator = 'TT' THEN vendor_name END) AS TeleTalk,
        MAX(CASE WHEN mobile_operator = 'ROBI' THEN vendor_name END) AS Robi
    FROM LatestVendors
    WHERE rn = 1;
    """,

    "date_time": """
    SELECT TO_CHAR(NOW(), 'YYYY-MM-DD HH24:MI') AS date_time;
    """,

    "recharge_current_day_vendor_wise_duration": """
        SELECT
            to_char(create_date, 'HH24') AS hour,
            CASE
                WHEN extract(epoch FROM (update_date - create_date)) <= 60 THEN '1 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 120 THEN '2 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 300 THEN '5 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 600 THEN '10 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 3600 THEN '1 hr or less'
                ELSE 'greater than 1 hour'
            END AS duration,
            CONCAT(vendor_name, ' - (', 
                CASE
                    WHEN extract(epoch FROM (update_date - create_date)) <= 60 THEN '1 min or less'
                    WHEN extract(epoch FROM (update_date - create_date)) <= 120 THEN '2 min or less'
                    WHEN extract(epoch FROM (update_date - create_date)) <= 300 THEN '5 min or less'
                    WHEN extract(epoch FROM (update_date - create_date)) <= 600 THEN '10 min or less'
                    WHEN extract(epoch FROM (update_date - create_date)) <= 3600 THEN '1 hr or less'
                    ELSE 'greater than 1 hour'
                END, ')') AS vendor_wise_duration,
            COUNT(1) AS transaction_count
        FROM
            topup_service.public.top_up_info tui
        WHERE
            create_date::date = CURRENT_DATE
            AND create_date <= NOW() - INTERVAL '5 minutes'
        GROUP BY
            1, 2, 3
        ORDER BY
            1;
    """,

    "recharge_current_day_telco_wise_duration": """
        SELECT
        to_char(create_date, 'HH24') AS hour,
        CASE
            WHEN extract(epoch FROM (update_date - create_date)) <= 60 THEN '1 min or less'
            WHEN extract(epoch FROM (update_date - create_date)) <= 120 THEN '2 min or less'
            WHEN extract(epoch FROM (update_date - create_date)) <= 300 THEN '5 min or less'
            WHEN extract(epoch FROM (update_date - create_date)) <= 600 THEN '10 min or less'
            WHEN extract(epoch FROM (update_date - create_date)) <= 3600 THEN '1 hr or less'
            ELSE 'greater than 1 hour'
        END AS duration,
        CONCAT(mobile_operator, ' - (', 
            CASE
                WHEN extract(epoch FROM (update_date - create_date)) <= 60 THEN '1 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 120 THEN '2 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 300 THEN '5 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 600 THEN '10 min or less'
                WHEN extract(epoch FROM (update_date - create_date)) <= 3600 THEN '1 hr or less'
                ELSE 'greater than 1 hour'
            END, ')') AS telco_wise_duration,
        COUNT(1)
    FROM
        top_up_info tui
    WHERE
        create_date::date = CURRENT_DATE
        AND create_date <= NOW() - INTERVAL '5 minutes'
    GROUP BY
        1, 2, 3
       ORDER BY
                1;
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

def recharge_new():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "recharge_current_day_txn_count": "topup_service",
        "recharge_monthly_txn_count": "topup_service",
        "recharge_current_day_txn_amount": "topup_service",
        "recharge_monthly_txn_amount": "topup_service",
        "recharge_current_day_failed_reversed_txn": "topup_service",
        "recharge_current_day_failed_reversed_amount": "topup_service",
        "recharge_current_day_not_success_breakdown": "topup_service",
        "recharge_monthly_not_success_breakdown": "topup_service",
        "vendor": "topup_service",
        "date_time": "topup_service",
        "recharge_current_day_vendor_wise_duration": "topup_service",
        "recharge_current_day_telco_wise_duration": "topup_service"
    }

    sheet_mapping = {
        "recharge_current_day_txn_count": "recharge_current_day_txn_count_sheet",
        "recharge_monthly_txn_count": "recharge_monthly_txn_count_sheet",
        "recharge_current_day_txn_amount": "recharge_current_day_txn_amount_sheet",
        "recharge_monthly_txn_amount": "recharge_monthly_txn_amount_sheet",
        "recharge_current_day_failed_reversed_txn": "recharge_current_day_failed_reversed_txn_sheet",
        "recharge_current_day_failed_reversed_amount": "recharge_current_day_failed_reversed_amount_sheet",
        "recharge_current_day_not_success_breakdown": "recharge_current_day_not_success_sheet",
        "recharge_monthly_not_success_breakdown": "recharge_monthly_not_success_sheet",
        "vendor": "vendor_sheet",
        "date_time": "date_time_sheet",
        "recharge_current_day_vendor_wise_duration": "recharge_current_day_vendor_wise_duration_sheet",
        "recharge_current_day_telco_wise_duration": "recharge_current_day_telco_wise_duration_sheet"
    }

    headers_mapping = {
        "recharge_current_day_txn_count": ["hour", "status_operator", "transaction_count"],
        "recharge_monthly_txn_count": ["day", "status_operator", "transaction_count"],
        "recharge_current_day_txn_amount": ["hour","status_operator", "transaction_count", "total_amount"],
        "recharge_monthly_txn_amount": ["day", "status_operator", "transaction_count", "total_amount"],
        "recharge_current_day_failed_reversed_txn": ["hour", "status", "mobile_operator", "transaction_count"],
        "recharge_current_day_failed_reversed_amount": ["hour", "status", "mobile_operator", "total_amount"],
        "recharge_current_day_not_success_breakdown": ["hour", "status", "mobile_operator", "description", "transaction_count"],
        "recharge_monthly_not_success_breakdown": ["day", "status", "mobile_operator", "description", "transaction_count"],
        "vendor": ["Airtel","GrameenPhone","BanglaLink","TeleTalk","Robi"],
        "date_time": ["Date_Time"],
        "recharge_current_day_vendor_wise_duration": ["hour", "duration", "vendor_wise_duration", "transaction_count"],
        "recharge_current_day_telco_wise_duration": ["hour", "duration", "telco_wise_duration", "transaction_count"]
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
    recharge_new()