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
    "threshold_sqr": """
            WITH avg_thresholds AS (
        SELECT 
            hour,
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(rl.create_date, 'HH24') as hour,
                COUNT(*) as transaction_count
            FROM 
                tallypay_issuer.public.request_log rl
            WHERE 
                rl.create_date >= NOW() - interval '4 weeks'
                AND rl.request_id IS NOT NULL
                AND rl.request NOT ILIKE '%hex%'
                AND rl.response NOT ILIKE '%{%NPSB transfer credit%}%'
                AND rl.create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(rl.create_date, 'HH24'), to_char(rl.create_date, 'IW'), to_char(rl.create_date, 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(rl.create_date, 'HH24') as hour,
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            tallypay_issuer.public.request_log rl
        WHERE 
            rl.create_date::date = CURRENT_DATE
            AND rl.create_date <= NOW() - interval '5 minutes'
            AND rl.request_id IS NOT NULL
            AND rl.request NOT ILIKE '%hex%'
            AND rl.response NOT ILIKE '%{%NPSB transfer credit%}%'
        GROUP BY 
            hour
    )
    -- Combining the two datasets
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,

    "threshold_recharge": """
        WITH avg_thresholds AS (
        SELECT 
            hour,  -- No alias needed here yet
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(tui.create_date, 'HH24') as hour,  -- Alias 'tui' used here
                COUNT(*) as transaction_count
            FROM 
                topup_service.public.top_up_info tui  -- Alias 'tui' defined for the table
            WHERE 
                tui.create_date >= NOW() - interval '4 weeks'
                AND tui.status != 'SUCCESS'
                AND tui.create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(tui.create_date, 'HH24'), to_char(tui.create_date, 'IW'), to_char(tui.create_date, 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(tui.create_date, 'HH24') as hour,  -- Alias 'tui' used here
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            topup_service.public.top_up_info tui  -- Alias 'tui' defined here for the table
        WHERE 
            tui.create_date::date = CURRENT_DATE
            AND tui.status NOT IN ('SUCCESS', 'CHECKOUT')
            AND tui.create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            hour
    )
    -- Combining the two datasets
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,

    "threshold_nagad_AM_q": """
           WITH avg_thresholds AS (
        SELECT 
            hour,
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(create_date, 'HH24') as hour,
                COUNT(*) as transaction_count
            FROM 
                public.nagad_txn  -- replace 'public' with the correct schema if needed
            WHERE 
                create_date >= NOW() - interval '4 weeks'
                AND nagad_status = 'Failed'
                AND create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(create_date, 'HH24'), to_char(create_date, 'IW'), to_char(create_date, 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(create_date, 'HH24') as hour,
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            public.nagad_txn  -- replace 'public' with the correct schema if needed
        WHERE 
            create_date::date = CURRENT_DATE
            AND nagad_status = 'Failed'
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            hour
    )
    -- Combining the two datasets
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,

    "threshold_nagad_mo": """
                WITH avg_thresholds AS (
        SELECT 
            hour,
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(create_date, 'HH24') as hour,
                COUNT(*) as transaction_count
            FROM 
                public.transaction_info  -- replace 'public' with the correct schema if needed
            WHERE 
                create_date >= NOW() - interval '4 weeks'
                AND financial_institute = 'NAGAD'
                AND status NOT IN ('SUCCESS')
                AND create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(create_date, 'HH24'), to_char(create_date, 'IW'), to_char(create_date, 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(create_date, 'HH24') as hour,
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            public.transaction_info  -- replace 'public' with the correct schema if needed
        WHERE 
            create_date::date = CURRENT_DATE
            AND financial_institute = 'NAGAD'
            AND status NOT IN ('SUCCESS')
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            hour
    )
    -- Combine both average and current day data
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,

    "threshold_rocket_am": """
            WITH avg_thresholds AS (
        SELECT 
            hour,
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(create_date, 'HH24') as hour,
                COUNT(*) as transaction_count
            FROM 
                public.dbbl_transaction -- replace 'public' with the correct schema if needed
            WHERE 
                create_date >= NOW() - interval '4 weeks'
                AND status NOT IN ('SUCCESS')
                AND create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(create_date, 'HH24'), to_char(create_date, 'IW'), to_char(create_date, 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(create_date, 'HH24') as hour,
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            public.dbbl_transaction  -- replace 'public' with the correct schema if needed
        WHERE 
            create_date::date = CURRENT_DATE
            AND status NOT IN ('SUCCESS')
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            hour
    )
    -- Combining the two datasets
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,


    "threshold_rocket_mo": """
            WITH avg_thresholds AS (
        SELECT 
            hour,
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(create_date, 'HH24') as hour,
                COUNT(*) as transaction_count
            FROM 
                public.transaction_info  -- replace 'public' with the correct schema if needed
            WHERE 
                create_date >= NOW() - interval '4 weeks'
                AND financial_institute = 'ROCKET'
                AND status NOT IN ('SUCCESS')
                AND create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(create_date, 'HH24'), to_char(create_date, 'IW'), to_char(create_date, 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(create_date, 'HH24') as hour,
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            public.transaction_info  -- replace 'public' with the correct schema if needed
        WHERE 
            create_date::date = CURRENT_DATE
            AND financial_institute = 'ROCKET'
            AND status NOT IN ('SUCCESS')
            AND create_date <= NOW() - interval '5 minutes'
        GROUP BY 
            hour
    )
    -- Combine both average and current day data
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,

    "threshold_cbl": """
            WITH avg_thresholds AS (
        SELECT 
            hour,
            'last 4week not success avg' as txn,
            ROUND(AVG(transaction_count), 2) as txn_count
        FROM (
            SELECT
                to_char(issue_time + interval '6 hours', 'HH24') as hour,
                COUNT(*) as transaction_count
            FROM 
                backend_db.public.bank_txn_request btr
            WHERE 
                (issue_time + interval '6 hours') >= NOW() - interval '4 weeks'
                AND btr.channel = 'CBL'
                AND btr.status != 'SUCCESS'
                AND (issue_time + interval '6 hours') <= NOW() - interval '5 minutes'
            GROUP BY 
                to_char(issue_time + interval '6 hours', 'HH24'), to_char(issue_time + interval '6 hours', 'IW'), to_char(issue_time + interval '6 hours', 'YYYY-MM-DD')
        ) as weekly_data
        GROUP BY 
            hour
    ),
    current_day_data AS (
        SELECT 
            to_char(issue_time + interval '6 hours', 'HH24') as hour,
            'today not success' as txn,
            COUNT(*) as txn_count
        FROM 
            backend_db.public.bank_txn_request btr
        WHERE 
            (issue_time + interval '6 hours')::date = CURRENT_DATE
            AND btr.channel = 'CBL'
            AND btr.status != 'SUCCESS'
            AND (issue_time + interval '6 hours') <= NOW() - interval '5 minutes'
        GROUP BY 
            hour
    )
    -- Combine both average and current day data
    SELECT * FROM (
        SELECT * FROM avg_thresholds
        UNION ALL
        SELECT * FROM current_day_data
    ) as combined_data
    ORDER BY hour, txn;
    """,


    "beftn_threshold": """
        WITH avg_thresholds AS (
            SELECT 
                hour,
                'last 4week not success avg' as txn,
                ROUND(AVG(transaction_count), 2) as txn_count
            FROM (
                SELECT
                    to_char(issue_time + interval '6 hours', 'HH24') as hour,
                    COUNT(*) as transaction_count
                FROM 
                    backend_db.public.bank_txn_request btr
                WHERE 
                    (issue_time + interval '6 hours') >= NOW() - interval '4 weeks'
                    AND btr.channel = 'BEFTN'
                    AND btr.status NOT IN ('SUCCESS', 'REQUESTED')
                    AND (issue_time + interval '6 hours')::date <= CURRENT_DATE - interval '3 days'
                GROUP BY 
                    to_char(issue_time + interval '6 hours', 'HH24'), to_char(issue_time + interval '6 hours', 'IW'), to_char(issue_time + interval '6 hours', 'YYYY-MM-DD')
            ) as weekly_data
            GROUP BY 
                hour
        ),
        current_day_data AS (
            SELECT 
                to_char(issue_time + interval '6 hours', 'HH24') as hour,
                'today not success' as txn,
                COUNT(*) as txn_count
            FROM 
                backend_db.public.bank_txn_request btr
            WHERE 
                (issue_time + interval '6 hours')::date = CURRENT_DATE - interval '3 days'
                AND btr.channel = 'BEFTN'
                AND btr.status NOT IN ('SUCCESS', 'REQUESTED')
            GROUP BY 
                hour
        )
        SELECT * FROM (
            SELECT * FROM avg_thresholds
            UNION ALL
            SELECT * FROM current_day_data
        ) as combined_data
        ORDER BY hour, txn;
    """,

    "visa_threshold": """
        WITH avg_thresholds AS (
            SELECT 
                hour,
                'last 4week not success avg' as txn,
                ROUND(AVG(transaction_count), 2) as txn_count
            FROM (
                SELECT
                    to_char(create_date, 'HH24') as hour,
                    COUNT(*) as transaction_count
                FROM 
                    card_txn_log
                WHERE 
                    create_date >= NOW() - interval '4 weeks'
                    AND status NOT IN ('SUCCESS')
                    AND create_date <= NOW() - interval '5 minutes'
                GROUP BY 
                    to_char(create_date, 'HH24'), to_char(create_date, 'IW'), to_char(create_date, 'YYYY-MM-DD')
            ) as weekly_data
            GROUP BY 
                hour
        ),
        current_day_data AS (
            SELECT 
                to_char(create_date, 'HH24') as hour,
                'today not success' as txn,
                COUNT(*) as txn_count
            FROM 
                card_txn_log
            WHERE 
                create_date::date = CURRENT_DATE
                AND status NOT IN ('SUCCESS')
                AND create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                hour
        )
        SELECT * FROM (
            SELECT * FROM avg_thresholds
            UNION ALL
            SELECT * FROM current_day_data
        ) as combined_data
        ORDER BY hour, txn;
    """,

    "npsb_threshold": """
        WITH avg_thresholds AS (
            SELECT 
                hour,
                'last 4week not success avg' as txn,
                ROUND(AVG(transaction_count), 2) as txn_count
            FROM (
                SELECT
                    to_char(create_date, 'HH24') as hour,
                    COUNT(*) as transaction_count
                FROM 
                    bank_transaction_info
                WHERE 
                    create_date >= NOW() - interval '4 weeks'
                    AND channel IN ('NPSB', 'MTB')
                    AND status NOT IN ('SUCCESS')
                    AND create_date <= NOW() - interval '5 minutes'
                GROUP BY 
                    to_char(create_date, 'HH24'), to_char(create_date, 'IW'), to_char(create_date, 'YYYY-MM-DD')
            ) as weekly_data
            GROUP BY 
                hour
        ),
        current_day_data AS (
            SELECT 
                to_char(create_date, 'HH24') as hour,
                'today not success' as txn,
                COUNT(*) as txn_count
            FROM 
                bank_transaction_info
            WHERE 
                create_date::date = CURRENT_DATE
                AND channel IN ('NPSB', 'MTB')
                AND status NOT IN ('SUCCESS')
                AND create_date <= NOW() - interval '5 minutes'
            GROUP BY 
                hour
        )
        SELECT * FROM (
            SELECT * FROM avg_thresholds
            UNION ALL
            SELECT * FROM current_day_data
        ) as combined_data
        ORDER BY hour, txn;
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

def threshold_avg():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "threshold_sqr": "tallypay_issuer",
        "threshold_recharge": "topup_service",
        "threshold_nagad_AM_q": "nobopay_payment_gw",
        "threshold_nagad_mo": "tallypay_to_fi_integration",
        "threshold_rocket_am": "nobopay_payment_gw",
        "threshold_rocket_mo": "tallypay_to_fi_integration",
        "threshold_cbl": "backend_db",
        "beftn_threshold": "backend_db",
        "visa_threshold": "tp_bank_service",
        "npsb_threshold": "tp_bank_service"
    }

    sheet_mapping = {
        "threshold_sqr": "threshold_sqr_sheet",
        "threshold_recharge": "threshold_recharge_sheet",
        "threshold_nagad_AM_q": "threshold_nagad_AM_q_sheet",
        "threshold_nagad_mo": "threshold_nagad_mo_sheet",
        "threshold_rocket_am": "threshold_rocket_am_sheet",
        "threshold_rocket_mo": "threshold_rocket_mo_sheet",
        "threshold_cbl": "threshold_cbl_sheet",
        "beftn_threshold": "beftn_threshold_sheet",
        "visa_threshold": "visa_threshold_sheet",
        "npsb_threshold": "npsb_threshold_sheet"
    }

    headers_mapping = {
        "threshold_sqr": ["hour", "txn", "txn_count"],
        "threshold_recharge": ["hour", "txn", "txn_count"],
        "threshold_nagad_AM_q": ["hour", "txn", "txn_count"],
        "threshold_nagad_mo": ["hour", "txn", "txn_count"],
        "threshold_rocket_am": ["hour", "txn", "txn_count"],
        "threshold_rocket_mo": ["hour", "txn", "txn_count"],
        "threshold_cbl": ["hour", "txn", "txn_count"],
        "beftn_threshold": ["hour", "txn", "txn_count"],
        "visa_threshold": ["hour", "txn", "txn_count"],
        "npsb_threshold": ["hour", "txn", "txn_count"]
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

# if __name__ == "__main__":
#     threshold_avg()
