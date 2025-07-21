import psycopg2
from isheet_controller import sheet_update
from decimal import Decimal
from dotenv import load_dotenv
import os


# Load environment variables from .env file
load_dotenv()

# Define SQL queries with database names included
queries = {
    "Recharge": """
        SELECT 
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount
        FROM topup_service.public.top_up_info  -- Replace 'your_database_name' with actual database name
        WHERE status ='SUCCESS'
        AND create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
    """,

    "CBL": """
        select
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    from
        backend_db.public.bank_txn_request btr
    where
        1 = 1
        and btr.channel = 'CBL'
        and btr.status = 'SUCCESS'
        and (btr.issue_time + interval '6 hours')BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
    """,

    "BEFTN": """
    select
        COUNT(*) as transaction_count,
        SUM(amount) as total_amount
    from
        backend_db.public.bank_txn_request btr
    where
        1 = 1
        and btr.channel = 'BEFTN'
        and (btr.issue_time + INTERVAL '6 hours')::date BETWEEN (CURRENT_DATE - INTERVAL '3 days') AND (CURRENT_DATE - INTERVAL '1 day');
    """,

    "Nagad Money in": """
        SELECT 
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount
        FROM nobopay_payment_gw.public.nagad_txn  -- Replace 'another_database' with actual database name
        WHERE status ='SUCCESS'
        AND create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
    """,
    "Rocket money in": """
    SELECT 
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount
    FROM nobopay_payment_gw.public.dbbl_transaction 
    WHERE 1=1
        and status ='SUCCESS'
        and  create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
    """,

    "Nagad Money Out": """
            SELECT 
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount
            FROM tallypay_to_fi_integration.public.transaction_info ti
            WHERE 1=1
                AND financial_institute = 'NAGAD'
                AND status = 'SUCCESS'
                AND create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
        """,
    "Rocket Money Out": """
            SELECT 
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount
            FROM tallypay_to_fi_integration.public.transaction_info ti
            WHERE 1=1
                AND financial_institute = 'ROCKET'
                AND status = 'SUCCESS'
                AND create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
        """,

    "SQR Payment": """
            SELECT 
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount
            FROM tp_bank_service.public.npsb_transaction_info nti
            WHERE 1=1
                AND status = 'SUCCESS'
                AND create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
        """,
    "VISA Card Transfer": """
            SELECT 
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount
            FROM tp_bank_service.public.card_txn_log ctl
            WHERE 1=1
                AND status = 'SUCCESS'
                AND create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
        """,

    "NPSB": """
        SELECT 
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount
    FROM tp_bank_service.public.bank_transaction_info bti
    where 1=1
       and channel in ('NPSB')
       and status = 'SUCCESS'
        and create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
        """,

    "MTB": """
        SELECT 
                COUNT(*) AS transaction_count,
                SUM(amount) AS total_amount
    FROM tp_bank_service.public.bank_transaction_info bti
    where 1=1
       and channel in ('MTB')
       and status = 'SUCCESS'
       and create_date BETWEEN (CURRENT_DATE - INTERVAL '1 day') AND (CURRENT_DATE - INTERVAL '1 second');
    """
}

def fetch_data(query, db_params, dbname):
    try:
        # Connect using only the required parameters including dbname
        conn = psycopg2.connect(
            dbname=dbname,
            user=db_params['user'],
            password=db_params['password'],
            host=db_params['host']
            #port=db_params['port']
        )
        cur = conn.cursor()
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        #print(f"Error fetching data: {e}")
        return None

def main_daily_service_analysis():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
        #"port": os.getenv("DB_PORT")
    }

    # Mapping of service names to their corresponding database names
    db_mapping = {
        "Recharge": "topup_service",
        "CBL": "backend_db",
        "BEFTN": "backend_db",
        "Nagad Money in": "nobopay_payment_gw",
        "Rocket money in": "nobopay_payment_gw",
        "Nagad Money Out": "tallypay_to_fi_integration",
        "Rocket Money Out": "tallypay_to_fi_integration",
        "SQR Payment": "tp_bank_service",
        "VISA Card Transfer": "tp_bank_service",
        "NPSB": "tp_bank_service",
        "MTB": "tp_bank_service"
    }

    # Define headers to match the example
    headers = ["Service name", "Total Successful transactions", "Total Amount"]

    data = [headers]  # Start with headers

    for service_name, query in queries.items():
        dbname = db_mapping.get(service_name)
        result = fetch_data(query, db_params, dbname)
        if result:
            # Convert Decimal values to float
            transaction_count = int(result[0])
            total_amount = float(result[1]) if isinstance(result[1], Decimal) else result[1]
            data.append([service_name, transaction_count, total_amount, None])  # None for MO if not applicable

    # Add any necessary formatting to data before sending to Google Sheets
    sheet_update(data, "daily_transactions_count")  # Replace with your actual sheet name


# if __name__ == "__main__":
#      main_daily_service_analysis()