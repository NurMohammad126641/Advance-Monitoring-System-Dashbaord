import psycopg2
from isheet_controller import sheet_update, sheet_update2
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os

# Load environment variables from .env file
load_dotenv()

# Queries for Number2
queries = {
    "recharge_cashback_current_day": """
    SELECT 
        to_char(update_date::timestamp, 'HH24') as hour,
        txn_status, 
        COUNT(*) as transaction_count
    FROM 
        cashback_txn_record
    WHERE 
        update_date::date = CURRENT_DATE
        AND update_date::timestamp <= NOW() - interval '5 minutes'
    GROUP BY 
        1, 2
    ORDER BY 
        1;
    """,

    "recharge_cashback_monthly": """
    SELECT 
        to_char(update_date::timestamp, 'YYYY-MM-DD') as day,
        txn_status,
        COUNT(*) as transaction_count
    FROM 
        cashback_txn_record
    WHERE 
        create_date >= NOW() - interval '1 month'
    GROUP BY 
        1, 2
    ORDER BY 
        1;
    """,
    "recharge_cashback_failed_list": """
    SELECT 
        to_char(update_date::timestamp, 'YYYY-MM-DD') as hour,
        txn_status,
        wallet,
        cashback_txn_id,
        COUNT(*) as transaction_count
    FROM 
        cashback_txn_record
    WHERE 
        create_date >= NOW() - interval '1 month'
        and txn_status = 'FAILED'
    GROUP BY 
        1, 2, 3, 4
    ORDER BY 
        1; 
     """
}

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
        #print(f"Error fetching data: {e}")
        return None

def recharge_cashbackk():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "recharge_cashback_current_day": "nobopay_offer_backend",
        "recharge_cashback_monthly": "nobopay_offer_backend",
        "recharge_cashback_failed_list": "nobopay_offer_backend"
    }

    sheet_mapping = {
        "recharge_cashback_current_day": "recharge_cashback_current_day_sheet",
        "recharge_cashback_monthly": "recharge_cashback_monthly_sheet",
        "recharge_cashback_failed_list": "recharge_cashback_failed_list_sheet"
    }

    headers_mapping = {
        "recharge_cashback_current_day": ["hour", "txn_status", "transaction_count"],
        "recharge_cashback_monthly": ["day", "txn_status", "transaction_count"],
        "recharge_cashback_failed_list": ["day", "txn_status", "wallet", "cashback_txn_id", "transaction_count"]
    }

    for service_name, query in queries.items():
        dbname = db_mapping.get(service_name)
        result = fetch_data(query, db_params, dbname)

        headers = headers_mapping.get(service_name)
        data = [headers]

        if result:
            for row in result:
                converted_row = [x.strftime('%Y-%m-%d') if isinstance(x, datetime.date) else float(x) if isinstance(x, Decimal) else x for x in row]

                if len(converted_row) == len(headers):
                    data.append(converted_row)
                else:
                    pass
                    #print(f"Unexpected row length: {len(converted_row)}. Skipping row.")

            # Determine the correct sheet update function to use
            sheet_name = sheet_mapping.get(service_name)
            if service_name == "recharge_cashback_failed_list":
                sheet_update2(data, sheet_name)
            else:
                sheet_update(data, sheet_name)
        else:
            pass
            #print(f"No data returned for {service_name}")

# if __name__ == "__main__":
#     recharge_cashbackk()
