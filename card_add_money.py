import psycopg2
from isheet_controller import sheet_update3
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
    "card_add_money_today_txn": """
                select 
          to_char(create_date, 'HH24') as hour,
          status,
          SUM(amount) AS total_amount,
          COUNT(*) as transaction_count
    from public.payment_info
    where 
    create_date :: date = current_date
    group by 1, 2
    order by 1 desc;
    """,

    # New queries
    "card_add_money_monthly_txn": """
                   select 
      to_char(create_date, 'YYYY-MM-DD') as day,
      status,
      sum(amount) as total_amount,
      count(*)  as transaction_count
from public.payment_info
where 
create_date >= now() - interval '1 month' 
group by 
1,2
order by 1 desc;
    """,


    "card_wise_today_count": """
        select 
      to_char(create_date, 'HH24') as hour,
      card_type ,
      status,
      sum(amount) as total_amount,
      COUNT(*) as transaction_count
from public.payment_info
where 
create_date :: date = current_date 
and status = 'SUCCESS'
group by 
1,2,3
order by 1 desc;
    """,


    "card_wise_monthly_count": """
    select 
      to_char(create_date, 'YYYY-MM-DD') as day,
      status,
      card_type ,
      sum(amount) as total_amount,
      count(*) as transaction_count 
from public.payment_info
where 
create_date >= now() - interval '1 month'
and status = 'SUCCESS'
group by 
1,2,3
order by 1 desc;
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

        sheet_update3(data, sheet_name)
        print(f"Data for {service_name} updated successfully in sheet '{sheet_name}'.")
    else:
        print(f"No data returned for {service_name}")
def card_add_money():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "card_add_money_today_txn": "nobopay_payment_gw",
        "card_add_money_monthly_txn": "nobopay_payment_gw",
        "card_wise_today_count": "nobopay_payment_gw",
        "card_wise_monthly_count": "nobopay_payment_gw"
    }

    sheet_mapping = {
        "card_add_money_today_txn": "card_add_money_today_txn_sheet",
        "card_add_money_monthly_txn": "card_add_money_monthly_txn_sheet",
        "card_wise_today_count": "card_wise_today_count_sheet",
        "card_wise_monthly_count": "card_wise_monthly_count_sheet"
    }

    headers_mapping = {
        "card_add_money_today_txn": ["hour","status","total_amount","transaction_count"],
        "card_add_money_monthly_txn": ["day","status","total_amount","transaction_count"],
        "card_wise_today_count": ["hour", "card_type", "status", "total_amount", "transaction_count"],
        "card_wise_monthly_count": ["day", "card_type", "status", "total_amount", "transaction_count"]
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
        card_add_money()
