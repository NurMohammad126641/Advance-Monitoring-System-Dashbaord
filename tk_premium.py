import psycopg2
from isheet_controller import sheet_update
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

queries = {
  "current_day_premium_purchase":
        """ SELECT
        to_char(created_at, 'YYYY-MM-DD') AS date,
        to_char(created_at, 'HH24') AS hour,
        amount,
        status,
        status || '(' || amount || 'tk package)' AS status_with_package,
        COUNT(status) AS count,
        amount * COUNT(status) AS total_amount
    FROM
        payment_purchasesubscription
    WHERE
        created_at::date = CURRENT_DATE
    GROUP BY
        1, 2, 3, 4, 5
    ORDER BY
        2;
  """,

  "monthly_premium_purchase":
    """ SELECT
    to_char(created_at, 'YYYY-MM-DD')
    AS
    date,
    amount,
        status,
        status || '(' || amount || 'tk package)' AS status_with_package,
        COUNT(status) AS count,
        amount * COUNT(status) AS total_amount
    FROM
    payment_purchasesubscription
    WHERE
    created_at >= NOW() - interval'1 month'
    GROUP
    BY
    1, 2, 3, 4
    ORDER
    BY
    1
    desc;
  """,

  "active_pm_packages":
    """
    select "name",subscription_type,regular_price,offer_price,is_active,updated_at from tallykhata_v2_live.public.payment_subscriptionplan
    where 1=1
    and is_active is true 
    order by id desc;
    """,

  "purchased_count_amount":
    """
    SELECT
        date,
        amount,
        status,
        transaction_count,
        amount * transaction_count AS total_amount
    FROM (
        SELECT
            to_char(created_at, 'YYYY-MM-DD') AS date,
            amount,
            status,
            COUNT(*) AS transaction_count
        FROM
            payment_purchasesubscription
        WHERE
            status = 'PURCHASED'
        GROUP BY
            to_char(created_at, 'YYYY-MM-DD'), amount, status
    ) subquery
    ORDER BY
        date DESC;
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

def tk_premium():
    db_params = {
        "user": os.getenv("TP_PG_USR_2"),
        "password": os.getenv("TP_PG_PWD_2"),
        "host": os.getenv("TP_HOST_2")
    }

    db_mapping = {
        "current_day_premium_purchase": "tallykhata_v2_live",
        "monthly_premium_purchase": "tallykhata_v2_live",
        "active_pm_packages": "tallykhata_v2_live",
        "purchased_count_amount": "tallykhata_v2_live"
    }

    sheet_mapping = {
        "current_day_premium_purchase": "current_day_premium_purchase_sheet",
        "monthly_premium_purchase": "monthly_premium_purchase_sheet",
        "active_pm_packages": "active_pm_packages_sheet",
        "purchased_count_amount": "purchased_count_amount_sheet"
    }

    headers_mapping = {
        "current_day_premium_purchase": ["date","hour","amount","status","status_with_package","count","total_amount"],
        "monthly_premium_purchase": ["date","amount","status","status_with_package","count","total_amount"],
        "purchased_count_amount": ["date","amount","status","transaction_count","total_amount"],
        "active_pm_packages": ["name","subscription_type","regular_price","offer_price","is_active","updated_at"]
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
    tk_premium()