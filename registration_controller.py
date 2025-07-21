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
    "profile_hour": """
        select
                        to_char(created_at + interval '6 hours', 'HH24') as hour,
                        case
                            when identity_status = 'VERIFIED' and is_pin_set = true then 'SUCCESSFULLY COMPLETED REGISTRATION'
                            else 'ATTEMPTED'
                        end as status,
                          COUNT(*) AS count
                    from
                        backend_db.public.profile p
                    where
                        created_at::date = current_date 
                        GROUP BY 1, 2
                        ORDER BY 1 DESC;
    """,

    # New queries
    "profile_monthly": """
                      SELECT 
        to_char(created_at + interval '6 hours', 'YYYY-MM-DD') AS date,
        CASE 
            when identity_status = 'VERIFIED' and is_pin_set = true then 'SUCCESSFULLY COMPLETED REGISTRATION'
                            else 'ATTEMPTED'
        END AS status,
        COUNT(*) AS count
    FROM backend_db.public.profile p
    WHERE created_at::date >= current_date - interval '30 days'
    GROUP BY 1, 2
    ORDER BY 1 DESC;
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
def tp_reg():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "profile_hour": "backend_db",
        "profile_monthly": "backend_db"
    }

    sheet_mapping = {
        "profile_hour": "tp_reg_profile_hr_sheet",
        "profile_monthly": "tp_reg_profile_mnthly_sheet"
    }

    headers_mapping = {
        "profile_hour": ["hour","status","count"],
        "profile_monthly": ["date","status","count"]
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
    tp_reg()
