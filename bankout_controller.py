import psycopg2
from isheet_controller import sheet_update
from decimal import Decimal
from dotenv import load_dotenv
import datetime
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env file
load_dotenv()

queries={
    "CBL_hr_stat": """select
                        to_char(issue_time + interval '6 hours', 'HH24'),
                        status,
                        coalesce(SUBSTRING(btl.response_text from '.*"responseMessage":\s*"([^"]+)"'),'No data in bank_txn_log') as response_msg,
                        count(btr.id)::text,
                        sum(amount)::text
                    from
                        backend_db.public.bank_txn_request btr
                    left join backend_db.public.bank_txn_log btl 
                    on btr.id =  btl.np_txn_request_id 
                    where
                        1 = 1
                        and (btr.issue_time + interval '6 hours')::date >= current_date 
                        and btr.txn_request_type = 'CASH_OUT'
                        and btr.bank_swift_code = 'CIBLBDDH'
                    group by
                        1,
                        2,
                        3
                    order by
                        1 asc,
                        4 desc;
                    """,

    "CBL_day_stat": """select
                            to_char(issue_time, 'YYYY-MM-DD'),
                            status,
                            coalesce(SUBSTRING(btl.response_text from '.*"responseMessage":\s*"([^"]+)"'),'No data in bank_txn_log') as response_msg,
                            count(btr.id)::text,
                            sum(amount)::text
                        from
                            backend_db.public.bank_txn_request btr
                        left join backend_db.public.bank_txn_log btl 
                        on btr.id =  btl.np_txn_request_id 
                        where
                            1 = 1
                            and (btr.issue_time + interval '6 hours')::date >= current_date - interval '30 days'
                            and btr.txn_request_type = 'CASH_OUT'
                            and btr.bank_swift_code = 'CIBLBDDH'
                        group by
                            1,
                            2,
                            3
                        order by
                            1 asc,
                            4 desc
                        ;""",

    "EFT_hr_stat" : """select
                        to_char(issue_time + interval '6 hours', 'HH24'),
                        status,
                        coalesce(SUBSTRING(btl.response_text from '.*"responseMessage":\s*"([^"]+)"'), 'No data in bank_txn_log') as response_msg,
                        b.bank_name,
                        count(btr.id)::text,
                        sum(amount)::text
                    from
                        backend_db.public.bank_txn_request btr
                        inner join backend_db.public.bank b 
                        on btr.bank_swift_code = b.swift_code
                    left join backend_db.public.bank_txn_log btl 
                                        on
                        btr.id = btl.np_txn_request_id
                    where
                        1 = 1
                        and (btr.issue_time + interval '6 hours')::date = current_date - interval '3 days'
                        and btr.txn_request_type = 'CASH_OUT'
                        and btr.bank_swift_code <> 'CIBLBDDH'
                        and btr.channel = 'BEFTN'
                    group by
                        1,
                        2,
                        3,
                        4
                    order by
                        1 asc,
                        5 desc
                    ;""",

    "EFT_day_stat": """select
                        to_char(issue_time + interval '6 hours', 'YYYY-MM-DD'),
                        status,
                        coalesce(SUBSTRING(btl.response_text from '.*"responseMessage":\s*"([^"]+)"'), 'No data in bank_txn_log') as response_msg,
                        b.bank_name,
                        count(btr.id)::text,
                        sum(amount)::text
                    from
                        backend_db.public.bank_txn_request btr
                        inner join backend_db.public.bank b 
                        on btr.bank_swift_code = b.swift_code
                    left join backend_db.public.bank_txn_log btl 
                                        on
                        btr.id = btl.np_txn_request_id
                    where
                        1 = 1
                        and (btr.issue_time + interval '6 hours')::date >= current_date - interval '30 days'
                        and (btr.issue_time + interval '6 hours')::date <= current_date - interval '3 days'
                        and btr.txn_request_type = 'CASH_OUT'
                        and btr.bank_swift_code <> 'CIBLBDDH'
                        and btr.channel = 'BEFTN'
                    group by
                        1,
                        2,
                        3,
                        4
                    order by
                        1 asc,
                        5 desc
                    ;""",


    "beftn_3_day": """
             select
                            to_char(issue_time + interval '6 hours',  'YYYY-MM-DD') as day,
                            to_char(issue_time + interval '6 hours', 'HH24') as hour,
                            status,
                            count(btr.id),
                            sum(amount)
                        from
                            backend_db.public.bank_txn_request btr
                        where
                            1 = 1
                            and (btr.issue_time + interval '6 hours')::date = current_date - interval '3 days'
                            and btr.txn_request_type = 'CASH_OUT'
                            and btr.channel = 'BEFTN'
                        group by
                            1,
                            2,
                            3
                        order by
                            2 asc;""",


    "beftn_monthly_status": """
              select
                        to_char(issue_time + interval '6 hours',  'YYYY-MM-DD') as date,
                        status,
                        count(btr.id),
                        sum(amount)
                    from
                        backend_db.public.bank_txn_request btr
                    where
                        1 = 1
                       and (btr.issue_time + interval '6 hours')::date >= current_date - interval '30 days'
                        and (btr.issue_time + interval '6 hours')::date <= current_date - interval '3 days'
                        and btr.txn_request_type = 'CASH_OUT'
                        and btr.channel = 'BEFTN'
                    group by
                        1,
                        2
                    order by
                        1 asc;"""
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
def bank_money_out():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "CBL_hr_stat": "backend_db",
        "CBL_day_stat": "backend_db",
        "EFT_hr_stat": "backend_db",
        "EFT_day_stat": "backend_db",
        "beftn_3_day": "backend_db",
        "beftn_monthly_status": "backend_db"
    }

    sheet_mapping = {
        "CBL_hr_stat": "CBL_hr_stat",
        "CBL_day_stat": "CBL_day_stat",
        "EFT_hr_stat": "EFT_hr_stat",
        "EFT_day_stat": "EFT_day_stat",
        "beftn_3_day": "beftn_3_day_sheet",
        "beftn_monthly_status": "beftn_monthly_status_sheet"
    }

    headers_mapping = {
        "CBL_hr_stat": ['hour', 'status', 'log_response', 'count', 'amt'],
        "CBL_day_stat": ['date', 'status', 'log_response', 'count', 'amt'],
        "EFT_hr_stat": ['date', 'status', 'log_response', 'bank_name', 'count', 'amt'],
        "EFT_day_stat": ['date', 'status', 'log_response', 'bank_name', 'count', 'amt'],
        "beftn_3_day": ['date', 'hour', 'status', 'count', 'sum'],
        "beftn_monthly_status": ['date','status','count', 'sum']
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
    bank_money_out()