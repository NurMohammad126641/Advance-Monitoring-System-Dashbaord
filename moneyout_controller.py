import os
import traceback
from dotenv import load_dotenv
import psycopg2
from isheet_controller import sheet_update
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# Define the queries
queries = {
    'hourly_cohort': '''
        SELECT
            to_char(create_date, 'HH24') AS hour,
            financial_institute,
            status,
            COUNT(status)::text,
            SUM(amount)::text
        FROM
            tallypay_to_fi_integration.public.transaction_info ti
        WHERE
            ti.create_date::date = CURRENT_DATE
            AND ti.financial_institute IN ('ROCKET', 'NAGAD')
        GROUP BY 1, 2, 3;
    ''',
    'day_cohort': '''
        SELECT
            to_char(create_date, 'YYYY-MM-DD') AS date,
            financial_institute,
            status,
            COUNT(status)::text,
            SUM(amount)::text
        FROM
            tallypay_to_fi_integration.public.transaction_info ti
        WHERE
            ti.create_date::date >= CURRENT_DATE - interval '30 days'
            AND ti.create_date <= NOW() - interval '5 minutes'
            AND ti.financial_institute IN ('ROCKET', 'NAGAD')
        GROUP BY 1, 2, 3;
    ''',

    'rckt_log_hr_cohort': '''
        select
                                to_char(ti.create_date, 'HH24') as hour,
                                (regexp_match(rl.response,
                                '<ResponseCode>(.*?)</ResponseCode>.*?<ResponseMessage>(.*?)</ResponseMessage>'))[1] || ' -> ' || (regexp_match(rl.response,
                                '<ResponseCode>(.*?)</ResponseCode>.*?<ResponseMessage>(.*?)</ResponseMessage>'))[2] as combined_result,
                                count(distinct(ti.request_id))
                            from
                                tallypay_to_fi_integration.public.transaction_info ti
                            inner join tallypay_to_fi_integration.public.request_log rl 
                            on
                                ti.request_id = rl.request_id
                            where
                                1 = 1
                                and ti.create_date::date = 'now()'
                                and ti.financial_institute = 'ROCKET'
                                and rl.request ilike '%https://dbblobftlive.dutchbanglabank.com:8003/rocketgw/api/v1/%'
                            group by
                                1,
                                2
                            order by
                                1 asc,
                                3 desc
                            ;
    ''',

    'rckt_log_day_cohort': '''
        select
                                    to_char(ti.create_date, 'YYYY-MM-DD') as date,
                                    (regexp_match(rl.response,
                                    '<ResponseCode>(.*?)</ResponseCode>.*?<ResponseMessage>(.*?)</ResponseMessage>'))[1] || ' -> ' || (regexp_match(rl.response,
                                    '<ResponseCode>(.*?)</ResponseCode>.*?<ResponseMessage>(.*?)</ResponseMessage>'))[2] as combined_result,
                                    count(distinct(ti.request_id))
                                from
                                    tallypay_to_fi_integration.public.transaction_info ti
                                inner join tallypay_to_fi_integration.public.request_log rl 
                                on
                                    ti.request_id = rl.request_id
                                where
                                    1 = 1
                                    and ti.create_date::date >= now() - interval '30 days'
                                    and ti.create_date <= now() - interval '5 minutes'
                                    and ti.financial_institute = 'ROCKET'
                                    and rl.request ilike '%https://dbblobftlive.dutchbanglabank.com:8003/rocketgw/api/v1/%'
                                group by
                                    1,
                                    2
                                order by
                                    1 asc,
                                    3 desc
                                ;
    ''',

    'nagad_notsuccess_hr_cohort': '''
        select
                                        to_char(ti.create_date, 'HH24') as request_hr,
                                        rl.response, 
                                        count(distinct(ti.request_id)) as count
                                    from
                                        tallypay_to_fi_integration.public.transaction_info ti
                                    inner join tallypay_to_fi_integration.public.request_log rl
                                    on
                                        ti.request_id = rl.request_id
                                    where
                                        1 = 1
                                        and ti.create_date::date = 'now()'
                                        and ti.financial_institute = 'NAGAD'
                                        and ti.status <> 'SUCCESS'
                                        and rl.request ilike '%https://api.mynagad.com/api%'
                                        and rl.response not ilike '%"sensitiveData"%'
                                    group by
                                        1,
                                        2
                                    order by
                                        1 desc,
                                        3 desc
                                    ;
    ''',

    'nagad_notsuccess_day_cohort': '''
        select
                                        to_char(ti.create_date, 'YYYY-MM-DD') as request_day,
                                        rl.response, 
                                        count(distinct(ti.request_id)) as count
                                    from
                                        tallypay_to_fi_integration.public.transaction_info ti
                                    inner join tallypay_to_fi_integration.public.request_log rl
                                    on
                                        ti.request_id = rl.request_id
                                    where
                                        1 = 1
                                        and ti.create_date::date >= now() - interval '30 days'
                                        and ti.financial_institute = 'NAGAD'
                                        and ti.status <> 'SUCCESS'
                                        and rl.request ilike '%https://api.mynagad.com/api%'
                                        and rl.response not ilike '%"sensitiveData"%'
                                    group by
                                        1,
                                        2
                                    order by
                                        1 desc,
                                        3 desc
                                    ;
    '''

}


# Function to execute a query and return the results
def fetch_data(query, db_params):
    try:
        conn = psycopg2.connect(
            database=db_params["database"],
            user=db_params["user"],
            password=db_params["password"],
            host=db_params["host"],
            port=db_params["port"]
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


# Function to process each query and update the Google Sheet
def process_cohort(query_name, query, db_params, headers, sheet_name):
    result = fetch_data(query, db_params)
    data = [headers]
    if result:
        for row in result:
            data.append(row)
        sheet_update(data, sheet_name)
        print(f"Data for {query_name} updated successfully in sheet '{sheet_name}'.")
    else:
        print(f"No data returned for {query_name}")


# Main function to run all cohort queries in parallel
def moneyout_main():
    db_params = {
        "database": "tallypay_to_fi_integration",
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST"),
        "port": '5432'
    }

    headers_mapping = {
        'hourly_cohort': ["hour", "medium", "status", "count", "amt"],
        'day_cohort': ["date", "medium", "status", "count", "amt"],
        'rckt_log_hr_cohort': ["hour", "rocket response", "count"],
        'rckt_log_day_cohort': ["date", "rocket response", "count"],
        'nagad_notsuccess_hr_cohort': ["hour", "response", "count"],
        'nagad_notsuccess_day_cohort': ["date", "response", "count"]

    }

    sheet_mapping = {
        'hourly_cohort': "moneyout_hourly_cohort",
        'day_cohort': "moneyout_curm_cohort",
        'rckt_log_hr_cohort': "rckt_log_hr_cohort",
        'rckt_log_day_cohort': "rckt_log_day_cohort",
        'nagad_notsuccess_hr_cohort': "nagad_notsuccess_hr_cohort",
        'nagad_notsuccess_day_cohort': "nagad_notsuccess_day_cohort"

    }

    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_query = {
            executor.submit(
                process_cohort,
                query_name,
                query,
                db_params,
                headers_mapping[query_name],
                sheet_mapping[query_name]
            ): query_name
            for query_name, query in queries.items()
        }

        for future in as_completed(future_to_query):
            query_name = future_to_query[future]
            try:
                future.result()
            except Exception as e:
                print(f"Error processing {query_name}: {e}")


# if __name__ == "__main__":
#     moneyout_main()
