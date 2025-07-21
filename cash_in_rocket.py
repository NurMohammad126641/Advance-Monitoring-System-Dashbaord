import os
from dotenv import load_dotenv

import psycopg2
from isheet_controller import sheet_update
import traceback

load_dotenv()


def gen_q(q):
    
    day_sum = '''select
                    to_char(create_date, 'HH24'),
                    case 
                        when status not in ('SUCCESS', 'FAILED', 'REFUNDED') then 'DISPUTE'
                        else status 
                    end as status,
                    coalesce(result, 'NULL'), 
                    count(status)::text as "Count",
                --  round(count(status)* 100 / sum(count(status)) over (partition by to_char(create_date, 'HH24')), 2)::text as "percentage",
                    sum(amount)::text
                from
                    nobopay_payment_gw.public.dbbl_transaction dt
                where
                    create_date::date = 'now()'
                    and create_date <= now() - interval '5 minutes'
                group by
                    1,
                    2,
                    3
                order by
                    1 asc,
                    4 desc
                ;'''

    month_sum = '''select
                        to_char(create_date, 'YYYY-MM-DD'),
                        case 
                            when status not in ('SUCCESS', 'FAILED', 'REFUNDED') then 'DISPUTE'
                            else status 
                        end as status,
                        coalesce(result, 'NULL'),
                        count(status)::text as "Count",
                    --  round(count(status)* 100 / sum(count(status)) over (partition by to_char(create_date, 'YYYY-MM-DD')), 2)::text as "percentage",
                        sum(amount)::text
                    from
                        nobopay_payment_gw.public.dbbl_transaction dt
                    where
                        1 = 1
                        and create_date::date >= now() - interval '30 days'
                        and create_date <= now() - interval '20 minutes'
                    group by
                        1,
                        2,
                        3
                    order by
                        1 asc
                ;'''

    query = {
        "day_sum": day_sum,
        "month_sum": month_sum
        }
    
    return query[q]


def pg_conn(query):
    conn = None
    cur = None

    try:
        # establishing the connection
        conn = psycopg2.connect(
            # ---Please change the credentials---
            database='nobopay_payment_gw',
            user=os.environ.get("TP_PG_USR"),
            password=os.environ.get("TP_PG_PWD"),
            host=os.environ.get("TP_HOST"),
            port='5432'
        )

        # ##print('Connection established')
        cur = conn.cursor()
        # ##print('Executing query')
        cur.execute(query)
        # ##print('Retreiving queryset')
        queryset = cur.fetchall()
    except psycopg2.Error as error:
        queryset = f"Error connecting to Postgres database: {error}"
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    # ##print(queryset)
    return queryset


def cashin_rocket_main():

    # ##print('Initiating rocket cash-in controller')

    q = gen_q("day_sum")
    dt_r = pg_conn(q)
    # ##print(dt_r)
    curday_hourly = [('Hour', 'status', 'RCKT_status', 'Count', 'Amt')] + dt_r

    q = gen_q("month_sum")
    dt_r = pg_conn(q)
    monthly_sum = [('Day', 'status', 'RCKT_status', 'Count', 'Amt')] + dt_r

    sheet_update(curday_hourly, 'rocketin_curday_hour_cohort')
    sheet_update(monthly_sum, 'rocketin_curm_day_cohort')


# try:
#     cashin_rocket_main()
# except Exception as e:
#     error_msg = traceback.format_exc()
#     ##print('Encountered below error: ', error_msg)