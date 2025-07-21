import os
from dotenv import load_dotenv

import psycopg2
import traceback
from isheet_controller import sheet_update

load_dotenv()

def gen_q(q):

    nagadin_hr = '''select
                        to_char(create_date, 'HH24'),
                        case 
                            WHEN status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED', 'ORDER_ERR', 'INITIATED', 'CHECKOUT') THEN 'DISPUTE'
                            else status 
                        end as status,
                        coalesce(nagad_status,'NULL'), 
                        count(status)::text,
                    --  round(count(status)* 100 / sum(count(status)) over (partition by to_char(create_date, 'HH24')), 2)::text as "Percentage",
                        sum(amount)::text
                    from
                        nobopay_payment_gw.public.nagad_txn nt
                    where
                        nt.create_date::date = 'now()'
                        and nt.create_date <= now() - interval '5 minutes'
                        and nt.status not in ('ORDER_ERR', 'INITIATED', 'CHECKOUT')
                    group by
                        1,
                        2,
                        3
                    ;'''

    nagadin_m = '''select
                    to_char(create_date,
                    'YYYY-MM-DD'),
                    case 
                        when status not in ('SUCCESS', 'FAILED', 'REFUNDED') then 'DISPUTE'
                        else status 
                    end as status,
                    coalesce(nagad_status,
                    'NULL'),
                    count(status)::text,
                    --  round(count(status)* 100 / sum(count(status)) over (partition by to_char(create_date, 'YYYY-MM-DD')), 2)::text as "Percentage",
                    sum(amount)::text
                from
                    nobopay_payment_gw.public.nagad_txn nt
                where
                    1 = 1
                    and nt.create_date::date >= now() - interval '30 days'
                    and nt.create_date <= now() - interval '5 minutes'
                    and nt.status not in ('ORDER_ERR', 'INITIATED', 'CHECKOUT')
                group by
                    1,
                    2,
                    3
                ;'''

    query = {
        "nagadin_hr": nagadin_hr,
        "nagadin_m": nagadin_m
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

        #print('Connection established')
        cur = conn.cursor()
        #print('Executing query')
        cur.execute(query)
        #print('Retreiving queryset')
        queryset = cur.fetchall()
    except psycopg2.Error as error:
        queryset = f"Error connecting to Postgres database: {error}"
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    # #print(queryset)
    return queryset


def nagadin_main():

    #print('Initiating nagad cash-in controller')

    q = gen_q("nagadin_hr")
    q_r = pg_conn(q)
    nagadin_hr = [('Hour', 'status', 'NGD_Status', 'Count', 'Amt')] + q_r
    sheet_update(nagadin_hr, 'nagadin_curday_hour_cohort')

    q = gen_q('nagadin_m')
    q_r = pg_conn(q)
    nagadin_m = [('Day', 'status', 'NGD_Status', 'Count', 'Amt')] + q_r
    sheet_update(nagadin_m, 'nagadin_curmonth_cohort')

    #print('Nagad cash-in controller terminating...')


# try:
#     nagadin_main()
# except Exception as e:
#     error_msg = traceback.format_exc()
#     #print('Encountered error: ', error_msg)