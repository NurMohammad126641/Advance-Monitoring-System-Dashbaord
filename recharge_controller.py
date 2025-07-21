import os
from dotenv import load_dotenv

import psycopg2


import traceback

from isheet_controller import sheet_update

load_dotenv()


def gen_q(q):
    
    stat_q = f'''select
                    to_char(create_date, 'HH24') as hour,
                    status,
                    mobile_operator,
                    COUNT(status)::text as status_count,
                 -- ROUND(COUNT(status) * 100.0 / SUM(COUNT(status)) over (partition by to_char(create_date, 'HH24')), 2)::text as percentage,
                    sum(amount)::text
                from
                    topup_service.public.top_up_info tui
                where
                    create_date::date = 'now()'
                    and create_date <= NOW() - interval '5 minutes'
                group by
                    1,
                    2,
                    3
                order by
                    1
                ;'''

    
    desc_q = '''select
                    "day/hour",
                    description,
                    count::text
                --  ROUND((count * 100.0) / total_count, 2)::text as percentage
                from
                    (
                    select
                        to_char(create_date, 'HH24') as "day/hour",
                        case
                            when description is null then 'NULL'
                            when description like 'invalid transfer value%' then 'Invalid Transfer Value'
                            when description like 'The mobile number%cannot use the same recharge service within 2.00 minutes of last successful transaction as last transfer amount is same as current requested amount.' then 'duplicate 2 minutes'
                            when description like '%3006202:The mobile number%cannot use the any recharge service within 3.00 minutes of last successful transaction as last transfer amount is same as current requested amount.%' then 'duplicate 3 minutes'
                            when description like '%Your current request to transfer %TAKA cannot be processed as you do not have enough credit.%' then 'Your current request to transfer {amount} TAKA cannot be processed as you do not have enough credit'
                            when description like 'Your request to recharge % is already in queue. Please wait for sometime.' then 'Your request to recharge {rcvr_num} is already in queue. Please wait for sometime.' 
                            else description
                        end as description,
                        COUNT(1) as count,
                        row_number() over (partition by to_char(create_date, 'HH24')
                    order by
                        COUNT(1) desc) as row_num,
                        SUM(COUNT(1)) over (partition by to_char(create_date, 'HH24')) as total_count
                    from
                        topup_service.public.top_up_info t
                    where
                        create_date::date = 'now()'
                        and create_date <= now() - interval '5 minutes'
                        and t.status != 'SUCCESS'
                    group by
                        1,
                        2
                ) ranked_data
                where
                    row_num <= 5
                order by
                    1 asc,
                    3 desc;'''

    sum_q = f'''select
                    to_char(create_date, 'YYYY-MM-DD'),
                    status,
                    COUNT(status)::text as status_count,
                    SUM(amount)::text as status_amount
                from
                    topup_service.public.top_up_info tui
                where
                    1=1 
                    and create_date::date >= now() - interval '30 days'
                    and create_date <= now() - interval '5 minutes'
                group by
                    1,
                    2
                order by
                    1 asc;'''

    hr_duration_cohort = '''select
                                to_char(create_date, 'HH24') as hour,
                                case
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 60 then '1 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 120 then '2 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 300 then '5 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 600 then '10 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 3600 then '1 hr or less'
                                        else 'greater then 1 hour'
                                    end as duration,
                                count(1)
                            from
                                top_up_info tui
                            where
                                1 = 1
                                and create_date::date = 'now()'
                                and create_date <= now() - interval '5 minutes'
                            group by
                                1,
                                2
                            ;'''

    month_duration_cohort = '''select
                                    to_char(create_date,
                                    'YYYY-MM-DD') as date,
                                    case
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 60 then '1 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 120 then '2 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 300 then '5 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 600 then '10 min or less'
                                        when extract(epoch
                                    from
                                        (update_date - create_date)) <= 3600 then '1 hr or less'
                                        else 'greater then 1 hour'
                                    end as duration,
                                    count(1)
                                from
                                    top_up_info tui
                                where
                                    1 = 1
                                    and create_date::date >= now() - interval '30 days'
                                    and create_date <= now() - interval '5 minutes'
                                group by
                                    1,
                                    2
                                ;'''

    desc_m_q = '''select
                    "day",
                    description,
                    count::text
                    --  ROUND((count * 100.0) / total_count, 2)::text as percentage
                    from
                    (
                    select
                        to_char(create_date, 'YYYY-MM-DD') as "day",
                        case
                            when description is null then 'NULL'
                            when description like 'invalid transfer value%' then 'Invalid Transfer Value'
                            when description like 'The mobile number%cannot use the same recharge service within 2.00 minutes of last successful transaction as last transfer amount is same as current requested amount.' then 'duplicate 2 minutes'
                            when description like '%3006202:The mobile number%cannot use the any recharge service within 3.00 minutes of last successful transaction as last transfer amount is same as current requested amount.%' then 'duplicate 3 minutes'
                            when description like '%Your current request to transfer % TAKA cannot be processed as you do not have enough credit.%' then 'Your current request to transfer {amount} TAKA cannot be processed as you do not have enough credit'
                            else description
                        end as description,
                        COUNT(t.txn_id) as count,
                        row_number() over (partition by to_char(create_date,
                        'YYYY-MM-DD')
                    order by
                        COUNT(t.txn_id) desc) as row_num,
                        SUM(COUNT(t.txn_id)) over (partition by to_char(create_date,
                        'YYYY-MM-DD')) as total_count
                    from
                        topup_service.public.top_up_info t
                    where
                        create_date::date >= now() - interval '30 days'
                        and create_date <= now() - interval '5 minutes'
                        and t.status != 'SUCCESS'
                    group by
                        1,
                        2
                                ) ranked_data
                    where
                    row_num <= 5
                    order by
                    1 asc,
                    3 desc;'''
    
    query = {
        "stat_q": stat_q,
        "desc_q": desc_q,
        "sum_q": sum_q,
        "hr_duration_cohort": hr_duration_cohort,
        "month_duration_cohort": month_duration_cohort,
        "desc_m_q": desc_m_q
        }
    
    return query[q]


def pg_conn(query):
    conn = None
    cur = None

    try:
        # establishing the connection
        conn = psycopg2.connect(
            # ---Please change the credentials---
            database='topup_service',
            user=os.environ.get("TP_PG_USR"),
            password=os.environ.get("TP_PG_PWD"),
            host=os.environ.get("TP_HOST"),
            port='5432'
        )

        ##print('Connection established')
        cur = conn.cursor()
        ##print('Executing query')
        cur.execute(query)
        ##print('Retreiving queryset')
        queryset = cur.fetchall()
    except psycopg2.Error as error:
        queryset = f"Error connecting to Postgres database: {error}"
    except Exception as query_error:
        queryset = f"Error executing query: {query_error}"
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    # ##print(queryset)
    return queryset


def recharge_main():
    
    ##print('Recharge program initiated...')

    q = gen_q("stat_q")
    tui_r = pg_conn(q)
    ##print(tui_r)
    stat_r = [('hour', 'status', 'operator', 'status_count', 'amount')] + tui_r
    sheet_update(stat_r, 'recharge_curday_hour_cohort')

    q = gen_q("desc_q")
    tui_r = pg_conn(q)
    desc_r = [('hour', 'description', 'count')] + tui_r
    sheet_update(desc_r, 'recharge_description')

    q = gen_q("sum_q")
    tui_r = pg_conn(q)
    sum_r = [('day','status', 'status_count', 'amount')] + tui_r
    sheet_update(sum_r, 'recharge_month_summary')

    q = gen_q("hr_duration_cohort")
    tui_r = pg_conn(q)
    tui_r = [('hour', 'duration', 'status_count')] + tui_r
    sheet_update(tui_r, 'recharge_duration_curday_hour_cohort')

    q = gen_q("month_duration_cohort")
    tui_r = pg_conn(q)
    tui_r = [('day', 'duration', 'status_count')] + tui_r
    sheet_update(tui_r, 'recharge_duration_curm_cohort')

    q = gen_q("desc_m_q")
    tui_r = pg_conn(q)
    tui_r = [('day', 'description', 'status_count')] + tui_r
    sheet_update(tui_r, 'desc_month_cohort')

    ##print('Recharge Execution complete')


# try:
#     recharge_main()
# except Exception as e:
#     error_message = traceback.format_exc()
#     ##print(f"An error occurred: {str(e)}")
