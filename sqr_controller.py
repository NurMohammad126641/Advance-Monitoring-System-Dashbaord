import os
from dotenv import load_dotenv
import psycopg2
from isheet_controller import sheet_update
from threading import Thread

load_dotenv()

def gen_q(q):
    hr_log = '''SELECT
                    to_char(create_date, 'HH24') AS "Hour",
                    CASE
                        when response ilike '%{%NPSB transfer credit%}%' then 'SUCCESS'
                        WHEN response ~ '.*"message":\s*"([^"]+)"' THEN substring(response from '.*"message":\s*"([^"]+)"')
                        WHEN response ~ '.*"error":\s*"([^"]+)"' THEN substring(response from '.*"error":\s*"([^"]+)"')
                        ELSE response
                    END AS status,
                    count(request_id)
                FROM
                    tallypay_issuer.public.request_log rl
                WHERE
                    create_date::date = 'now()'
                    AND create_date <= current_timestamp - interval '5 minutes'
                    AND request_id IS NOT NULL
                    and request not ilike '%hex%'
                GROUP BY
                    1,
                    2
                ORDER BY
                    1 ASC,
                    3 desc
                ;'''

    hr_cohort = '''select
                        to_char(nti.create_date,
                        'HH24'),
                        nfc.acquirer, 
                        nti.status,
                        count(nti.status)::text,
                        sum(nti.amount)::text
                    from
                        tp_bank_service.public.npsb_transaction_info nti
                    inner join tp_bank_service.public.npsb_fi_config nfc 
                        on
                        nti.acquirer_id = nfc.acquirer_id 
                    where
                        1 = 1
                        and nti.create_date::date = 'now()'
                        and nti.create_date <= now() - interval '5 minutes'
                    group by
                        1,
                        2,
                        3
                    ;'''

    hr_err_user_count = '''select
                                to_char(create_date,
                                'HH24') as "Hour",
                                case
                                    when response ~ '.*"message":\s*"([^"]+)"' then substring(response
                                from
                                    '.*"message":\s*"([^"]+)"')
                                    when response ~ '.*"error":\s*"([^"]+)"' then substring(response
                                from
                                    '.*"error":\s*"([^"]+)"')
                                    else response
                                end as status,
                                count(distinct(SUBSTRING(request from '.*"receiver_wallet_no":\s*"([^"]+)"')))::text
                            from
                                tallypay_issuer.public.request_log rl
                            where
                                rl.create_date::date = 'now()'
                                and rl.create_date <= now() - interval '5 minutes'
                                and request_id is not null
                                and request not ilike '%hex%'
                                and response not ilike '%{%NPSB transfer credit%}%'
                            group by
                                1,
                                2
                            order by
                                1 desc,
                                3 desc
                            ;'''

    m_log = '''select
                    to_char(create_date, 'YYYY-MM-DD') as "Hour",
                    case
                        when response ilike '%{%NPSB transfer credit%}%' then 'SUCCESS'
                        when response ~ '.*"message":\s*"([^"]+)"' then substring(response from '.*"message":\s*"([^"]+)"') when response ~ '.*"error":\s*"([^"]+)"' then substring(response  from '.*"error":\s*"([^"]+)"')
                        else response
                    end as status,
                    count(request_id)
                from
                    tallypay_issuer.public.request_log rl
                where
                    create_date::date >= now() - interval '30 days' 
                    and create_date <= current_timestamp - interval '5 minutes'
                    and request_id is not null
                    and request not ilike '%hex%'
                group by
                    1,
                    2
                order by
                    1 asc,
                    3 desc
                ;'''

    m_cohort = '''select
                        to_char(nti.create_date,
                        'YYYY-MM-DD'),
                        nfc.acquirer,
                        nti.status,
                        count(nti.status)::text,
                        sum(nti.amount)::text
                    from
                        tp_bank_service.public.npsb_transaction_info nti
                    inner join tp_bank_service.public.npsb_fi_config nfc
                        on
                        nti.acquirer_id = nfc.acquirer_id
                    where
                        1 = 1
                        and nti.create_date::date >= now() - interval '30 days'
                        and nti.create_date <= now() - interval '5 minutes'
                    group by
                        1,
                        2,
                        3
                    ;'''

    m_err_user_count = '''select
                                to_char(create_date, 'YYYY-MM-DD') as "Day",
                                case
                                    when response ~ '.*"message":\s*"([^"]+)"' then substring(response from '.*"message":\s*"([^"]+)"')
                                    when response ~ '.*"error":\s*"([^"]+)"' then substring(response from '.*"error":\s*"([^"]+)"')
                                    else response
                                end as status,
                                count(distinct(SUBSTRING(request from '.*"receiver_wallet_no":\s*"([^"]+)"')))::text
                            from
                                tallypay_issuer.public.request_log rl
                            where
                                rl.create_date::date >= now() - interval '30 days'
                                and rl.create_date <= now() - interval '5 minutes'
                                and request_id is not null
                                and request not ilike '%hex%'
                                and response not ilike '%{%NPSB transfer credit%}%'
                            group by
                                1,
                                2
                            order by
                                1 desc,
                                3 desc
                            ;'''

    m_growth_rate = '''WITH DailyStats AS (
                                    SELECT
                                        TO_CHAR(create_date, 'YYYY-MM-DD') AS transaction_date,
                                        status,
                                        COUNT(status)::text AS transaction_count,
                                        SUM(amount)::text AS total_amount,
                                        LAG(SUM(amount)) OVER (ORDER BY TO_CHAR(create_date, 'YYYY-MM-DD')) AS prev_day_amount
                                    FROM
                                        tp_bank_service.public.npsb_transaction_info
                                    WHERE
                                        create_date::date >= now() - interval '30 days'
                                        AND create_date::date < 'now()'
                                    GROUP BY
                                        TO_CHAR(create_date, 'YYYY-MM-DD'),
                                        status
                                )
                                SELECT
                                    transaction_date,
                                    status,
                                    transaction_count,
                                    total_amount,
                                    ROUND(
                                        CASE
                                            WHEN NULLIF(prev_day_amount, 0) IS NULL THEN 0
                                            ELSE ((total_amount::numeric - prev_day_amount::numeric) / NULLIF(prev_day_amount::numeric, 0) * 100)
                                        END,
                                        2
                                    )::text AS total_amount_growth
                                FROM
                                    DailyStats
                                ORDER BY
                                    1 DESC,
                                    status;'''

    query = {
        'hr_log': hr_log,
        'hr_cohort': hr_cohort,
        'hr_err_user_count': hr_err_user_count,
        'm_log': m_log,
        'm_cohort': m_cohort,
        'm_err_user_count': m_err_user_count,
        'm_growth_rate': m_growth_rate
    }

    return query[q]

def pg_conn(db, query):
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            database=db,
            user=os.environ.get("TP_PG_USR"),
            password=os.environ.get("TP_PG_PWD"),
            host=os.environ.get("TP_HOST"),
            port='5432'
        )
        cur = conn.cursor()
        cur.execute(query)
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
    return queryset

def update_sheet_with_query(query_key, db_name, sheet_name, headers):
    query = gen_q(query_key)
    results = pg_conn(db_name, query)
    if not results:
        data = [headers] + [('0',) * len(headers)]
    else:
        data = [headers] + results
    sheet_update(data, sheet_name)

def sqr_main_threaded():
    tasks = [
        ('hr_log', 'tallypay_issuer', 'sqr_log', ('hour', 'status', 'count')),
        ('hr_cohort', 'tp_bank_service', 'sqr_hr_cohort', ('hour', 'acquirer', 'status', 'count', 'amount')),
        ('hr_err_user_count', 'tallypay_issuer', 'hr_m_err_usr', ('hour', 'status', 'count')),
        ('m_log', 'tallypay_issuer', 'sqr_m_log', ('date', 'status', 'count')),
        ('m_cohort', 'tp_bank_service', 'sqr_m_cohort', ('date', 'acquirer', 'status', 'count', 'amount')),
        ('m_err_user_count', 'tallypay_issuer', 'sqr_m_err_usr', ('date', 'status', 'count')),
        ('m_growth_rate', 'tp_bank_service', 'm_growth_rate', ('date', 'status', 'count', 'amount', 'growth_percentage')),
    ]

    threads = []
    for query_key, db_name, sheet_name, headers in tasks:
        thread = Thread(target=update_sheet_with_query, args=(query_key, db_name, sheet_name, headers))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print(f"{sqr_main_threaded.__name__} execution complete")

# Run the threaded function
# sqr_main_threaded()
