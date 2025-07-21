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
    "RECHARGE (GP)": """
    WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM top_up_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND mobile_operator = 'GP'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN tui.status = 'SUCCESS' THEN tui.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN tui.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN tui.status = 'REVERSED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN tui.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN tui.status NOT IN ('SUCCESS', 'REVERSED', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN tui.status = 'SUCCESS' THEN tui.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN top_up_info tui
    ON DATE(tui.create_date) = dg.date
   AND tui.create_date <= NOW() - interval '3 minutes'
   AND tui.mobile_operator = 'GP'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,


    "RECHARGE (BL)": """
    WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM top_up_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND mobile_operator = 'BL'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN tui.status = 'SUCCESS' THEN tui.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN tui.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN tui.status = 'REVERSED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN tui.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN tui.status NOT IN ('SUCCESS', 'REVERSED', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN tui.status = 'SUCCESS' THEN tui.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN top_up_info tui
    ON DATE(tui.create_date) = dg.date
   AND tui.create_date <= NOW() - interval '3 minutes'
   AND tui.mobile_operator = 'BL'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,


    "RECHARGE (ROBI)": """
     WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM top_up_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND mobile_operator = 'ROBI'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN tui.status = 'SUCCESS' THEN tui.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN tui.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN tui.status = 'REVERSED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN tui.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN tui.status NOT IN ('SUCCESS', 'REVERSED', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN tui.status = 'SUCCESS' THEN tui.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN top_up_info tui
    ON DATE(tui.create_date) = dg.date
   AND tui.create_date <= NOW() - interval '3 minutes'
   AND tui.mobile_operator = 'ROBI'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,



    "RECHARGE (AIRTEL)": """
      WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM top_up_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND mobile_operator = 'AIRTEL'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN tui.status = 'SUCCESS' THEN tui.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN tui.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN tui.status = 'REVERSED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN tui.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN tui.status NOT IN ('SUCCESS', 'REVERSED', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN tui.status = 'SUCCESS' THEN tui.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN top_up_info tui
    ON DATE(tui.create_date) = dg.date
   AND tui.create_date <= NOW() - interval '3 minutes'
   AND tui.mobile_operator = 'AIRTEL'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,



   "RECHARGE (TT)": """
     WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM top_up_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND mobile_operator = 'TT'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN tui.status = 'SUCCESS' THEN tui.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN tui.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN tui.status = 'REVERSED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN tui.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN tui.status NOT IN ('SUCCESS', 'REVERSED', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN tui.status = 'SUCCESS' THEN tui.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN top_up_info tui
    ON DATE(tui.create_date) = dg.date
   AND tui.create_date <= NOW() - interval '3 minutes'
   AND tui.mobile_operator = 'TT'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,



    "NAGAD IN": """
             WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
            SUM(
        CASE 
            WHEN status = 'SUCCESS' 
            AND NOT (nagad_status IN ('Ready', 'Aborted', 'Cancelled') AND status = 'FAILED') 
            AND status != 'CHECKOUT' 
        THEN 1 ELSE 0 
        END
    ) * 100.0 / 
    NULLIF(
        COUNT(CASE 
            WHEN NOT (nagad_status IN ('Ready', 'Aborted', 'Cancelled') AND status = 'FAILED') 
            AND status != 'CHECKOUT' 
        THEN 1 END), 
        0
    ) AS success_percentage
    FROM nagad_txn
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN ntx.status = 'SUCCESS' THEN ntx.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN ntx.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN ntx.status = 'REFUNDED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN ntx.nagad_status = 'Failed' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN ntx.status NOT IN ('SUCCESS', 'REFUNDED', 'FAILED', 'ORDER_ERR', 'CHECKOUT', 'INITIATED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN ntx.status = 'SUCCESS' THEN ntx.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN nagad_txn ntx
    ON DATE(ntx.create_date) = dg.date
   AND ntx.create_date <= NOW() - interval '3 minutes'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,



    "ROCKET IN": """
            WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN result != 'TIMEOUT' THEN 1 ELSE 0 END), 0) AS success_percentage
    FROM dbbl_transaction
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN dtx.status = 'SUCCESS' THEN dtx.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN dtx.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN dtx.status = 'REFUNDED' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN dtx.result = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN dtx.status NOT IN ('SUCCESS', 'REFUNDED', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN dtx.status = 'SUCCESS' THEN dtx.amount ELSE 0 END)), 0) AS total_success_amount,
    COALESCE(
       CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN dbbl_transaction dtx
    ON DATE(dtx.create_date) = dg.date
   AND dtx.create_date <= NOW() - interval '3 minutes'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,



    "ROCKET OUT": """
              WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' AND amount >= 10 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(CASE WHEN amount >= 10 THEN 1 END), 0) AS success_percentage
    FROM transaction_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND financial_institute = 'ROCKET'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN ti.status = 'SUCCESS' THEN ti.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN ti.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN ti.status = 'REVERSE' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN ti.status = 'FAILED' AND ti.amount >= 10 THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN ti.status NOT IN ('SUCCESS', 'REVERSE', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN ti.status = 'SUCCESS' THEN ti.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN transaction_info ti
    ON DATE(ti.create_date) = dg.date
   AND ti.create_date <= NOW() - interval '3 minutes'
   AND ti.financial_institute = 'ROCKET'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,




    "NAGAD OUT": """
               WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM transaction_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND financial_institute = 'NAGAD'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN ti.status = 'SUCCESS' THEN ti.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN ti.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN ti.status = 'REVERSE' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN ti.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN ti.status NOT IN ('SUCCESS', 'REVERSE', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN ti.status = 'SUCCESS' THEN ti.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN transaction_info ti
    ON DATE(ti.create_date) = dg.date
   AND ti.create_date <= NOW() - interval '3 minutes'
   AND ti.financial_institute = 'NAGAD'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,



    "VISA CARD": """
          WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM card_txn_log
    WHERE create_date >= NOW() - interval '60 minutes'
      AND create_date <= NOW() - interval '3 minutes'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN ti.status = 'SUCCESS' THEN ti.create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN ti.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN ti.status = 'REVERSE' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN ti.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN ti.status NOT IN ('SUCCESS', 'REVERSE', 'FAILED') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN ti.status = 'SUCCESS' THEN ti.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 50 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 50 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN card_txn_log ti
    ON DATE(ti.create_date) = dg.date
   AND ti.create_date <= NOW() - interval '3 minutes'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
    """,




    "SQR PAYMENT": """
      WITH last_30_min_health AS (
    SELECT 
        COUNT(*) FILTER (
            WHERE 
                CASE
                    WHEN response ILIKE '%{%NPSB transfer credit%}%' THEN 'SUCCESS'
                    WHEN response ~ '.*"message":\\s*"([^"]+)"' THEN substring(response FROM '.*"message":\\s*"([^"]+)"')
                    WHEN response ~ '.*"error":\\s*"([^"]+)"' THEN substring(response FROM '.*"error":\\s*"([^"]+)"')
                    ELSE 'FAILED'
                END = 'SUCCESS'
        ) AS recent_success_count
    FROM request_log
    WHERE create_date >= NOW() - interval '30 minutes' 
      AND create_date <= NOW() - interval '3 minutes'
)

SELECT
    DATE(create_date) AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN status = 'SUCCESS' THEN create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS success,
    COUNT(CASE WHEN status = 'REVERSED' THEN 1 END) AS reverse,
    COUNT(CASE WHEN status NOT IN ('SUCCESS', 'REVERSED') THEN 1 END) AS failed,
    0 AS dispute,
    ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END)) AS success_amount,
    CASE 
        WHEN recent_success_count < 5 THEN 'NOT OK'
        WHEN recent_success_count BETWEEN 5 AND 20 THEN 'ACCEPTABLE'
        ELSE 'OK'
    END AS health,
    TO_CHAR(
        ROUND(
            (
                COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END)::NUMERIC * 100.0
            ) / NULLIF(COUNT(*), 0), 2
        ), 'FM999999999.00'
    ) || '%' AS success_rate
FROM (
    SELECT
        rl.create_date,
        CASE
            WHEN response ILIKE '%{%NPSB transfer credit%}%' THEN 'SUCCESS'
            WHEN response ~ '.*"message":\\s*"([^"]+)"' THEN substring(response FROM '.*"message":\\s*"([^"]+)"')
            WHEN response ~ '.*"error":\\s*"([^"]+)"' THEN substring(response FROM '.*"error":\\s*"([^"]+)"')
            ELSE 'FAILED'
        END AS status,
        CAST(response::jsonb ->> 'amount' AS NUMERIC) AS amount
    FROM
        tallypay_issuer.public.request_log rl
    WHERE
        rl.create_date::date = CURRENT_DATE
        AND rl.request_id IS NOT NULL
        AND rl.request NOT ILIKE '%hex%'
) subquery,
last_30_min_health
GROUP BY 
    DATE(create_date),
    recent_success_count
ORDER BY 
    DATE(create_date) DESC;
    """,


    "CBL": """
                WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM backend_db.public.bank_txn_request btr2
    WHERE (btr2.issue_time + interval '6 hours') >= NOW() - interval '30 minutes'
      AND (btr2.issue_time + interval '6 hours') <= NOW() - interval '3 minutes'
      AND btr2.txn_request_type = 'CASH_OUT'
      AND btr2.bank_swift_code = 'CIBLBDDH'
      AND btr2.channel = 'CBL'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN btr.status = 'SUCCESS' THEN btr.issue_time + interval '6 hours' END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN btr.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN btr.status = 'REVERSE' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN btr.status NOT IN ('SUCCESS') THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN btr.status IN ('DISPUTE', 'PENDING') THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN btr.status = 'SUCCESS' THEN btr.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 50 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 50 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN backend_db.public.bank_txn_request btr
    ON DATE(btr.issue_time + interval '6 hours') = dg.date
   AND (btr.issue_time + interval '6 hours') <= NOW() - interval '3 minutes'
   AND btr.txn_request_type = 'CASH_OUT'
   AND btr.bank_swift_code = 'CIBLBDDH'
   AND btr.channel = 'CBL'
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
            """,

    "BEFTN": """
            WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status IN ('SUCCESS', 'REQUESTED', 'PENDING') THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM backend_db.public.bank_txn_request btr2
    WHERE (btr2.issue_time + interval '6 hours') >= NOW() - interval '60 minutes'
      AND (btr2.issue_time + interval '6 hours') <= NOW() - interval '3 minutes'
      AND btr2.txn_request_type = 'CASH_OUT'
      AND btr2.channel = 'BEFTN'
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN btr.status IN ('SUCCESS', 'REQUESTED', 'PENDING') THEN btr.issue_time + interval '6 hours' END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN btr.status IN ('SUCCESS', 'REQUESTED', 'PENDING') THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN btr.status = 'REVERSIBLE' THEN 1 ELSE 0 END), 0) AS reversible,
    COALESCE(SUM(CASE WHEN btr.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN btr.status NOT IN ('SUCCESS', 'REVERSIBLE', 'FAILED', 'REQUESTED', 'PENDING') THEN 1 ELSE 0 END), 0) AS pending,
    COALESCE(TRUNC(SUM(CASE WHEN btr.status IN ('SUCCESS', 'REQUESTED', 'PENDING') THEN btr.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 30 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 30 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN backend_db.public.bank_txn_request btr
    ON DATE(btr.issue_time + interval '6 hours') = dg.date
   AND btr.txn_request_type = 'CASH_OUT'
   AND btr.channel = 'BEFTN'
LEFT JOIN service_health_calc shc ON TRUE
WHERE
    (btr.issue_time + interval '6 hours') <= NOW()
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reversible DESC,
    failed DESC,
    pending DESC;
            """,




    "NPSB INSTANT": """
       WITH date_generator AS (
    SELECT CURRENT_DATE AS date
),
service_health_calc AS (
    SELECT 
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS success_percentage
    FROM tp_bank_service.public.bank_transaction_info
    WHERE create_date >= NOW() - interval '30 minutes'
      AND create_date <= NOW() - interval '3 minutes'
      AND channel IN ('NPSB', 'MTB')
)
SELECT
    dg.date AS date,
    COALESCE(TO_CHAR(MAX(CASE WHEN status = 'SUCCESS' THEN create_date END), 'HH24:MI'), '00:00') AS last_success_time,
    COALESCE(SUM(CASE WHEN bti.status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success,
    COALESCE(SUM(CASE WHEN bti.status = 'REVERSE' THEN 1 ELSE 0 END), 0) AS reverse,
    COALESCE(SUM(CASE WHEN bti.status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed,
    COALESCE(SUM(CASE WHEN bti.status = 'UNKNOWN' THEN 1 ELSE 0 END), 0) AS dispute,
    COALESCE(TRUNC(SUM(CASE WHEN bti.status = 'SUCCESS' THEN bti.amount ELSE 0 END)), 0) AS today_success_amount,
    COALESCE(
        CASE 
            WHEN shc.success_percentage IS NULL OR shc.success_percentage = 0 THEN 'DOWN'
            WHEN shc.success_percentage < 50 THEN 'NOT OK'
            WHEN shc.success_percentage BETWEEN 50 AND 70 THEN 'ACCEPTABLE'
            WHEN shc.success_percentage > 70 THEN 'OK'
        END, 'DOWN'
    ) AS service_health,
    COALESCE(ROUND(shc.success_percentage, 2) || '%', '0%') AS success_rate
FROM 
    date_generator dg
LEFT JOIN tp_bank_service.public.bank_transaction_info bti
    ON DATE(bti.create_date) = dg.date
   AND bti.create_date <= NOW() - interval '3 minutes'
   AND bti.channel IN ('NPSB', 'MTB')
LEFT JOIN service_health_calc shc ON TRUE
GROUP BY 
    dg.date, shc.success_percentage
ORDER BY 
    success DESC,
    reverse DESC,
    failed DESC,
    dispute DESC;
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
        print(f"Error fetching data for {dbname}: {e}")
        return None

# Function to handle each query and update the corresponding Google Sheet
# Updates row by row instead of replacing the entire sheet
def process_query(service_name, query, db_params, dbname):
    result = fetch_data(query, db_params, dbname)
    processed_data = []

    if result:
        for row in result:
            converted_row = [
                x.strftime('%Y-%m-%d') if isinstance(x, datetime.date) else
                f"{int(x):,}" if isinstance(x, Decimal) and row.index(x) == 6 else  # Format SUCCESS_AMOUNT with commas
                float(x) if isinstance(x, Decimal) else
                x
                for x in row
            ]
            # Append the service name at the start of each row
            processed_data.append([service_name] + converted_row)
    else:
        print(f"No data returned for {service_name}.")

    return processed_data


# Main function to process all services and update the sheet
def service_health():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
    }

    db_mapping = {
        "RECHARGE (GP)": "topup_service",
        "RECHARGE (BL)": "topup_service",
        "RECHARGE (ROBI)": "topup_service",
        "RECHARGE (AIRTEL)": "topup_service",
        "RECHARGE (TT)": "topup_service",
        "NAGAD IN": "nobopay_payment_gw",
        "ROCKET IN": "nobopay_payment_gw",
        "ROCKET OUT": "tallypay_to_fi_integration",
        "NAGAD OUT": "tallypay_to_fi_integration",
        "VISA CARD": "tp_bank_service",
        "SQR PAYMENT": "tallypay_issuer",
        "CBL": "backend_db",
        "BEFTN": "backend_db",
        "NPSB INSTANT": "tp_bank_service"
    }

    headers = ["SERVICES", "DATE", "LAST_SUCCESS_TIME", "SUCCESS", "REVERSE", "FAILED", "DISPUTE", "SUCCESS_AMOUNT", "HEALTH", "SUCCESS_RATE"]
    all_data = [headers]  # Initialize with headers for the sheet

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}

        # Submit all queries for parallel processing
        for service_name, query in queries.items():
            dbname = db_mapping.get(service_name)
            futures[executor.submit(process_query, service_name, query, db_params, dbname)] = service_name

        # Collect results from all queries
        for future in as_completed(futures):
            try:
                query_result = future.result()
                all_data.extend(query_result)  # Append results row by row
            except Exception as e:
                print(f"Error processing query for {futures[future]}: {e}")

    # Update the sheet with all accumulated data
    sheet_update(all_data, "service_health")
    print("All data updated successfully in sheet 'service_health'.")

if __name__ == "__main__":
    service_health()