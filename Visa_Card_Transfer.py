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
    # Hourly Transaction Cohort
    "hourly_txn_cohort": """
        select
        to_char(create_date,
        'HH24') as hour,
        bank_name,
        status,
        COUNT(status) as count,
        SUM(amount) as total_amount
    from
        card_txn_log ctl
    where
        --status = 'SUCCESS'
        create_date::date = CURRENT_DATE
    group by
        hour,
        bank_name,
        status;
    """,

    # Daily Cohort
    "day_cohort": """
    WITH bank_totals AS (
    SELECT
        bank_name,
        SUM(amount) AS total_amount_per_bank
    FROM
        card_txn_log
    WHERE 
        create_date::date >= NOW() - INTERVAL '30 days'
        AND create_date <= NOW() - INTERVAL '5 minutes'
    GROUP BY
        bank_name
    )
    SELECT
        to_char(ctl.create_date, 'YYYY-MM-DD') as date,
        ctl.bank_name || ' (Total: ' || bt.total_amount_per_bank || ')' AS bank_name,
        ctl.status,
        COUNT(ctl.status) AS transaction_count,
        SUM(ctl.amount) AS total_amount
    FROM
        card_txn_log ctl
    INNER JOIN 
        bank_totals bt
        ON ctl.bank_name = bt.bank_name
    WHERE 
        create_date::date >= NOW() - INTERVAL '30 days'
        AND create_date <= NOW() - INTERVAL '5 minutes'
    GROUP BY
        date, ctl.bank_name, ctl.status, bt.total_amount_per_bank
    ORDER BY 
        date DESC;
    """,

    # New Query
    "visa_status_summary": """
        SELECT
            COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) AS today_success_count,
            COUNT(CASE WHEN status = 'REVERSE' THEN 1 END) AS today_reverse_count,
            COUNT(CASE WHEN status = 'DISPUTE' THEN 1 END) AS today_dispute_count,
            SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END) AS today_success_amount,
            SUM(CASE WHEN status = 'REVERSE' THEN amount ELSE 0 END) AS today_reverse_amount,
            SUM(CASE WHEN status = 'DISPUTE' THEN amount ELSE 0 END) AS today_dispute_amount
        FROM
            card_txn_log
        WHERE
            DATE(create_date) = CURRENT_DATE;
    """,

    "visa_dispute_count_amount": """
    SELECT 
        COALESCE(COUNT(*), 0) AS total_dispute_count,
        COALESCE(SUM(amount), 0) AS total_dispute_amount
    FROM 
        card_txn_log
    WHERE 
        status = 'DISPUTE';
    """,

    "Visa_Reverse_reasons": """
      SELECT 
        to_char(ctl.create_date, 'YYYY-MM-DD') as date,
        ctl.bank_name || ' (' || COUNT(*) OVER (PARTITION BY ctl.bank_name) || ')' AS bank_name_with_count,
        ctl.status,
        rl.response::jsonb ->> 'actionCode' AS action_code,
    CASE 
        WHEN rl.response::jsonb ->> 'actionCode' = '61' THEN '61-Exceeds approval amount limit'
        WHEN rl.response::jsonb ->> 'actionCode' = '15' THEN '15-No such issuer'
        WHEN rl.response::jsonb ->> 'actionCode' = '93' THEN '93-Transaction cannot be completed – violation of law'
        WHEN rl.response::jsonb ->> 'actionCode' = '12' THEN '12-Invalid transaction'
        WHEN rl.response::jsonb ->> 'actionCode' = '91' THEN '91-Receiver account is not valid. Please recheck the VISA card details'
        WHEN rl.response::jsonb ->> 'actionCode' = '14' THEN '14-Invalid card number (no such number)'
        WHEN rl.response::jsonb ->> 'actionCode' = '05' THEN '05-Do not honor'
        WHEN rl.response::jsonb ->> 'actionCode' = '54' THEN '54-Expired card'
        WHEN rl.response::jsonb ->> 'actionCode' = '57' THEN '57-Transaction not permitted to cardholder'
        WHEN rl.response::jsonb ->> 'actionCode' = '96' THEN '96-System malfunction'
        WHEN rl.response::jsonb ->> 'actionCode' = '59' THEN '59-Suspected fraud'
        WHEN rl.response::jsonb ->> 'actionCode' = '62' THEN '62-This VISA card is restricted for transactions. Please communicate with the issuer bank'
        WHEN rl.response::jsonb ->> 'actionCode' = '04' THEN '04-Pick-up'
        WHEN rl.response::jsonb ->> 'actionCode' = '75' THEN '75-Allowable number of PIN entry tries exceeded'
        ELSE 'Other'
    END || ' (' || COUNT(*) OVER (PARTITION BY 
    CASE 
        WHEN rl.response::jsonb ->> 'actionCode' = '61' THEN '61-Exceeds approval amount limit'
        WHEN rl.response::jsonb ->> 'actionCode' = '15' THEN '15-No such issuer'
        WHEN rl.response::jsonb ->> 'actionCode' = '93' THEN '93-Transaction cannot be completed – violation of law'
        WHEN rl.response::jsonb ->> 'actionCode' = '12' THEN '12-Invalid transaction'
        WHEN rl.response::jsonb ->> 'actionCode' = '91' THEN '91-Receiver account is not valid. Please recheck the VISA card details'
        WHEN rl.response::jsonb ->> 'actionCode' = '14' THEN '14-Invalid card number (no such number)'
        WHEN rl.response::jsonb ->> 'actionCode' = '05' THEN '05-Do not honor'
        WHEN rl.response::jsonb ->> 'actionCode' = '54' THEN '54-Expired card'
        WHEN rl.response::jsonb ->> 'actionCode' = '57' THEN '57-Transaction not permitted to cardholder'
        WHEN rl.response::jsonb ->> 'actionCode' = '96' THEN '96-System malfunction'
        WHEN rl.response::jsonb ->> 'actionCode' = '59' THEN '59-Suspected fraud'
        WHEN rl.response::jsonb ->> 'actionCode' = '62' THEN '62-This VISA card is restricted for transactions. Please communicate with the issuer bank'
        WHEN rl.response::jsonb ->> 'actionCode' = '04' THEN '04-Pick-up'
        WHEN rl.response::jsonb ->> 'actionCode' = '75' THEN '75-Allowable number of PIN entry tries exceeded'
        ELSE 'Other'
    END
    ) || ')' AS action_code_meaning_with_count
    FROM 
        card_txn_log ctl
    INNER JOIN 
        request_log rl 
        ON rl.request_id = ctl.request_id 
    WHERE 
        ctl.status = 'REVERSE'
        AND rl.request ILIKE '%VisaFundTransferDto%'
    ORDER BY 
        ctl.id DESC;
    """,

    "hr_reverse_visa_reasons": """
    SELECT 
    to_char(ctl.create_date,'HH24') as hour,
    ctl.bank_name || ' (' || COUNT(*) OVER (PARTITION BY ctl.bank_name) || ')' AS bank_name_with_count,
    ctl.status,
    rl.response::jsonb ->> 'actionCode' AS action_code,
    CASE 
        WHEN rl.response::jsonb ->> 'actionCode' = '61' THEN '61-Exceeds approval amount limit'
        WHEN rl.response::jsonb ->> 'actionCode' = '15' THEN '15-No such issuer'
        WHEN rl.response::jsonb ->> 'actionCode' = '93' THEN '93-Transaction cannot be completed – violation of law'
        WHEN rl.response::jsonb ->> 'actionCode' = '12' THEN '12-Invalid transaction'
        WHEN rl.response::jsonb ->> 'actionCode' = '91' THEN '91-Receiver account is not valid. Please recheck the VISA card details'
        WHEN rl.response::jsonb ->> 'actionCode' = '14' THEN '14-Invalid card number (no such number)'
        WHEN rl.response::jsonb ->> 'actionCode' = '05' THEN '05-Do not honor'
        WHEN rl.response::jsonb ->> 'actionCode' = '54' THEN '54-Expired card'
        WHEN rl.response::jsonb ->> 'actionCode' = '57' THEN '57-Transaction not permitted to cardholder'
        WHEN rl.response::jsonb ->> 'actionCode' = '96' THEN '96-System malfunction'
        WHEN rl.response::jsonb ->> 'actionCode' = '59' THEN '59-Suspected fraud'
        WHEN rl.response::jsonb ->> 'actionCode' = '62' THEN '62-This VISA card is restricted for transactions. Please communicate with the issuer bank'
        WHEN rl.response::jsonb ->> 'actionCode' = '04' THEN '04-Pick-up'
        WHEN rl.response::jsonb ->> 'actionCode' = '75' THEN '75-Allowable number of PIN entry tries exceeded'
        ELSE 'Other'
    END || ' (' || COUNT(*) OVER (PARTITION BY 
    CASE 
        WHEN rl.response::jsonb ->> 'actionCode' = '61' THEN '61-Exceeds approval amount limit'
        WHEN rl.response::jsonb ->> 'actionCode' = '15' THEN '15-No such issuer'
        WHEN rl.response::jsonb ->> 'actionCode' = '93' THEN '93-Transaction cannot be completed – violation of law'
        WHEN rl.response::jsonb ->> 'actionCode' = '12' THEN '12-Invalid transaction'
        WHEN rl.response::jsonb ->> 'actionCode' = '91' THEN '91-Receiver account is not valid. Please recheck the VISA card details'
        WHEN rl.response::jsonb ->> 'actionCode' = '14' THEN '14-Invalid card number (no such number)'
        WHEN rl.response::jsonb ->> 'actionCode' = '05' THEN '05-Do not honor'
        WHEN rl.response::jsonb ->> 'actionCode' = '54' THEN '54-Expired card'
        WHEN rl.response::jsonb ->> 'actionCode' = '57' THEN '57-Transaction not permitted to cardholder'
        WHEN rl.response::jsonb ->> 'actionCode' = '96' THEN '96-System malfunction'
        WHEN rl.response::jsonb ->> 'actionCode' = '59' THEN '59-Suspected fraud'
        WHEN rl.response::jsonb ->> 'actionCode' = '62' THEN '62-This VISA card is restricted for transactions. Please communicate with the issuer bank'
        WHEN rl.response::jsonb ->> 'actionCode' = '04' THEN '04-Pick-up'
        WHEN rl.response::jsonb ->> 'actionCode' = '75' THEN '75-Allowable number of PIN entry tries exceeded'
        ELSE 'Other'
    END
    ) || ')' AS action_code_meaning_with_count
    FROM 
        card_txn_log ctl
    INNER JOIN 
        request_log rl 
        ON rl.request_id = ctl.request_id 
    WHERE 
        ctl.status = 'REVERSE'
        AND rl.request ILIKE '%VisaFundTransferDto%'
        AND ctl.create_date::date = CURRENT_DATE
    ORDER BY 
        hour DESC;
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

def VISA_Transfers():
    db_params = {
        "user": os.getenv("TP_PG_USR"),
        "password": os.getenv("TP_PG_PWD"),
        "host": os.getenv("TP_HOST")
        # "port": os.getenv("DB_PORT")
    }

    db_mapping = {
        "hourly_txn_cohort": "tp_bank_service",
        "day_cohort": "tp_bank_service",
        "visa_status_summary": "tp_bank_service",
        "Visa_Reverse_reasons": "tp_bank_service",
        "hr_reverse_visa_reasons": "tp_bank_service",
        "visa_dispute_count_amount": "tp_bank_service"# Add the new query's database mapping
    }

    # Define sheet names corresponding to each query
    sheet_mapping = {
        "hourly_txn_cohort": "hourly_txn_cohort_visa__sheet",
        "day_cohort": "day_cohort_visa_sheet",
        "visa_status_summary": "visa_status_summary_sheet",
        "Visa_Reverse_reasons": "Visa_Reverse_reasons_sheet",
        "hr_reverse_visa_reasons": "visa_hr_reverse_with_reasons_sheet",
        "visa_dispute_count_amount": "visa_lifeTime_dispute_sheet"# New sheet for the new query
    }

    # Define headers for each sheet
    headers_mapping = {
        "hourly_txn_cohort": ["time_date", "bank_name", "status", "transaction_count", "total_amount"],
        "day_cohort": ["time_date", "bank_name", "status", "transaction_count", "total_amount"],
        "visa_status_summary": ["today_success_count", "today_reverse_count", "today_dispute_count",
                                "today_success_amount", "today_reverse_amount", "today_dispute_amount"],  # Headers for the new query
        "Visa_Reverse_reasons": ["create_date",	"bank_name_with_count", "status", "action_code", "action_code_meaning"],
        "hr_reverse_visa_reasons": ["create_date",	"bank_name_with_count", "status", "action_code", "action_code_meaning"],
        "visa_dispute_count_amount": ["lifetime_dispute_count", "lifetime_dispute_amount"]
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
    VISA_Transfers()
