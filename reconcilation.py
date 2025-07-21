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
    # Overall picture
    "overall_picture": """
        SELECT
            create_date::date,
            COUNT(*),
            SUM(amount)
        FROM
            reconciliation r
        GROUP BY 1
        ORDER BY 1 DESC;
    """,

    # Service wise dispute
    "service_wise_dispute": """
        SELECT
            create_date::date,
            service_name,
            txn_type,
            COUNT(*),
            SUM(amount)
        FROM
            reconciliation r
        GROUP BY 1,2,3
        ORDER BY 1 DESC;
    """,

    # In/Out wise report
    "in_out_wise_report": """
        SELECT
            create_date::date,
            txn_type,
            COUNT(*),
            SUM(amount)
        FROM
            reconciliation r
        GROUP BY 1,2
        ORDER BY 1 DESC;
    """,

    # Proposed Decision wise report
    "proposed_decision_report": """
        SELECT
            create_date::date,
            proposed_decision,
            COUNT(*),
            SUM(amount)
        FROM
            reconciliation r
        WHERE 
            proposed_decision != 'NO ACTION NEEDED'    
        GROUP BY 1,2
        ORDER BY 1 DESC;
    """,

    # Service vs Proposed Decision report
    "service_vs_proposed_decision_report": """
            select
        create_date::date,
        case 
            when service_name in ('NAGAD','ROCKET') then concat(service_name ,'-',txn_type)
            else service_name
        end as service_type,
        proposed_decision ,
        count(*),
        sum(amount) 
    from
        reconciliation r 
    WHERE 
        proposed_decision != 'NO ACTION NEEDED'    
    group by 1,2,3
    order by 1 desc;
    """,

    # Money In/Out vs Proposed Decision report
    "money_in_out_vs_proposed_decision_report": """
        SELECT
            create_date::date,
            txn_type,
            proposed_decision,
            COUNT(*),
            SUM(amount)
        FROM
            reconciliation r
        WHERE 
            proposed_decision != 'NO ACTION NEEDED'    
        GROUP BY 1,2,3
        ORDER BY 1 DESC;
    """,

    "email_kpi": """
        select
        to_char(created_at, 'YYYY-MM-DD') as day,
        assignee ,
        status,
        count(assignee)
    from
        public.display_data_app_pne_support_monitoring
    where
        1 = 1
        and action_type = 'EMAIL'
        and assignee != ''
        and created_at>= NOW() - interval '1 month'
    GROUP BY 
                1, 2,3
            ORDER BY 
                1 desc;
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
        #print(f"Error fetching data: {e}")
        return None


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

def reconciliation_reports():
    db_params = {
        "user": os.getenv("TP_PG_USR_3"),
        "password": os.getenv("TP_PG_PWD_3"),
        "host": os.getenv("TP_HOST_3")
    }

    db_mapping = {
        "overall_picture": "reconciliation",
        "service_wise_dispute": "reconciliation",
        "in_out_wise_report": "reconciliation",
        "proposed_decision_report": "reconciliation",
        "service_vs_proposed_decision_report": "reconciliation",
        "money_in_out_vs_proposed_decision_report": "reconciliation",
        "email_kpi": "support_portal"
    }

    sheet_mapping = {
        "overall_picture": "recon_overall_picture_sheet",
        "service_wise_dispute": "recon_service_wise_dispute_sheet",
        "in_out_wise_report": "recon_in_out_wise_report_sheet",
        "proposed_decision_report": "recon_proposed_decision_report_sheet",
        "service_vs_proposed_decision_report": "recon_service_vs_proposed_decision_report_sheet",
        "money_in_out_vs_proposed_decision_report": "recon_money_in_out_vs_proposed_decision_report_sheet",
        "email_kpi": "email_kpi_sheet"
    }

    headers_mapping = {
        "overall_picture": ["create_date", "count", "sum_amount"],
        "service_wise_dispute": ["create_date", "service_name", "txn_type", "count", "sum_amount"],
        "in_out_wise_report": ["create_date", "txn_type", "count", "sum_amount"],
        "proposed_decision_report": ["create_date", "proposed_decision", "count", "sum_amount"],
        "service_vs_proposed_decision_report": ["create_date", "service_name", "proposed_decision", "count", "sum_amount"],
        "money_in_out_vs_proposed_decision_report": ["create_date", "txn_type", "proposed_decision", "count", "sum_amount"],
        "email_kpi": ["day","assignee","status","count"]
    }

    # Use threading to parallelize the process
    with ThreadPoolExecutor(max_workers=6) as executor:
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

# if __name__ == "__main__":
#     reconciliation_reports()
