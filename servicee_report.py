from datetime import datetime, timedelta
from typing import List
import os
from dotenv import load_dotenv
import psycopg2
import pandas as pd
from pretty_html_table import build_table
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import matplotlib.pyplot as plt
import seaborn as sns

load_dotenv()


def get_week_dates() -> List[str]:
    '''Return a list of strings of last week start, cur week start and cur day'''

    today = datetime.today()

    # Calculate the start of the current week (Sunday)
    start_of_this_week = today - timedelta(days=(today.weekday() + 1) % 7)

    # Calculate the start of the previous week
    start_of_last_week = start_of_this_week - timedelta(days=7)

    # formatting the datetime objs as date str
    start_of_last_week_str = start_of_last_week.strftime('%Y-%m-%d')
    start_of_this_week_str = start_of_this_week.strftime('%Y-%m-%d')
    today_str = today.strftime('%Y-%m-%d')

    return [start_of_last_week_str, start_of_this_week_str, today_str]


def gen_q(dates: List[str], query) -> str:
    money_out_FI = f'''select
                            ti.financial_institute, 
                            count(id),
                            COUNT(case when extract(epoch from (update_date - create_date)) <= 60 then 1 end) as count_within_60_seconds,
                            ROUND((COUNT(case when extract(epoch from (update_date - create_date)) <= 60 then 1 end) * 100.0) / COUNT(id), 2) as customer_satisfaction
                        from
                            tallypay_to_fi_integration.public.transaction_info ti
                        where
                            1 = 1
                            and ti.create_date::date >= '{dates[0]}'
                            and ti.create_date::date < '{dates[1]}'
                            and ti.status in ('REVERSE', 'SUCCESS', 'FAILED')
                        group by
                            1
                        ;'''

    money_out_BANK = f'''select
                            case 
                                when btr.bank_swift_code = 'CIBLBDDH' then 'CBS'
                                else 'EFT'
                            end as service,
                            count(btr.id),
                            COUNT(CASE 
                                    WHEN btr.bank_swift_code = 'CIBLBDDH' and extract(epoch from (btr.request_time - btr.issue_time)) <= 60 * 5 THEN 1 
                                    WHEN btr.bank_swift_code <> 'CIBLBDDH' and extract(epoch from (btr.request_time - btr.issue_time)) <= 60 * 60 * 24 * 3 THEN 1 
                                  END) AS satisfactory_txn,
                           ROUND(
                                (COUNT(CASE 
                                         WHEN btr.bank_swift_code = 'CIBLBDDH' and extract(epoch from (btr.request_time - btr.issue_time)) <= 60 * 5 THEN 1 
                                         WHEN btr.bank_swift_code <> 'CIBLBDDH' and extract(epoch from (btr.request_time - btr.issue_time)) <= 60 * 60 * 24 * 3 THEN 1 
                                       END) * 100.0) / COUNT(btr.id), 2
                            ) AS customer_satisfaction
                        from
                            backend_db.public.bank_txn_request btr
                        left join backend_db.public.np_txn_log ntl 
                        on
                            btr.np_txn_log_id = ntl.id
                        where
                            1 = 1
                            and (btr.issue_time + interval '6 hours')::date >= '{dates[0]}'
                            and (btr.issue_time + interval '6 hours')::date < '{dates[1]}'
                            and btr.txn_request_type = 'CASH_OUT'
                            and btr.status in ('SUCCESS','SUCCESS','FAILED')
                        group by
                            1
                        ;'''

    recharge = f'''select
                        'Recharge' as service,
                        count(id),
                        COUNT(CASE 
                                WHEN extract(epoch from (update_date - create_date)) <= 350 THEN 1 
                              END) AS count_within_60_seconds,
                       ROUND(
                            (COUNT(CASE 
                                     WHEN extract(epoch from (update_date - create_date)) <= 350 THEN 1 
                                   END) * 100.0) / COUNT(id), 2
                        ) AS customer_satisfaction
                    from
                        topup_service.public.top_up_info tui
                    where
                        1 = 1
                        and tui.create_date::date >= '{dates[0]}'
                        and tui.create_date::date < '{dates[1]}'
                    ;'''

    money_in_NGD = f'''select
                            'Nagad IN' as service,
                            count(id),
                            COUNT(CASE 
                                    WHEN extract(epoch from (update_date - create_date)) <= 60 * 5 THEN 1 
                                  END) AS satisfactory_transaction,
                           ROUND(
                                (COUNT(CASE 
                                         WHEN extract(epoch from (update_date - create_date)) <= 60 * 5 THEN 1 
                                       END) * 100.0) / COUNT(id), 2
                            ) AS customer_satisfaction
                        from
                            nobopay_payment_gw.public.nagad_txn nt
                        where
                            1 = 1
                            and nt.create_date::date >= '{dates[0]}'
                            and nt.create_date::date < '{dates[1]}'
                            and nt.status in ('FAILED', 'SUCCESS')
                        ;'''

    money_in_RCKT = f'''select
                            'Rocket IN' as service,
                            count(id),
                            COUNT(CASE 
                                    WHEN extract(epoch from (update_date - create_date)) <= 60 * 5 THEN 1 
                                  END) AS satisfactory_transaction,
                           ROUND(
                                (COUNT(CASE 
                                         WHEN extract(epoch from (update_date - create_date)) <= 60 * 5 THEN 1 
                                       END) * 100.0) / COUNT(id), 2
                            ) AS customer_satisfaction
                        from
                            nobopay_payment_gw.public.dbbl_transaction dt
                        where
                            1 = 1
                            and dt.create_date::date >= '{dates[0]}'
                            and dt.create_date::date < '{dates[1]}'
                            and dt.status in ('FAILED', 'SUCCESS')
                        ;'''

    money_in_CRD = f'''select
                            'Card In' as service,
                            count(id),
                            COUNT(CASE 
                                    WHEN extract(epoch from (update_date - create_date)) <= 60 * 5 THEN 1 
                                  END) AS satisfactory_transaction,
                           ROUND(
                                (COUNT(CASE 
                                         WHEN extract(epoch from (update_date - create_date)) <= 60 * 5 THEN 1 
                                       END) * 100.0) / COUNT(id), 2
                            ) AS customer_satisfaction
                        from
                            nobopay_payment_gw.public.payment_info pi2
                        where
                            1 = 1
                            and pi2.create_date::date >= '{dates[0]}'
                            and pi2.create_date::date < '{dates[1]}'
                            and pi2.status in ('FAILED', 'SUCCESS')
                        ;'''

    SQR = f'''select
                    'Super QR Payment' as service,
                    count(case 
                            when response ilike '%{{%NPSB transfer credit%}}%' then 1 
                            when response ilike '%User profile not found%' then 1
                            when response ilike '%Failed due to Amount falls within fail rate%' then 1
                            when response ilike '%timeout%' then 1
                            when response ilike '%Something went wrong%' then 1
                        end) as total_attempt,
                    count(case when response ilike '%{{%NPSB transfer credit%}}%' then 1 end) as satisfactory_txn,
                    round(count(case when response ilike '%{{%NPSB transfer credit%}}%' then 1 end)::numeric / 
                          count(case 
                            when response ilike '%{{%NPSB transfer credit%}}%' then 1 
                            when response ilike '%User profile not found%' then 1
                            when response ilike '%Failed due to Amount falls within fail rate%' then 1
                            when response ilike '%timeout%' then 1
                            when response ilike '%Something went wrong%' then 1
                          end)::numeric * 100, 2) as satisfaction_level
                from
                    tallypay_issuer.public.request_log rl
                where
                    rl.create_date::date >= '{dates[0]}'
                    and rl.create_date::date < '{dates[1]}'
                    and request_id is not null
                    and request not ilike '%hex%'
                group by
                    1
                ;'''

    query_li = {
        'money_out_FI': money_out_FI,
        'money_out_BANK': money_out_BANK,
        'recharge': recharge,
        'money_in_NGD': money_in_NGD,
        'money_in_RCKT': money_in_RCKT,
        'money_in_CRD': money_in_CRD,
        'SQR': SQR
    }

    return query_li[query]


def select_db(db: str, query: str):
    conn = None
    cur = None

    try:
        # establishing the connection
        conn = psycopg2.connect(
            # ---Please change the credentials---
            database=db,
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
    except Exception as query_error:
        queryset = f"Error executing query: {query_error}"
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    # #print(queryset)
    return queryset


def gen_sum_row(serv_name: str, df):
    total_attempt = df['Total Attempt'].sum()
    satisfactory_transactions = df['Satisfactory Transactions'].sum()
    satisfaction_level = round((satisfactory_transactions / total_attempt * 100), 2)

    total_row = pd.DataFrame([[serv_name, total_attempt, satisfactory_transactions, satisfaction_level]],
                             columns=df.columns)

    return total_row


def gen_measure():
    data = {
        "Service": [
            "Money-In",
            "Money-Out",
            "Mobile Recharge",
            "Super QR payment"
        ],
        "Measure": [
            "Money credited to TallyPay users in 5 minutes",
            "CBS - Money sent to CBL within 5 minutes, EFT - Money sent to EFT within 3 days, MFS - Money sent to External FIs in 1 minutes",
            "Mobile recharge terminal state in 1 minute",
            "Successful requests are instantaneously received as such whether the attempt was successful or failed served as criteria"
        ]
    }

    measure_df = pd.DataFrame(data)

    return measure_df


def mak_bold(serv_name: str, html_table) -> str:
    '''the function bolds the row of the serv_name'''

    lines = html_table.split('\n')
    for i, line in enumerate(lines):
        if f'>{serv_name}<' in line:
            lines[i] = '<tr style="font-weight: bold;">' + line.split('<tr>')[-1]
    html_table = '\n'.join(lines)

    return html_table


def gen_chart(dates: List[str], df):
    chart_df = df.copy()
    chart_df.set_index('Service', inplace=True)

    plt.figure(figsize=(10, 6))

    ax1 = sns.barplot(data=chart_df, x=chart_df.index, y='Satisfaction Level', color='green',
                      label='Satisfaction Level')
    ax1.set_xlabel('Service')
    ax1.set_ylabel('Satisfaction Percentage')

    # Create a secondary y-axis
    ax2 = ax1.twinx()

    # Plot Total Attempt as a line
    ax2.plot(chart_df.index, chart_df['Total Attempt'], marker='o', color='blue', linewidth=2, label='Total Attempt')

    ax2.set_ylabel('Count')
    ax1.set_title('Total Attempt vs Satisfaction Level by Service')

    ax1.tick_params(axis='x', rotation=45)

    # Get handles and labels for both axes
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    handles = handles1 + handles2
    labels = labels1 + labels2
    ax1.legend(handles, labels, loc='upper left', bbox_to_anchor=(1.05, 1), borderaxespad=0.)

    plt.tight_layout()

    chart_img_path = f'chart_{dates[2]}.png'
    plt.savefig(chart_img_path)
    plt.close()

    return chart_img_path


def send_mail(dates: List[str], html_table_code, chart_img_path=None, csv_string=None):
    sender_email = os.environ.get('MAIL_SENDER_ADD')
    sender_pass = os.environ.get('MAIL_SENDER_PASS')

    receiver_email = 'product_eng@tallykhata.com'
    cc_email = ['amyou@tallykhata.com', 'mahidul@tallykhata.com', 'irfan.ahmed@tallykhata.com', 'tech_ops@surecash.net']

    # receiver_email = 'irfan.ahmed@tallykhata.com'
    # cc_email = ['overlodahmed.irfan@gmail.com']

    smtp_server = "smtp.gmail.com"  # for Gmail
    port = 587
    text1 = f'''Dear Team,\nPlease find the service summary from <strong>{dates[0]}</strong> to <strong>{dates[1]}</strong>'''
    text2 = f'''\n\nBest regards,\nIrfan Ahmed\nEngineer, Product Engineering\nGenerated on: {dates[2]}'''
    text3 = html_table_code

    msg = MIMEMultipart()
    msg["Subject"] = f'[PE Report] Service Summary {dates[0]} to {dates[1]}'
    msg["From"] = sender_email
    msg['To'] = receiver_email
    msg['Cc'] = ', '.join(cc_email)

    if chart_img_path:
        with open(chart_img_path, 'rb') as fp:
            img_data = fp.read()
        img_cid = f'chart_{dates[2]}.png'
        part = MIMEApplication(img_data, Name=img_cid)
        part['Content-Disposition'] = f'inline; filename="{img_cid}"'
        part['Content-ID'] = f'<{img_cid}>'
        msg.attach(part)
        text3 += f'<img src="cid:{img_cid}" alt="Chart">'

    if csv_string:
        part = MIMEApplication(csv_string.encode(), Name=f'Service Summary-{dates[2]}.csv')
        part['Content-Disposition'] = f'attachment; filename="Service Summary-{dates[2]}.csv"'
        msg.attach(part)
        text2 = '''\nPlease find the file attached below with the details.''' + text2
    else:
        pass
    #         text2 = '''\nNo attachment''' + text2

    msg.attach(MIMEText(text1, 'html'))
    msg.attach(MIMEText(text3, 'html'))
    msg.attach(MIMEText(text2, 'plain'))

    context = ssl.create_default_context()
    # Try to log in to server and send email
    try:
        server = smtplib.SMTP(smtp_server, port)
        server.ehlo()  # check connection
        server.starttls(context=context)  # Secure the connection
        server.ehlo()  # check connection
        server.login(sender_email, sender_pass)

        # Send email here
        server.sendmail(sender_email, [receiver_email] + cc_email, msg.as_string())
        #print('Sent email Successfully')
        return ('Success')

    except Exception as e:
        # #print any error messages
        #print("Error is: " + str(e))
        return "Error is: " + str(e)
    finally:
        server.quit()


def main():
    dates = get_week_dates()

    money_out_FI = gen_q(dates, 'money_out_FI')
    money_out_FI_r = select_db('tallypay_to_fi_integration', money_out_FI)
    mo_fi = pd.DataFrame(data=money_out_FI_r,
                         columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    money_out_BANK = gen_q(dates, 'money_out_BANK')
    money_out_BANK_r = select_db('backend_db', money_out_BANK)
    mo_b = pd.DataFrame(data=money_out_BANK_r,
                        columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    mo_df = pd.concat([mo_fi, mo_b], ignore_index=True)
    t_mo_df = gen_sum_row('Money Out', mo_df)

    recharge = gen_q(dates, 'recharge')
    recharge_r = select_db('topup_service', recharge)
    recharge_df = pd.DataFrame(data=recharge_r,
                               columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    serv_df = pd.concat([mo_df, recharge_df], ignore_index=True)
    sum_df = pd.concat([t_mo_df, recharge_df], ignore_index=True)

    mi_ngd = gen_q(dates, 'money_in_NGD')
    mi_ngd_r = select_db('nobopay_payment_gw', mi_ngd)
    mi_ngd_df = pd.DataFrame(data=mi_ngd_r,
                             columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    mi_rckt = gen_q(dates, 'money_in_RCKT')
    mi_rckt_r = select_db('nobopay_payment_gw', mi_rckt)
    mi_rckt_df = pd.DataFrame(data=mi_rckt_r,
                              columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    mi_df = pd.concat([mi_ngd_df, mi_rckt_df], ignore_index=True)

    mi_crd = gen_q(dates, 'money_in_CRD')
    mi_crd_r = select_db('nobopay_payment_gw', mi_crd)
    mi_crd_df = pd.DataFrame(data=mi_crd_r,
                             columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    mi_df = pd.concat([mi_df, mi_crd_df], ignore_index=True)
    t_mi_df = gen_sum_row('Money In', mi_df)

    serv_df = pd.concat([serv_df, mi_df], ignore_index=True)
    sum_df = pd.concat([sum_df, t_mi_df], ignore_index=True)

    mi_sqr = gen_q(dates, 'SQR')
    mi_sqr_r = select_db('tallypay_issuer', mi_sqr)
    mi_sqr_df = pd.DataFrame(data=mi_sqr_r,
                             columns=['Service', 'Total Attempt', 'Satisfactory Transactions', 'Satisfaction Level'])

    serv_df = pd.concat([serv_df, mi_sqr_df], ignore_index=True)
    #     t_serv_df = gen_sum_row('Total', serv_df)
    #     serv_df = pd.concat([serv_df, t_serv_df], ignore_index=True)

    sum_df = pd.concat([sum_df, mi_sqr_df], ignore_index=True)
    t_sum_df = gen_sum_row('Total', sum_df)
    sum_df = pd.concat([sum_df, t_sum_df], ignore_index=True)

    serv_df_html_table = build_table(serv_df, 'red_dark')
    sum_df_html_table = build_table(sum_df, 'red_dark')
    measure_df_html_table = build_table(gen_measure(), 'green_dark')

    #     serv_df_html_table = mak_bold('Total', serv_df_html_table)
    sum_df_html_table = mak_bold('Total', sum_df_html_table)

    serv_df_html_table = '<h2>Service Breakdown</h2>' + serv_df_html_table + '<br>'
    measure_df_html_table = '<h2>How is the satisfactory level measured?</h2>' + measure_df_html_table + '<br>'
    sum_df_html_table = '<h2>Service Summary</h2>' + sum_df_html_table + '<br>'

    fin_html_table = sum_df_html_table + measure_df_html_table + serv_df_html_table
    img_path = gen_chart(dates, serv_df)

    send_mail(dates, fin_html_table, img_path)

    os.remove(img_path)


main()
