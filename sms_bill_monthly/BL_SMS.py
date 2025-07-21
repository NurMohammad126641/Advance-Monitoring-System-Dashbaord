import mysql.connector
from pretty_html_table import build_table
import pandas as pd
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv


load_dotenv()
DEBUG = False

def find_last_month_name():
    # Get the current date
    today = datetime.today()

    # Calculate the first day of the current month
    first_day_of_current_month = today.replace(day=1)

    # Calculate the last day of the previous month
    last_day_of_last_month = first_day_of_current_month - timedelta(days=1)

    # Get the name of the last month
    last_month_name = last_day_of_last_month.strftime('%B')
    return last_month_name


def return_time():
    timeframe = []

    curM = datetime.now().month
    curY = datetime.now().year
    lastM = curM - 1

    start_T = f'{curY}-{str(lastM).zfill(2)}-01'
    end_T = f'{curY}-{str(curM).zfill(2)}-01'

    timeframe = [start_T, end_T]

    return timeframe


def gen_query(timeframe):
    sms_query = f'''
                SELECT
                    channel,
                    sms_count,
                    sms_count * 0.28 AS bill
                FROM
                    (
                    SELECT
                        channel, 
                        SUM(CASE
                                WHEN channel in ('TALLYKHATA_TXN', 'TALLYPAY_TXN') AND CHAR_LENGTH(message_body) > 268 THEN 5
                                WHEN channel in ('TALLYKHATA_TXN', 'TALLYPAY_TXN') AND CHAR_LENGTH(message_body) > 201 THEN 4
                                WHEN channel in ('TALLYKHATA_TXN', 'TALLYPAY_TXN') AND CHAR_LENGTH(message_body) > 134 THEN 3
                                WHEN channel in ('TALLYKHATA_TXN', 'TALLYPAY_TXN') AND CHAR_LENGTH(message_body) > 70 THEN 2
                                WHEN channel in ('TALLYKHATA_OTP', 'TALLYPAY_OTP') AND CHAR_LENGTH(message_body) > 765 THEN 6
                                WHEN channel in ('TALLYKHATA_OTP', 'TALLYPAY_OTP') AND CHAR_LENGTH(message_body) > 612 THEN 5
                                WHEN channel in ('TALLYKHATA_OTP', 'TALLYPAY_OTP') AND CHAR_LENGTH(message_body) > 459 THEN 4
                                WHEN channel in ('TALLYKHATA_OTP', 'TALLYPAY_OTP') AND CHAR_LENGTH(message_body) > 306 THEN 3
                                WHEN channel in ('TALLYKHATA_OTP', 'TALLYPAY_OTP') AND CHAR_LENGTH(message_body) > 160 THEN 2
                                ELSE 1
                              END) AS sms_count
                    FROM
                        sc_sms_prod.message_v2 mv
                    WHERE
                        1 = 1
                        AND mv.telco_identifier_id IN ('70','72','67')
                        AND mv.message_status IN ('0', 'SUCCESS')
                        and telecom_operator = 'BANGLALINK'
                        AND mv.request_time >= '{timeframe[0]}'
                        AND mv.request_time < '{timeframe[1]}'
                        and channel in ('TALLYKHATA_TXN', 'TALLYPAY_TXN', 'TALLYKHATA_OTP', 'TALLYPAY_OTP')
                        group by 1
                                  ) AS subquery;
                '''

    return sms_query


def mysql_conn(query):
    print('Establishing connection to mySQL')
    conn = None
    cur = None
    try:
        # establishing the connection
        conn = mysql.connector.connect(
            database='sc_sms_prod',
            user=os.environ.get("SMS_USR"),
            password=os.environ.get("SMS_PWD"),
            host=os.environ.get("SMS_HOST"),
            port='3306'
        )
        print('Connection established')
        cur = conn.cursor()
        print('Executing query')
        cur.execute(query)
        print('Retreiving queryset')
        queryset = cur.fetchall()
    except mysql.connector.Error as error:
        queryset = f"Error connecting to MySQL database: {error}"
        print(queryset)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return queryset


def send_mail(timeframe, html_table_code):
    if DEBUG:
        sender_email = os.environ.get('MAIL_SENDER_ADD')
        sender_pass = os.environ.get('MAIL_SENDER_PASS')
        receiver_email = 'tech_ops@surecash.net'
        cc_email = ['mahidul@tallykhata.com', 'irfan.ahmed@tallykhata.com']

    else:
        sender_email = os.environ.get('MAIL_SENDER_ADD')
        sender_pass = os.environ.get('MAIL_SENDER_PASS')
        receiver_email = 'amin@tallykhata.com'
        cc_email = ['mahidul@tallykhata.com', 'amyou@tallykhata.com', 'product_eng@tallykhata.com ', 'tech_ops@surecash.net', 'finance@tallykhata.com']

    smtp_server = "smtp.gmail.com"  # for Gmail
    port = 587
    last_month = find_last_month_name()
    text1 = f'''To Whom It May Concern, \nKindly get the SMS count and bill for BANGLALINK of {last_month}, {datetime.now().year}. According to our database. Please keep in mind that we are using an assumed cost per message of 0.28 tk. The starting timeframe is {timeframe[0]} and ends at excluding {timeframe[1]}
    '''
    text2 = f'''\n\nBest regards,\nProduct Engineering Team\nGenerated on: {datetime.now().strftime("%A, %B %d, %y %H:%M:%S")}'''
    text3 = html_table_code
    text4 =  f'''\nThis email has been automatically generated by our system. Please thoroughly review the data before submitting. If you have any concerns, please reach out to our TechOps team for assistance.'''

    msg = MIMEMultipart()
    msg["Subject"] = f'Summary Bill Month of {last_month},{datetime.now().year}___PROGOTI SYSTEMS LIMITED [Banglalink SMS]'
    msg["From"] = sender_email
    msg['To'] = receiver_email
    msg['Cc'] = ', '.join(cc_email)

    msg.attach(MIMEText(text1, 'plain'))
    msg.attach(MIMEText(text3, 'html'))
    msg.attach(MIMEText(text4, 'plain'))
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
        print('Sent email Successfully')
        return ('Success')

    except Exception as e:
        # Print any error messages
        print("Error is: " + str(e))
        return "Error is: " + str(e)
    finally:
        server.quit()


def main():
    timeframe = return_time()
    print(timeframe)

    sms_q = gen_query(timeframe)
    print(sms_q)

    sms_r = mysql_conn(sms_q)
    print(sms_r)

    data_df = pd.DataFrame(data=sms_r, columns=['Channel', 'Estimated DB Count', 'Estimated DB Bill'])

    # Calculate the sums
    total_row = {
        'Channel': 'Total',
        'Estimated DB Count': data_df['Estimated DB Count'].sum(),
        'Estimated DB Bill': data_df['Estimated DB Bill'].sum()
    }

    # Convert the total_row dictionary to a DataFrame
    total_df = pd.DataFrame([total_row])

    # Concatenate the total_df with the original data_df
    data_df = pd.concat([data_df, total_df], ignore_index=True)

    data_df['Estimated DB Count'] = data_df['Estimated DB Count'].apply(lambda x: '{:,.2f}'.format(x))
    data_df['Estimated DB Bill'] = data_df['Estimated DB Bill'].apply(lambda x: '{:,.2f}'.format(x))
    mail2send = build_table(data_df, 'red_dark')

    send_mail(timeframe, mail2send)

    print('Execution complete. Terminating...')


main()
