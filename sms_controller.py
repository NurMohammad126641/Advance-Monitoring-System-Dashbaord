import os
from dotenv import load_dotenv

import mysql.connector
import traceback
from isheet_controller import sheet_update

load_dotenv()


def gen_q(q):
    
    hour_q = '''SELECT
                    HOUR(request_time) AS hour_column,
                    channel,
                    telecom_operator,
                    CASE
                        WHEN message_status = '0' or message_status = 'SUCCESS' THEN 'SUCCESS'
                        ELSE message_status 
                    END AS status,
                    COUNT(1) as count
                FROM
                    sc_sms_prod.message_v2 mv
                WHERE
                    1 = 1
                    AND request_time >= CURDATE()
                    AND request_time <= NOW() - INTERVAL 5 minute
                    and channel in ('TALLYKHATA_OTP', 'TALLYKHATA_TXN', 'TALLYPAY_OTP', 'TALLYPAY_TXN')
                GROUP BY
                    HOUR(request_time),
                    channel,
                    telecom_operator,
                    message_status
                ORDER BY
                    hour_column DESC
                ;'''
    
    month_q = '''SELECT
                    DATE_FORMAT(request_time, '%Y-%m-%d') as date_column,
                    channel,
                    telecom_operator,
                    CASE
                        WHEN message_status = '0' THEN 'SUCCESS'
                        WHEN message_status = 'SUCCESS' THEN 'SUCCESS'
                        ELSE message_status
                    END AS 
                    status,
                    COUNT(1) as count
                FROM
                    sc_sms_prod.message_v2 mv
                WHERE
                    1=1
                    AND request_time >= DATE_ADD(NOW(), INTERVAL -1 MONTH)
                    and channel in ('TALLYKHATA_OTP', 'TALLYKHATA_TXN', 'TALLYPAY_OTP', 'TALLYPAY_TXN')
                GROUP BY
                    date_column,
                    channel,
                    telecom_operator,
                    message_status
                ORDER BY
                    date_column ASC;'''
    
    query= {
        "hour_q": hour_q,
        "month_q": month_q
        }
    
    return query[q]
    
    
def mysql_conn(query):
    #print('Establishing connection to mySQL')

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
        
        #print('Connection established')
        cur = conn.cursor()
        #print('Executing query')
        cur.execute(query)
        #print('Retreiving queryset')
        queryset = cur.fetchall()
    except mysql.connector.Error as error:
        queryset = f"Error connecting to MySQL database: {error}"
        #print(queryset)
    finally:
        if cur == None or conn == None:
            cur.close()
            conn.close()
    
    ##print(queryset)
    return queryset


def sms_main():
    
    #print('Initiating sms controller...')
    
    q = gen_q('hour_q')
    q_r = mysql_conn(q)
    sms_hr = [('hour_column', 'channel', 'telecom_operator', 'status', 'count')] + q_r
    
    q = gen_q('month_q')
    q_r = mysql_conn(q)
    sms_m = [('date_column', 'channel', 'telecom_operator', 'status', 'count')] + q_r
    
    sheet_update(sms_hr, 'tksms_curday_hr_cohort')
    sheet_update(sms_m, 'tksms_30day_day_cohort')

    #print('sms controller terminating...')
    

# sms_main()
    