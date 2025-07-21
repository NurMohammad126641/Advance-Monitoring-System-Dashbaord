from datetime import date
from datetime import datetime, timedelta
import pandas as pd
import warnings
import mysql.connector
import matplotlib.pyplot as plt
from email.mime.image import MIMEImage
import numpy as np
from email.mime.text import MIMEText
import smtplib
from email.mime.multipart import MIMEMultipart
import math
from isheet_controller import sheet_update
from dotenv import load_dotenv
import os

DEBUG = False

today = str(date.today())
load_dotenv()

def db_connection_and_result_set(host, database, user, password, conn, cur):
    try:
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )

        cur = conn.cursor()

        query = f'''
                                SELECT
                    j.id,
                    i.id,
                    j.journalized_type,
                    j.user_id,
                    j.notes,
                    i.start_date,
                    j.private_notes,
                    i.subject,
                    ea.address,
                    te.hours 
                FROM
                    issues i
                Left join journals j 
                on
                    i.id = j.journalized_id
                INNER JOIN email_addresses ea
                                            ON
                    ea.user_id = i.author_id
                INNER JOIN issue_statuses is2 
                                            ON
                    i.status_id = is2.id
                Inner JOIN time_entries te 
                on
                    te.issue_id = i.id
                WHERE
                    1 = 1
                    AND i.author_id in ('44', '45', '59', '6', '41')
                    AND DATE(i.start_date) >= CURDATE() - INTERVAL 30 DAY
                   ;

              '''

        df = pd.read_sql(query, con=conn)
        data_list = df.values.tolist()
        #print(len(data_list))

        return data_list

    except Exception as error:
        pass
        #print("Something wents wrong: ", error)

    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def db_result_dividor(db_result):
    parent_list = []
    irfan_bhai_list = ["Irfan Bhai"]
    jannat_apu_list = ["Jannat Apu"]
    javed_bhai_list = ["Javed Bhai"]
    shilton_bhai_list = ["Shilton Bhai"]
    afzal_list = ["Afzal"]

    for i in db_result:
        if 'irfan.ahmed' in i[8]:
            irfan_bhai_list.append(i)
        elif 'jannat.akter' in i[8]:
            jannat_apu_list.append(i)
        elif 'javeed' in i[8]:
            javed_bhai_list.append(i)
        elif 'shilton.saha' in i[8]:
            shilton_bhai_list.append(i)
        elif 'afzal' in i[8]:
            afzal_list.append(i)
    length_list = [len(javed_bhai_list) - 1, len(shilton_bhai_list) - 1, len(irfan_bhai_list) - 1,
                   len(jannat_apu_list) - 1, len(afzal_list) - 1]
    if len(javed_bhai_list) > 1:
        parent_list.append(javed_bhai_list)

    if len(shilton_bhai_list) > 1:
        parent_list.append(shilton_bhai_list)

    if len(irfan_bhai_list) > 1:
        parent_list.append(irfan_bhai_list)

    if len(jannat_apu_list) > 1:
        parent_list.append(jannat_apu_list)

    if len(afzal_list) > 1:
        parent_list.append(afzal_list)
    parent_list.append(length_list)
    return parent_list


def distinct_db_result(db_result):
    list1 = []
    for i in db_result:
        flag = False
        for j in list1:
            if i[1] == j[1]:
                flag = True
                break
        if flag == False:
            list1.append(i)
    return list1


def current_day_list(dist_db_result):
    today = datetime.today().date()
    cur_day = today.strftime('%Y-%m-%d')
    list1 = []
    for i in dist_db_result:
        timestamp = i[5]
        date_string = timestamp.strftime('%Y-%m-%d')
        if date_string == cur_day:
            list1.append(i)
    return list1


def thirty_days_list():
    today = datetime.today().date()

    date_strings = []

    for i in range(30, -1, -1):
        date = today - timedelta(days=i)
        date_string = date.strftime('%Y-%m-%d')
        date_strings.append(date_string)

    return date_strings


def date_wise_count(date_list, db_list):
    irfan_bhai_list = []
    jannat_apu_list = []
    afzal_list = []
    javeed_bhai_list = []
    shilton_bhai_list = []
    for date in date_list:
        irfan_bhai = 0
        jannat_apu = 0
        afzal = 0
        javeed_bhai = 0
        shilton_bhai = 0
        for ticket in db_list:
            timestamp = ticket[5]
            hour = float(ticket[9])
            date_string = timestamp.strftime('%Y-%m-%d')
            if date == date_string and "irfan" in ticket[8]:
                irfan_bhai += hour
            elif date == date_string and "jannat" in ticket[8]:
                jannat_apu += hour
            elif date == date_string and "afzal" in ticket[8]:
                afzal += hour
            elif date == date_string and "javeed" in ticket[8]:
                javeed_bhai += hour
            elif date == date_string and "shilton" in ticket[8]:
                shilton_bhai += hour
        irfan_bhai_list.append(math.ceil(irfan_bhai))
        jannat_apu_list.append(math.ceil(jannat_apu))
        afzal_list.append(math.ceil(afzal))
        javeed_bhai_list.append(math.ceil(javeed_bhai))
        shilton_bhai_list.append(math.ceil(shilton_bhai))
    return [irfan_bhai_list, jannat_apu_list, javeed_bhai_list, shilton_bhai_list, afzal_list]


def build_sheet_data(date_list, individual_count):
    result = [("Date","User","Hourly Count")]
    #print(len(date_list))
    #print(len(individual_count[0]))
    for i in range(len(date_list)):
        child = []
        for j in range(len(individual_count)):
            name = ''
            if j == 0:
                name = "IRFAN"
            elif j == 1:
                name = "JANNAT"
            elif j == 2:
                name = 'JAVEED'
            elif j == 3:
                name = 'SHILTON'
            elif j == 4:
                name = 'AFZAL'

            child = (date_list[i], name, individual_count[j][i])
            result.append(child)
    #print(result)
    return result


def graph_builder(date_list, individual):
    date_list1 = []
    for i in date_list:
        date = i[5:7] + "\n" + i[8:] + "\n" + i[0:4]
        date_list1.append(date)

    x = date_list1
    y1 = np.array(individual[0])
    y2 = np.array(individual[1])
    y3 = np.array(individual[2])
    y4 = np.array(individual[3])
    y5 = np.array(individual[4])

    # plot bars in stack manner
    plt.figure(figsize=(9, 6))
    ax1 = plt.bar(x, y1, color='r')
    ax2 = plt.bar(x, y2, bottom=y1, color='b')
    ax3 = plt.bar(x, y3, bottom=y1 + y2, color='y')
    ax4 = plt.bar(x, y4, bottom=y1 + y2 + y3, color='g')
    ax5 = plt.bar(x, y5, bottom=y1 + y2 + y3 + y4, color='purple')
    plt.xlabel("Dates")
    plt.xticks(rotation=0, fontsize=6)
    plt.ylabel("Hour")

    for r1, r2, r3, r4, r5 in zip(ax1, ax2, ax3, ax4, ax5):
        h1 = r1.get_height()
        h2 = r2.get_height()
        h3 = r3.get_height()
        h4 = r4.get_height()
        h5 = r5.get_height()
        if h1 > 0:
            plt.text(r1.get_x() + r1.get_width() / 2., h1 / 2., "%d" % h1, ha="center", va="center", color="white",
                     fontsize=6)
        if h2 > 0:
            plt.text(r2.get_x() + r2.get_width() / 2., h1 + h2 / 2., "%d" % h2, ha="center", va="center", color="white",
                     fontsize=6)
        if h3 > 0:
            plt.text(r3.get_x() + r3.get_width() / 2., h1 + h2 + h3 / 2., "%d" % h3, ha="center", va="center",
                     color="white", fontsize=6)
        if h4 > 0:
            plt.text(r3.get_x() + r3.get_width() / 2., h1 + h2 + h3 + h4 / 2., "%d" % h4, ha="center", va="center",
                     color="white", fontsize=6)
        if h5 > 0:
            plt.text(r3.get_x() + r3.get_width() / 2., h1 + h2 + h3 + h4 + h5 / 2., "%d" % h5, ha="center", va="center",
                     color="white", fontsize=6)

    plt.legend(["Irfan Bhai", "Jannat Apu", "Javeed Bhai", "Shilton Bhai", "Afzal"])
    plt.title("Tech_Ops Operation Hourly Count")
    plt.savefig("graph.png")

    with open('graph.png', 'rb') as f:
        img_data = f.read()
    img = MIMEImage(img_data)
    img.add_header('Content-ID', '<graph>')

    return img


def build_pivot_table(data_list1, length_list):
    data_list = []
    for i in data_list1:

        subject = i[7].title()

        words = subject.split()

        # Join the words back together with a single space
        new_subject = ' '.join(words)
        new = [new_subject, i[8], 1, i[5]]
        if 'irfan.ahmed' in i[8]:
            new.append(length_list[2])
        elif 'jannat.akter' in i[8]:
            new.append(length_list[3])
        elif 'javeed' in i[8]:
            new.append(length_list[0])
        elif 'shilton.saha' in i[8]:
            new.append(length_list[1])
        elif 'afzal' in i[8]:
            new.append(length_list[4])
        data_list.append(new)

    output = pd.DataFrame(data_list, columns=["Subject", "Name", "Count", 'Date', "Total Resolved"])

    pivot_table = pd.pivot_table(output, values=["Count"], index=["Name", "Total Resolved", "Subject"], columns=[],
                                 aggfunc={'Count': 'sum', }, fill_value=0, margins=True,
                                 margins_name='Grand Total')

    column_widths = {
        'Subject': '200px',
        1: '100px',
        2: '150px',
        3: '100px',
        "Week": '200px',
        'Grand Total': '150px'

    }
    pivot_table = pivot_table.style.set_table_styles([{
        'selector': 'th, td',
        'props': [('max-width', width), ('width', width)]}
        for column, width in column_widths.items()
    ])

    summary_table = pivot_table.to_html(classes='alternate',
                                        header=True,
                                        index=True,
                                        justify='left',
                                        col_space=200,
                                        na_rep='',
                                        border=1)

    # Apply CSS styles to the HTML table
    summary_table = summary_table.replace('<table',
                                          '<table style="border-collapse: collapse; border: 2px solid black; text-align: left;"')
    summary_table = summary_table.replace('<thead>',
                                          '<thead style="border-bottom: 2px solid black;">')
    summary_table = summary_table.replace('<th ',
                                          '<th style="border-right: 1px solid black; padding: 5px;"')
    summary_table = summary_table.replace('<td ',
                                          '<td style="border-right: 1px solid black; padding: 5px;"')
    summary_table = summary_table.replace('<tr>',
                                          '<tr style="border-bottom: 1px solid black;">')

    return summary_table


def send_mail(sender, receiver, Cc, mail_pass, subject, pivot_table, img):
    mail_body = f'''
        Dear Concern,
        <br>
        <br>
        Greetings.
        <br><br>
        <b>Please see the Bar Chart of one month Tech_ops Operation summary:</b>
        <br>
        <img src="cid:graph">
        <br><br> 
        <br>
        <b>Please see the summary of Operational task of today:</b>
        <br><br>
        {pivot_table}
        <br>
        <br> 
        Thanks and Regards,
        <br>
        <b>TechOps</b>
        '''

    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = receiver
    message["Subject"] = subject
    message["Cc"] = ",".join(Cc)
    message.attach(img)
    html_part = MIMEText(mail_body, 'html')
    message.attach(html_part)

    with smtplib.SMTP('smtp.gmail.com', 587) as smtp:
        smtp.starttls()
        smtp.login(sender, mail_pass)
        smtp.send_message(message)
        smtp.close()

    #print("Mail Sent")


def tech_ops_operation_main():
    # DB_info
    host = "10.9.0.112"
    database = "redmine"
    user = "afzal"
    password = "Afz@l@T@cp$l!23"

    # mail_info
    mail_pass = os.environ.get("MAIL_SENDER_PASS")
    sender = os.environ.get("MAIL_SENDER_ADD")
    # receiver = "tech_ops@surecash.net"
    receiver = "tech_ops@surecash.net"
    Cc = ["mahidul@tallykhata.com"]
    subject = "Tech_Ops Operation Tracker From Dashboard (" + today + ")"
    warnings.filterwarnings("ignore")
    conn = None
    cur = None

    db_result = db_connection_and_result_set(host, database, user, password, conn, cur)
    #print(len(db_result))

    dist_db_result = distinct_db_result(db_result)
    #print(len(dist_db_result))
    current_day_ls = current_day_list(dist_db_result)
    parent_list = db_result_dividor(dist_db_result)
    length_list = parent_list[-1]
    one_day = db_result_dividor(current_day_ls)
    length_list1 = one_day[-1]
    date_list = thirty_days_list()
    individual_count = date_wise_count(date_list, dist_db_result)
    data_of_sheet = build_sheet_data(date_list, individual_count)
    sheet_update(data_of_sheet,"Operation_tracker")
    img = graph_builder(date_list, individual_count)
    pivot_table = build_pivot_table(current_day_ls, length_list1)

    send_mail(sender, receiver, Cc, mail_pass, subject, pivot_table, img)





