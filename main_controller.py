import traceback
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv
import os
from recharge_controller import recharge_main
from sms_controller import sms_main
from cash_in_rocket import cashin_rocket_main
from moneyout_controller import moneyout_main
from sqr_controller import sqr_main_threaded
from bankout_controller import bank_money_out
from registration_controller_v2 import sqr_reg
from daily_service_analysis import main_daily_service_analysis
from Visa_Card_Transfer import VISA_Transfers
from service_health_all_service import service_health
from recharge_new import recharge_new
from NPSB import NPSB
from porichoy import porichoy
from reconcilation import reconciliation_reports
from avg_4week import threshold_avg
from sqr_not_success import run_sqr_user_extraction
from recharge_cashback import recharge_cashbackk
from tk_premium import tk_premium
from nagad_money_in import nagad_money_in
from all_balance import all_balance
from ssl_balance import fetch_and_update_ssl_balance
from registration_controller import tp_reg
from tk_log import tallykhata_log

# Load environment variables from the .env file
load_dotenv()

EMAIL_USER = os.getenv("EMAIL_USER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", 587))
RECIPIENT_EMAIL = os.getenv("RECIPIENT_EMAIL")

# Dictionary to store the time taken for each function
timing_data = {}


def profile_function(func):
    """
    Measures the execution time of a function and stores it in the timing_data dictionary.
    """

    def wrapper():
        start_time = time.time()
        try:
            func()
            timing_data[func.__name__] = time.time() - start_time
            print(f"{func.__name__} completed successfully.")
        except Exception as e:
            timing_data[func.__name__] = "Failed"
            error_msg = traceback.format_exc()
            print(f"Error in {func.__name__}: {error_msg}")

    return wrapper


# Email sending function
def send_email(subject, body, to_email):
    msg = MIMEMultipart()
    msg['From'] = EMAIL_USER
    msg['To'] = to_email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_USER, EMAIL_PASSWORD)
            server.send_message(msg)
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")


# List of functions to be executed sequentially
functions_to_run = [
    profile_function(recharge_main),
    profile_function(nagad_money_in),
    profile_function(sms_main),
    profile_function(cashin_rocket_main),
    profile_function(moneyout_main),
    profile_function(sqr_main_threaded),
    profile_function(bank_money_out),
    profile_function(sqr_reg),
    profile_function(main_daily_service_analysis),
    profile_function(VISA_Transfers),
    profile_function(service_health),
    profile_function(recharge_new),
    profile_function(NPSB),
    profile_function(porichoy),
    profile_function(reconciliation_reports),
    profile_function(threshold_avg),
    profile_function(run_sqr_user_extraction),
    profile_function(recharge_cashbackk),
    profile_function(tk_premium),
    profile_function(all_balance),
    profile_function(fetch_and_update_ssl_balance),
    profile_function(tp_reg),
    profile_function(tallykhata_log)
]


def main():
    print('Main controller initiating...')

    # Run each function sequentially to ensure full completion
    for func in functions_to_run:
        func()

    # Prepare timing summary
    total_time_seconds = 0
    timing_summary = "\nTiming Summary:\n"
    for func_name, elapsed_time in timing_data.items():
        if isinstance(elapsed_time, float):
            timing_summary += f"{func_name} took {elapsed_time:.2f} seconds to run.\n"
            total_time_seconds += elapsed_time
        else:
            timing_summary += f"{func_name} failed to execute.\n"

    # Calculate total time in minutes
    total_time_minutes = total_time_seconds / 60
    timing_summary += f"\nTotal execution time: {total_time_minutes:.2f} minutes."

    print(timing_summary)

    # Send the timing summary via email
    # send_email("DASHBOARD Script Execution Timing Summary", timing_summary, RECIPIENT_EMAIL)


try:
    main()

except Exception as e:
    error_msg = traceback.format_exc()
    print("Error occurred: ", error_msg)
    # send_skype_msg(error_msg)
