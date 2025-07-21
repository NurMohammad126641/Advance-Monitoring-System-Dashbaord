import requests
from isheet_controller import sheet_update

def fetch_and_update_ssl_balance():
    # API endpoint
    url = "https://common-api.sslwireless.com/api/vr/topup-balance"

    # Headers for the API request
    headers = {
        'STK-CODE': 'TallyKhata',
        'AUTH-KEY': 'OXvznaFvubq8FSAh1OwbAeoTr8907UhL'
    }

    try:
        # Make the GET request
        response = requests.get(url, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            print("API Output:")
            data = response.json()
            print(data)  # Print the JSON response

            # Extract and format the available credit
            available_credit = data.get('data', {}).get('available_credit', None)

            if available_credit:
                # Format the balance with commas and two decimal places
                formatted_balance = "{:,}".format(int(float(available_credit)))

                # Prepare the data with header and formatted balance
                sheet_name = 'ssl_balance'  # Replace with your Google Sheet name
                sheet_data = [["SSL Balance"], [formatted_balance]]

                # Update the Google Sheet
                sheet_update(sheet_data, sheet_name)
                print(f"Updated Google Sheet with formatted SSL balance: {formatted_balance}")
            else:
                print("Available credit not found in the API response.")
        else:
            print(f"Failed to fetch data. Status Code: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_and_update_ssl_balance()
