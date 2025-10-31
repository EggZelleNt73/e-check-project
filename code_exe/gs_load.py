import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
from glob import glob

def google_sheet_loader():
    csv_file = glob(f"/opt/bitnami/spark/result_csv/test_run_1/part-*.csv")[0]
    pdf = pd.read_csv(csv_file)

    # Authenticate
    scope = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]

    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        '/opt/bitnami/spark/google_auth/e-checks-project-0660066a503b.json', scope
    )

    gc = gspread.authorize(credentials)

    # Open the Google Sheet
    spreadsheet = gc.open("My_balance")
    worksheet = spreadsheet.worksheet("Products")

    # Write the DataFrame to the sheet
    worksheet.append_rows(pdf.values.tolist(), value_input_option="USER_ENTERED")