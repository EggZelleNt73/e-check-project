import csv, os
from utils.logger_setup import get_logger
from google.oauth2 import service_account
from googleapiclient.discovery import build

def load_to_google_sheet():
    """Append CSV rows (without header) to an existing Google Sheet."""
    logger = get_logger(__name__)

    CREDENTIALS_FILE = "/opt/spark/google_auth/authentication.json"
    CSV_DIR = "/opt/spark/sink_data/csv_file"
    SHEET_ID = "1sCVUdbZWuQhwpwDj5WG-I3dJZVRBuZHH21zjKu2XxH0"    
    SHEET_NAME = "test"
    BATCH_SIZE = 500

    csv_path = [f for f in os.listdir(CSV_DIR) if f.endswith(".csv")]
    if not csv_path:
        logger.warning("No csv files found to upload")
    
    csv_files = os.path.join(CSV_DIR, csv_path[0])
    
    logger.info("Start to upload file to google sheet...")
    try:
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = service_account.Credentials.from_service_account_file(
            CREDENTIALS_FILE, scopes=SCOPES
        )

        service = build("sheets", "v4", credentials=creds)

        # read csv file
        with open(csv_files, "r") as f:
            reader = csv.reader(f)
            next(reader, None) # skip header
            batch = []
            total_uploaded = 0

            for i, row in enumerate(reader, start=1):
                if not any(row):
                    continue
                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    service.spreadsheets().values().append(
                        spreadsheetId=SHEET_ID,
                        range=f"{SHEET_NAME}!A1",
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={"values": batch}
                    ).execute()

                    logger.info(f"Uploaded {total_uploaded} rows...")
                    batch.clear()

                total_uploaded += len(batch)
            
            if batch:
                service.spreadsheets().values().append(
                        spreadsheetId=SHEET_ID,
                        range=f"{SHEET_NAME}!A1",
                        valueInputOption="USER_ENTERED",
                        insertDataOption="INSERT_ROWS",
                        body={"values": batch}
                    ).execute()
    except Exception as e:
        logger.error(f"Couldn't load data to google sheet: {e}" )
    else:
        logger.info(f"Finished uploading rows to {SHEET_NAME} sheet")

if __name__ == "__main__":
    load_to_google_sheet()