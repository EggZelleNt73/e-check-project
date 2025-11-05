from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io, os, glob
from utils.logger_setup import get_logger

logger = get_logger(__name__)

def download_files_func():
    # Configuration
    SERVICE_ACCOUNT_FILE = ("/opt/spark/google_auth/authentication.json")
    FOLDER_ID = "1uW77S7QK2xsk-M6urShuGy2HqTpnKykd"
    LOCAL_DIR = "/opt/spark/source_data"
    CSV_DIR = os.path.join(LOCAL_DIR, "csv_files")
    JSON_DIR = os.path.join(LOCAL_DIR, "json_files")

    os.makedirs(CSV_DIR, exist_ok=True)
    os.makedirs(JSON_DIR, exist_ok=True)

    # Authenticate
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    drive_service = build("drive", "v3", credentials=creds)

    # List all CSV files in the folder
    query = f"'{FOLDER_ID}' in parents and (mimeType='text/csv' or mimeType='application/json') and trashed=false" 
    results = drive_service.files().list(q=query, fields="files(id, name, mimeType)").execute()
    files = results.get("files", [])

    if not files:
        logger.warning("No new files found in google drive")
    else:
        # Download files
        logger.info("Loading files from google drive")
        for file in files:
            file_id = file["id"]
            file_name = file["name"]
            mime_type = file["mimeType"]

            if mime_type == "text/csv" or file_name.lower().endswith(".csv"):
                local_path = os.path.join(CSV_DIR, file_name)
            elif mime_type == "application/json" or file_name.lower().endswith(".json"):
                local_path = os.path.join(JSON_DIR, file_name)
            else:
                logger.warning(f"Skipping usupported file type: {file_name}")
                continue

            logger.info(f"Downloading: {file_name} ...")
            request = drive_service.files().get_media(fileId=file_id)
            with io.FileIO(local_path, "wb") as fh:
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while not done:
                    done = downloader.next_chunk()
            
                logger.info(f"Downloaded: {file_name}")
    
        logger.info("All files have been downloaded to the server")

if __name__ == "__main__":
    download_files_func()