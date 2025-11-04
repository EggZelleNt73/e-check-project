from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io, os, glob

def download_files_func():
    # Configuration
    SERVICE_ACCOUNT_FILE = glob.glob("/opt/spark/google_auth/e-checks-project*.json")[0]
    FOLDER_ID = "1uW77S7QK2xsk-M6urShuGy2HqTpnKykd"
    LOCAL_DIR = "/opt/spark/source_data"

    # Authenticate
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive"]
    )

    drive_service = build("drive", "v3", credentials=creds)

    # List all CSV files in the folder
    query = f"'{FOLDER_ID}' in parents and (mimeType='text/csv' or mimeType='application/json') and trashed=false" 
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get("files", [])

    if not files:
        print("No new files found on drive")
    else:
        # Download files
        for file in files:
            file_id = file["id"]
            file_name = file["name"]
            local_path = os.path.join(LOCAL_DIR, file_name)

            print(f"Downloading: {file_name} ...")

            request = drive_service.files().get_media(fileId=file_id)
            fh = io.FileIO(local_path, "wb")

            downloader = MediaIoBaseDownload(fh, request)
            done = False

            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(f"Progress: {int(status.progress() * 100)}%")
            
            fh.close()
            print(f"Downloaded: {file_name}")