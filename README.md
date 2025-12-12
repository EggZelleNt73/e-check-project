# Grocery Store E-Check Parser

A data-processing pipeline that parses e-check receipts from grocery stores (for now only Polish grocery stores), transforms the data using a Spark engine running in Docker, and loads structured results into a Google Sheet. Works on Linux Ubuntu

### THIS IS NOT FINAL RESULT AND MANY THINGS WILL BE CHANGED SO THAT IT WILL BE MORE USER-FRIENDLY TO USE !

- Currently supports:
  - Biedronka (JSON e-checks) — fully supported
  - Lidl (CSV e-checks) — requires separate n8n adjustment (will be uploaded soon)


## Tech Stack
- Python
- PySpark
- Docker / Docker Compose
- Bash
- Google Drive API 
- Google Sheets API


## Project Flow

1) User uploads e-check files to a designated Google Drive folder.
2) Program loads the files into the container.
3) PySpark transforms and standardizes the data.
4) The final structured table is appended to a Google Sheet.

## Installation & Setup

1. Clone the repository
<pre> git clone https://github.com/EggZelleNt73/e-check-project.git </pre>

2. Create required directories

Make sure the following folders exist in your project root:
<pre>
./google_auth/
./logs/
./source_data/csv_files/
./source_data/json_files/
./sink_data/csv_file/
</pre>

3. Add your environment variables

Create _.env_ file in project root and add following variables:
<pre>
GOOGLE_APPLICATION_CREDENTIALS=/opt/spark/google_auth/authentication.json       #nothing to change here
GOOGLE_SHEET_ID="id_of_your_google_sheet"                                       # adjustment required
GOOGLE_SHEET_NAME="name_of_your_google_sheet"                                   # adjustment required
GOOGLE_DRIVE_DIRECTORY_ID="google_drive_folder_id_for_source_files"             # adjustment required
</pre>


Place your Google Service Account JSON into:
<pre>
./google_auth/authentication.json      # name it the same way
</pre>

#### ALLOW YOUR SERVICE ACCOUNT TO HAVE ACCESS TO YOUR GOOGLE DRIVE DIRECTORY AND GOOGLE SHEET (use "share" option in both and add your Service Account) !

4. Adjust _docker-compose.yml_

Update all volume mounts to point to your local project path in docker-compose.yml file. 
Example:
<pre>
- /home/your_name/project_directory/e-check-project/source_data:/opt/spark/source_data        # change only /home/your_name/project_directory/ part
</pre>



Use the same structure for:
- source_data
- sink_data
- logs
- google_auth
- script.exe

Update in _script.exe_ file:
<pre>
SPARK_COMP_DIR="your_project_directory_path"  
</pre>

## In Adjustment... (Not working right now)
5. n8n Workflow (Lidl E-Check Processing Pipeline + Automation)
- This project uses an n8n automation workflow to preprocess Lidl e-checks before they enter the Spark pipeline + automate the whole workflow (so you have to only drag and drop e-checks to the source google drive directory).
- The workflow is responsible for monitoring Google Drive, extracting data from PNG-based e-checks using AI, and finally triggering your Spark processing script on the server.

### 📌 Overview
The workflow performs the following automated steps:

1) Periodic Trigger (Every 8 Hours)
   - The workflow checks the configured Google Drive directory for any newly created files.

2) Google Drive File Detection
   - If a new file is found:

     - It inspects the file type

     - If the file is a PNG image, n8n assumes it is a Lidl e-check. If json then it moves to the last part of the pipeline

3) AI Data Extraction (PNG → JSON)
   - The PNG e-check is passed to an AI transformer node (LLM / Vision model) that:

     - Reads the receipt image

     - Extracts structured information (date, items, amounts, totals, etc.)

     - Outputs CSV data ready for ingestion

4) Trigger Spark Workflow
   - After preprocessing, n8n remotely executes your server-side script.exe file, which:

     - Starts the Dockerized Spark engine

     - Runs all PySpark transformations

     - Loads the final data into Google Sheets

<img width="[Uploading e-check project.json…]()
3077" height="495" alt="Screenshot 2025-12-12 115109" src="https://github.com/user-attachments/assets/eb8bdcab-fc8f-4227-9428-53af0618212c" />

Later will be added downloadable JSON n8n workflow




6. Build and run
<pre>
docker compose build
bash script.exe
</pre>


This will:

1) Start the Spark container
2) Run the parser pipeline
3) Push results to Google Sheets


## Category and Type mapping (_category_type.json_)

- The _category_type.json_ file is used by the program to standardize product categories and types from raw e-checks.
- Since different stores may use various names for the same product, this file ensures consistent naming in the final output.

- Purpose

  - Map raw e-check product names to standardized categories and types.

  - Allow users to customize naming for their own needs.

  - Enable the program to automatically classify new products over time.

### File Structure
  - Each entry in the JSON file follows this format:
<pre>
{
  "category": "category_name",
  "type": "type_name",
  "keywords": ["list_of_namings_used_in_raw_e_check_file"]
}
</pre>

- category – General category of the product (e.g., "Beverages")
- type – Sub-category or product type (e.g., "Coffee")
- keywords – Array of all possible raw product names found in the e-check that should be mapped to this category/type

### Example Entry
<pre>
{
  "category": "Beverages",
  "type": "Coffee",
  "keywords": ["Coffee 250g", "Instant Coffee", "Kawa 250g"]
}  
</pre>

### Customizing Categories
 - Users can edit or add new entries to match their store’s e-check naming.
 - The program will automatically use this mapping to rename products in the output table.

### Future Updates
 - Over time, new categories and types may be added and shared with users.
 - This ensures the parser stays up-to-date with new products and naming conventions.

## Folder Structure
<pre>
./code_exe/
    biedronka_e_check_code/
        amount_transformation.py
        date_extraction.py
        mass_extraction.py
        parse_json_b_check.py
        prod_sell_separation.py
    lidl_e_check_code/
        date_transformation.py
        parse_csv_lidl_e_check.py
    utils/
        logger_setup.py
    category_identification.py
    category_type.json
    delete_source_files.py
    gs_load.py
    load_files.py
    main.py

./google_auth/authentication.json
./logs/
./sink_data/csv_file/
./source_data/
    csv_files/
    json_files/
./spark-conf/spark-defaults.conf
./.env
./.gitignore
./docker-compose.yml
./dockerfile
./script.exe
</pre>

## Output Table Structure

Final Spark output written to Google Sheet includes:
<pre>
Column	                            Description
Cost	                            Price per item
Amount ps	                        Quantity of product (pieces)
Amount kg (L)	                    Mass or volume
Discount	                        Discount applied
Currency                            Currency info
Ex.rate (cur-cur)	                Exchange rate
Amount in PLN	                    Final normalized price
Category	                        General product category
Type	                            Sub-category
Date	                            Date of purchase
Time	                            Time of purchase
Place	                            Store name / location  
</pre>

## Supported Stores

- Biedronka	-> JSON	-> ✔ Fully supported
- Lidl	-> CSV	-> ⚠ Needs n8n adjustments — use Biedronka for now


## How to Run (Manually)
<pre>
bash script.exe  
</pre>
  

This executes the whole workflow.

## Future Updates...

- Add more store integrations
- Automate Lidl CSV normalization via n8n
- Add error reporting and notifications
- Store data in BigQuery or PostgreSQL instead of Google Sheets
