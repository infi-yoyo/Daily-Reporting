# =========================
# Standard Library
# =========================
import os
import io
import base64
import datetime as dt
from datetime import datetime, timedelta
from typing import Optional

# =========================
# Third-Party Libraries
# =========================
import numpy as np
import pandas as pd
from jinja2 import Template

# =========================
# Google API Libraries
# =========================
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =========================
# Email Libraries
# =========================
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

# =========================
# Database
# =========================
import psycopg2
from psycopg2 import OperationalError





# Define SCOPES
SCOPES = ['https://www.googleapis.com/auth/gmail.send', 'https://www.googleapis.com/auth/gmail.readonly']  # Adjust as needed for your use case



# Add this function to handle file paths in GitHub Actions
def get_credentials_path():
    """Get the correct path for credentials file"""
    for path in ['credentials.json', 'scripts/credentials.json', '../credentials.json']:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("credentials.json not found in any known path")

def get_token_path():
    """Find token.json relative to script"""
    for path in ['token.json', './scripts/token.json', '../scripts/token.json']:
        if os.path.exists(path):
            return path
    raise FileNotFoundError("token.json not found")

# Update your service_gmail_api function
def service_gmail_api():
    creds = None
    token_path = 'token.json'
    
    # 1. Load token if available
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(get_token_path(), SCOPES)
        except Exception as e:
            print(f"Error loading token: {e}")
    
    # 2. Refresh token if needed
    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
        except RefreshError as e:
            print(f"Token refresh failed: {e}")
            creds = None

    # 3. If no valid creds, try loading from credentials.json
    if not creds or not creds.valid:
        try:
            cred_path = get_credentials_path()
            print(f"Loading credentials from: {cred_path}")

            with open(cred_path, 'r') as f:
                cred_data = json.load(f)

            if 'GITHUB_ACTIONS' in os.environ:
                print("❌ Cannot perform interactive authentication in GitHub Actions.")
                print("➡️  Please generate token.json locally and include it as a GitHub secret or artifact.")
                sys.exit(1)
            else:
                from google_auth_oauthlib.flow import InstalledAppFlow
                flow = InstalledAppFlow.from_client_secrets_file(cred_path, SCOPES)
                creds = flow.run_local_server(port=8080, access_type='offline', prompt='consent')
                with open(token_path, 'w') as token:
                    token.write(creds.to_json())

        except json.JSONDecodeError as e:
            print(f"Invalid JSON in credentials file: {e}")
            sys.exit(1)
        except Exception as e:
            print(f"Authentication failed: {e}")
            sys.exit(1)

    # 4. Build the Gmail API service
    try:
        service = build('gmail', 'v1', credentials=creds)
        return service
    except HttpError as error:
        print(f'An error occurred building Gmail service: {error}')
        sys.exit(1)

service = service_gmail_api()

def find_message_id_for_sender_on_date(service, sender_email: str, date_str: str) -> Optional[str]:
   
    target_date = dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    next_day = target_date + dt.timedelta(days=1)
    
    # Gmail query: from specific sender on exact date
    # after:date includes that date, before:next_day excludes next day (so only target date matches)
    q = f'from:{sender_email} after:{target_date.strftime("%Y/%m/%d")} before:{next_day.strftime("%Y/%m/%d")}'
    
    print(f"Searching with query: {q}")  # Debug output
    results = service.users().messages().list(userId="me", q=q, maxResults=10).execute()
    messages = results.get("messages", [])
    if not messages:
        return None
    # return the first message id (you can modify logic to pick newest/oldest)
    return messages[0]["id"]

def get_csv_attachment_from_message(service, message_id: str) -> Optional[pd.DataFrame]:
    """
    Given a Gmail message id, find an attachment whose filename ends with .csv or mimeType 'text/csv',
    download it and return as pandas.DataFrame. If no CSV attachment found, returns None.
    """
    msg = service.users().messages().get(userId="me", id=message_id, format="full").execute()
    payload = msg.get("payload", {})
    parts = payload.get("parts", []) or []

    # Helper to recursively walk parts (for nested multiparts)
    def walk_parts(p_list):
        for p in p_list:
            yield p
            if p.get("parts"):
                for child in walk_parts(p.get("parts")):
                    yield child

    for part in walk_parts(parts):
        filename = part.get("filename", "")
        body = part.get("body", {})
        mime = part.get("mimeType", "")

        # Attachment in body: has attachmentId
        att_id = body.get("attachmentId")
        if att_id and (filename.lower().endswith(".csv") or mime == "text/csv"):
            att = service.users().messages().attachments().get(
                userId="me", messageId=message_id, id=att_id
            ).execute()
            data_b64 = att.get("data")
            if not data_b64:
                continue
            # Gmail returns urlsafe base64 without padding sometimes — fix padding
            data_b64 = data_b64.replace("-", "+").replace("_", "/")
            padding = len(data_b64) % 4
            if padding:
                data_b64 += "=" * (4 - padding)
            binary = base64.b64decode(data_b64)
            # Read into pandas
            try:
                df = pd.read_csv(io.BytesIO(binary))
                return df
            except Exception as e:
                # If CSV uses different delimiter or encoding, try some fallbacks
                try:
                    df = pd.read_csv(io.BytesIO(binary), encoding="utf-8", delimiter=",")
                    return df
                except Exception as e2:
                    raise RuntimeError(f"Failed to parse CSV attachment: {e}; fallback error: {e2}")
    return None

def download_csv_from_sender_on_date(sender_email: str, start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """
    High-level helper that downloads CSVs from a sender between start and end dates (inclusive).
    Concatenates all CSVs into a single DataFrame. Raises RuntimeError on failure.
    """
    from datetime import datetime, timedelta
    
    # Parse date strings
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Validate date range
    if start_date > end_date:
        raise ValueError("start_date must be before or equal to end_date")
    
    svc = service_gmail_api()
    master_df = pd.DataFrame()
    
    # Iterate through each date in the range
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")
        
        try:
            msg_id = find_message_id_for_sender_on_date(svc, sender_email, date_str)
            if msg_id:
                df = get_csv_attachment_from_message(svc, msg_id)
                if df is not None:
                    master_df = pd.concat([master_df, df], ignore_index=True)
        except Exception as e:
            # Log the error but continue with other dates
            print(f"Warning: Failed to process {date_str}: {str(e)}")
        
        current_date += timedelta(days=1)
    
    if master_df.empty:
        raise RuntimeError(f"No CSV attachments found from {sender_email} between {start_date_str} and {end_date_str}")
    
    return master_df

def create_html_message(
    sender,
    to_emails,                         
    subject,
    html_content,
    cc_emails=None,
    attachment_paths=None,             # optional: disk files
    attachments=None                   # optional: in-memory [(filename, bytes, mime_main, mime_sub)]
):
    """Create a Gmail API message with HTML + optional attachments (paths or in-memory)."""
    cc_emails = cc_emails or []
    attachment_paths = attachment_paths or []
    attachments = attachments or []

    # multipart/mixed container (for attachments); HTML body can be directly attached
    message = MIMEMultipart('mixed')
    message['from'] = sender
    message['to'] = ', '.join(to_emails)
    if cc_emails:
        message['cc'] = ', '.join(cc_emails)
    message['subject'] = subject

    # HTML body
    message.attach(MIMEText(html_content, 'html', 'utf-8'))

    # File-path attachments

    for path in attachment_paths:
        if not os.path.exists(path):
            print(f"⚠️ File not found, skipping: {path}")
            continue
        with open(path, 'rb') as f:
            payload = f.read()
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(payload)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{os.path.basename(path)}"')
        message.attach(part)

    # In-memory attachments
    for filename, blob, mime_main, mime_sub in attachments:
        part = MIMEBase(mime_main, mime_sub)
        part.set_payload(blob)
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
        # Add a Content-Type header for clarity (especially for CSV)
        part.add_header('Content-Type', f'{mime_main}/{mime_sub}; name="{filename}"')
        message.attach(part)

    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    return {'raw': raw_message}




def send_html_email_gmail_api(service, sender_email, to_emails, cc_emails, subject, html_content,
                              attachment_paths=None, attachments=None):
    """Send HTML email using Gmail API."""
    message = create_html_message(
        sender_email, to_emails, subject, html_content, cc_emails,
        attachment_paths=attachment_paths, attachments=attachments
    )
    try:
        return service.users().messages().send(userId='me', body=message).execute()
    except Exception as e:
        print(f'An error occurred: {e}')
        return None
    

def aggregate_user_durations(df):
    """
    Aggregates user attendance data by calculating days count and total duration.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe with columns: UserID, InDateTime, OutDateTime, Date
        
    Returns:
    --------
    pandas.DataFrame
        Aggregated dataframe with columns: UserID, days_count, duration_sum_HHMMSS
    """
    # Create a copy to avoid modifying the original
    df = df.copy()
    
    # Convert datetime columns
    df["InDateTime"]  = pd.to_datetime(df["InDateTime"],  errors="coerce")
    df["OutDateTime"] = pd.to_datetime(df["OutDateTime"], errors="coerce")
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce").dt.date
    
    # Calculate duration (subtract 1 hour 30 minutes)
    df["duration"] = df["OutDateTime"] - df["InDateTime"] - pd.Timedelta(hours=1, minutes=30)
    
    # Apply default duration of 9:30 for invalid/negative durations
    df["duration"] = df["duration"].apply(
        lambda x: pd.Timedelta(hours=9, minutes=30) if pd.isna(x) or x < pd.Timedelta(0) else x
    )
    
    # Convert to string format (HH:MM:SS)
    df["duration"] = df["duration"].astype(str).str.split(" ").str[-1]
    
    # Convert back to Timedelta for aggregation
    df["duration"] = pd.to_timedelta(df["duration"])
    
    # Aggregate by UserID
    agg = (
        df.groupby("UserID", as_index=False)
          .agg(
              days_count=("Date", "nunique"),
              duration_sum=("duration", "sum"),
          )
    )
    
    # Format total duration as H:MM:SS
    total_seconds = agg["duration_sum"].dt.total_seconds().round().astype("Int64")
    hours = (total_seconds // 3600).astype("Int64")
    minutes = ((total_seconds % 3600) // 60).astype("Int64")
    seconds = (total_seconds % 60).astype("Int64")
    
    agg["duration_sum_HHMMSS"] = (
        hours.astype(str) + ":" +
        minutes.astype(str).str.zfill(2) + ":" +
        seconds.astype(str).str.zfill(2)
    )
    
    # Return final aggregated dataframe
    return agg[["UserID", "days_count", "duration_sum_HHMMSS"]]
    


def ordinal(n):
    return "%d%s" % (n, "tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_csv(buf, index=False, encoding='utf-8-sig')
    return buf.getvalue()

# 1. Connect to your Database
def create_connection():
    connection = None
    try:
        connection = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            dbname=os.getenv("DB_NAME")
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Create connection
connection = create_connection()


template = """
<html>
<head>
    <style>
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }
        
        .email-container {
            background-color: #ffffff;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        h2 {
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
            margin-bottom: 20px;
            font-size: 20px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 13px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        thead th {
            padding: 14px 10px;
            text-align: center;
            font-weight: 600;
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            border-right: 1px solid rgba(255,255,255,0.2);
        }
        
        thead th:first-child {
            text-align: left;
            padding-left: 20px;
        }
        
        thead th:last-child {
            border-right: none;
        }
        
        tbody td {
            padding: 12px 10px;
            text-align: center;
            border-bottom: 1px solid #e0e0e0;
        }
        
        tbody td:first-child {
            text-align: left;
            font-weight: 600;
            color: #2c3e50;
            padding-left: 20px;
        }
        
        tbody tr {
            background-color: #ffffff;
            transition: all 0.3s ease;
        }
        
        tbody tr:nth-child(even) {
            background-color: #f8f9fa;
        }
        
        tbody tr:hover {
            background-color: #e3f2fd;
            transform: scale(1.01);
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        /* Grand Total row styling */
        tbody tr.grand-total {
            background: linear-gradient(135deg, #ffeaa7 0%, #fdcb6e 100%) !important;
            font-weight: 700 !important;
            border-top: 3px solid #fdcb6e !important;
        }
        
        tbody tr.grand-total td {
            border-bottom: none !important;
            color: #2d3436 !important;
            font-weight: 700 !important;
        }
        
        tbody tr.grand-total:hover {
            background: linear-gradient(135deg, #ffd970 0%, #f9b54c 100%) !important;
            transform: scale(1.01);
            box-shadow: 0 3px 10px rgba(0,0,0,0.15) !important;
        }
        
        /* Color coding for percentages */
        .excellent {
            background-color: #d4edda;
            color: #155724;
            padding: 4px 8px;
            border-radius: 4px;
            font-weight: 600;
        }
        
        .good {
            background-color: #d1ecf1;
            color: #0c5460;
            padding: 4px 8px;
            border-radius: 4px;
            font-weight: 600;
        }
        
        .warning {
            background-color: #fff3cd;
            color: #856404;
            padding: 4px 8px;
            border-radius: 4px;
            font-weight: 600;
        }
        
        .poor {
            background-color: #f8d7da;
            color: #721c24;
            padding: 4px 8px;
            border-radius: 4px;
            font-weight: 600;
        }
        
        .summary-box {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            margin: 20px 0;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        
        .summary-box p {
            margin: 5px 0;
            font-size: 14px;
        }
        
        .date-range {
            background-color: #f8f9fa;
            border-left: 4px solid #3498db;
            padding: 12px 15px;
            margin: 15px 0;
            border-radius: 4px;
            font-size: 13px;
        }
        
        .footer {
            margin-top: 30px;
            padding-top: 20px;
            border-top: 2px solid #e0e0e0;
            color: #7f8c8d;
            font-size: 13px;
        }
        
        .highlight {
            background-color: #fff9e6;
            padding: 2px 6px;
            border-radius: 3px;
            font-weight: 600;
        }
    </style>
</head>
<body>
    <div class="email-container">
        <h2>Same Day Data Upload Adherence Report</h2>
        
        <p>Hi BlueStone Team,</p>

        <div class="summary-box">
            <p><strong>Report Window:</strong>
            <ul>
            <li>WTD: {{date_query}} to {{date_query_2}}</li>
            <li>MTD: {{start_date_month}} to {{date_query_2}}</li>
            </ul>
            </p>
            <p>This report reflects data upload adherence on MTD and MTD basis</p>
            <p> This reports reflect data of only those Stores for whom either of the below conditions is met:
            <ul>
            <li>Time Log check % is less than 85%</li>
            <li>Blank file contribution is greater than 8%</li>
            </ul>
            </p>
        </div>

        <p>Please find below the summary of same day data upload adherence for <span class="highlight">{{date_query}} to {{date_query_2}}</span>:</p>
        
        {{html_table1}}

        <div class="summary-box">
            <p>
                <ul>
                <li>"Device no worn" - represents that the data has not been synced with the YoYo ecosystem (i.e. the device was not connected to the laptop for transfer till yesterday midnight)</li>
                <li>Inactive device - represents device allocated to the ABM, but not allocated to a SE</li>
                <li>Record hour adherence is calculated only for the "device active" or SEs who have transferred data yesterday.</li>
                <li>Customer recorded hours represent hours of customer conversation - after removing internal discussions, personal conversations etc</li>
                <li>The Same day data upload adherence data has been updated in the google sheet.</li>
                <li>We have updated the adherence sheet based on the latest device assigned information.</li>
                <li>We aim to achieve:</li>
                    <ul>
                        <li>Blank Files: less than 5%</li>
                        <li>Device Active/ Not Worn: 0</li>
                        <li>Recording Log: Greater than 85%</li>
                    </ul>
                </ul>
            </p>
        </div>

        <div class="footer">
            <p><strong>Best regards,</strong><br>YOYO AI Team</p>
        </div>
    </div>
</body>
</html>

"""
# Set the date as current date - 1
date_query = (datetime.now() - pd.Timedelta(days=7)).strftime('%Y-%m-%d')
#date_query = pd.to_datetime('2025-12-14').strftime('%Y-%m-%d')
date_query_2 = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
#date_query_2 = pd.to_datetime('2025-12-20').strftime('%Y-%m-%d')

date_query_dt = datetime.strptime(date_query, "%Y-%m-%d")
date_query_dt_2 = datetime.strptime(date_query_2, "%Y-%m-%d")
today = datetime.now()
start_of_month = date_query_dt.replace(day=1)
start_date_month = start_of_month.strftime('%Y-%m-%d')
weekday_idx = date_query_dt.weekday()
start_of_week = date_query_dt - pd.Timedelta(days=weekday_idx)
end_of_week = start_of_week + pd.Timedelta(days=6)
start_date_week = start_of_week.strftime('%Y-%m-%d')
end_date_week = end_of_week.strftime('%Y-%m-%d')


# ---------- USER CONFIG ----------
SENDER_EMAIL = "notifications@zohoanalytics.in"     # change to desired sender
START_DATE_WTD = (date_query_dt + pd.Timedelta(days=1)).strftime('%Y-%m-%d')# YYYY-MM-DD (the exact date you want to search)     
START_DATE_MTD = (start_of_month + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
END_DATE =  (date_query_dt_2 + + pd.Timedelta(days=1)).strftime('%Y-%m-%d')    
# ---------------------------------

try:
    df_wtd = download_csv_from_sender_on_date(SENDER_EMAIL, START_DATE_WTD, END_DATE)
    print("Downloaded CSV into DataFrame. Shape:", df_wtd.shape)
    #print(df.head())
    # Optionally save locally:
    #df.to_csv(f"downloaded_from_{SENDER_EMAIL.replace('@','_')}_{DATE_STR}.csv", index=False)
except Exception as exc:
    print("Error:", exc)

try:
    df_mtd = download_csv_from_sender_on_date(SENDER_EMAIL, START_DATE_MTD, END_DATE)
    print("Downloaded CSV into DataFrame. Shape:", df_mtd.shape)
    #print(df.head())
    # Optionally save locally:
    #df.to_csv(f"downloaded_from_{SENDER_EMAIL.replace('@','_')}_{DATE_STR}.csv", index=False)
except Exception as exc:
    print("Error:", exc)


agg_wtd = aggregate_user_durations(df_wtd)
agg_mtd = aggregate_user_durations(df_mtd)

# Check if the connection is still open
if connection.closed == 0:
    cursor = connection.cursor()

# Ensure any previously failed transaction is rolled back
connection.rollback()

query1 = f"""

    select a.*, 
	b.total_files as "Total Files",
	b.valid_files as "Valid Files",
	b.blank_files as "Blank Files",
	b.recorded_duration_hms as "Recorded Duration",
    b.active_days as "Active Days"
from (WITH base AS (
    SELECT dr.name, dr.store_id
    FROM device_registration AS dr
    WHERE dr.brand_id = 5
    UNION
    SELECT d.name, d.store_id
    FROM device AS d
    LEFT JOIN store AS s ON d.store_id = s.id
    WHERE d.assigned_on <= DATE '{date_query_2}'
      AND COALESCE(d.unassigned_on, DATE '2099-12-31') >= DATE '{date_query_2}'
      AND s.brand_id = 5
)
SELECT
    u.name                                      AS "ABM",
	st.name                                     AS "Store",
	sp.name                                     AS "Staff Name",
	b.name                                      AS "Device",
	upper(sp.employee_id) AS "Employee ID"
FROM base AS b
LEFT JOIN device AS d
       ON b.name = d.name
      AND d.assigned_on <= DATE '{date_query_2}'
      AND COALESCE(d.unassigned_on, DATE '2099-12-31') >= DATE '{date_query_2}'
	  AND (d.comments IS NULL OR d.comments <> 'To be Delete Row')
LEFT JOIN sales_person AS sp ON d.sales_person_id = sp.id
LEFT JOIN store AS st        ON b.store_id = st.id
LEFT JOIN area_business_manager AS abm ON st.abm_id = abm.id
LEFT JOIN users AS u         ON abm.user_id = u.id
where st.regional_manager_id = 3) as a
left join (
 select 
        device,
        count(id) as total_files,
        TO_CHAR(make_interval(secs => SUM(recorded_duration_seconds)), 'HH24:MI:SS') AS recorded_duration_hms,
        sum(valid_file) as valid_files,
		count(id) - sum(valid_file) as blank_files,
        count(distinct date) as active_days
    from (
        SELECT 
            LOWER(SUBSTRING(path FROM 'T[0-9]{{4}}')) as device,
            id,
            EXTRACT(EPOCH FROM a.duration::interval) AS recorded_duration_seconds,
                cast(a.speech_duration as integer) as speech_duration_seconds,
                CASE 
                WHEN a.duration IS NOT NULL  
                    AND EXTRACT(EPOCH FROM a.duration::interval) > 0
                    AND cast(a.speech_duration as integer) / EXTRACT(EPOCH FROM a.duration::interval) BETWEEN 0.1 AND 1
                THEN 1 
                ELSE 0 
                END AS valid_file, date
        FROM audio_file as a
        WHERE path LIKE '%bluestone%' and date between '{date_query}' and '{date_query_2}'
		and date = createddate
		AND SUBSTRING(filename FROM 10 FOR 6) BETWEEN '093000' AND '223000') as a 
        group by 1) as b
on a."Device" = b.device

    
"""

# Print the query to see the actual SQL string
#print(f"Executing SQL Query:\n{query1}")

try:
    cursor.execute(query1)
    
    # Fetch the data
    rows = cursor.fetchall()
    
    # Extract column names
    column_names = [desc[0] for desc in cursor.description]
    # Create the DataFrame using data and column names
    df1 = pd.DataFrame(rows, columns=column_names)
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


query2 = f"""

   SELECT
    UPPER(b.employee_id) AS employee_id,
    TO_CHAR(SUM(a.duration::interval), 'HH24:MI:SS') AS "Recorded Duration MTD",
    COUNT(DISTINCT a.date) AS "Day Data Received",
    ROUND(
        SUM(
            CASE
                WHEN a.duration IS NULL THEN 1
                WHEN NULLIF(EXTRACT(EPOCH FROM a.duration::interval), 0) IS NULL THEN 1
                WHEN (CAST(a.speech_duration AS numeric) / NULLIF(EXTRACT(EPOCH FROM a.duration::interval), 0)) > 1
                  OR (CAST(a.speech_duration AS numeric) / NULLIF(EXTRACT(EPOCH FROM a.duration::interval), 0)) <= 0.1
                THEN 1
                ELSE 0
            END
        )::numeric / NULLIF(COUNT(a.id), 0) * 100
    , 1) AS "Blank Files % (MTD)"
FROM audio_file AS a
LEFT JOIN sales_person AS b ON a.sales_person_id = b.id
WHERE a.path LIKE '%bluestone%'
  AND a.date BETWEEN date_trunc('month', DATE '{date_query_2}') AND DATE '{date_query_2}'
GROUP BY 1;

    
"""

# Print the query to see the actual SQL string
#print(f"Executing SQL Query:\n{query2}")

try:
    cursor.execute(query2)
    
    # Fetch the data
    rows = cursor.fetchall()
    
    # Extract column names
    column_names = [desc[0] for desc in cursor.description]
    # Create the DataFrame using data and column names
    df_mtd = pd.DataFrame(rows, columns=column_names)
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


finally:
    cursor.close()

#df['Shift Duration'] = (pd.to_timedelta(df['OutTime']) - pd.to_timedelta(df['InTime']) - pd.Timedelta(hours=1, minutes=30)).apply(lambda x: '9:30:00' if pd.isna(x) or x.total_seconds() < 0 else str(x).split(' ')[-1])
df1 = df1.merge(agg_wtd[['UserID','duration_sum_HHMMSS']], how='left',left_on=['Employee ID'], right_on= ['UserID']).drop(columns='UserID')

df1.rename(columns={'duration_sum_HHMMSS': 'Shift Duration'}, inplace=True)

#df1['Same Day Transfer'] = np.where(df1['Recorded Duration'].isna(), 'No', 'Yes')

df1['Shift Duration'] = np.where(df1['Shift Duration'].isna() & df1['Recorded Duration'].notna(),
    (pd.Timedelta(hours=9, minutes=30) * df1['Active Days']).astype(str).str.split(' ').str[-1],
    df1['Shift Duration']
)

df1['Shift Duration'] = np.where((df1['Shift Duration'].isna()) & (df1['Recorded Duration'].notna()), (pd.Timedelta(hours=9, minutes=30) * df1['Active Days']).astype(str).str.split(' ').str[-1], df1['Shift Duration'])
#df1['Week off/ OOO'] = np.where(df1['Shift Duration'].isna(), 'Yes', 'No')

df1['Blank Files (%) (WTD)'] = np.where(
    (df1['Total Files'] > 0),
    (df1['Blank Files'] / df1['Total Files'] * 100).round(1).astype(str) + '%',
        ''
    )


df1['Device Active adherence (%) (WTD)'] = np.where(
    (pd.to_timedelta(df1['Recorded Duration'], errors='coerce') > pd.Timedelta(0)),
    (pd.to_timedelta(df1['Recorded Duration'], errors='coerce') /
     pd.to_timedelta(df1['Shift Duration'], errors='coerce') * 100)
    .round(1)
    .astype(str) + '%',
        ''
    )


df1 = df1.merge(df_mtd, how = 'left', left_on = 'Employee ID', right_on = 'employee_id').drop(columns = 'employee_id')
df1 = df1.merge(agg_mtd[['UserID','duration_sum_HHMMSS']], how='left',left_on=['Employee ID'], right_on= ['UserID']).drop(columns='UserID')
df1.rename(columns={'duration_sum_HHMMSS': 'Shift Duration (MTD)'}, inplace=True)
df1['Shift Duration (MTD)'] = np.where((df1['Shift Duration (MTD)'].isna()) & (df1['Recorded Duration MTD'].notna()), (pd.Timedelta(hours=9, minutes=30) * df1['Day Data Received']).astype(str).str.split(' ').str[-1], df1['Shift Duration (MTD)'])


df1['Device Active adherence (%) (WTD) (MTD)'] = np.where(
    (pd.to_timedelta(df1['Recorded Duration MTD'], errors='coerce') > pd.Timedelta(0)),
    (pd.to_timedelta(df1['Recorded Duration MTD'], errors='coerce') /
     pd.to_timedelta(df1['Shift Duration (MTD)'], errors='coerce') * 100)
    .round(1)
    .astype(str) + '%',
        ''
    )

df1['Blank Files % (MTD)'] = np.where(df1['Blank Files % (MTD)'].isna(), None, df1['Blank Files % (MTD)'].astype(str) + '%')

df1 = df1.sort_values(
    by=['ABM', 'Store', 'Staff Name'],
    ascending=[True, True, True]
)

df_valid_1 = df1[df1['Staff Name'].notna()]
#df_valid_2 = df1[df1['Staff Name'].notna() & (df1['Week off/ OOO'] == 'No') ]


total_devices = (
    df1.groupby('ABM', as_index=False)
       .agg(total_devices=('Device', 'nunique'))
)



inactive = (
    df1.groupby('ABM')
       .apply(lambda g: g['Staff Name'].isna().sum())
       .reset_index(name='inactive')
)



active = (
    total_devices
    .merge(inactive, on=['ABM'], how='left')
)

active['active'] = (
    active['total_devices'].fillna(0)
    - active['inactive'].fillna(0)
)

active = active.drop(columns=['inactive', 'total_devices'])


total_files = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(total_files=('Total Files', 'sum'))
)

time_logged = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(time_logged=(
           'Device Active adherence (%) (WTD)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged['time_logged'] = time_logged['time_logged'].round(0).astype('Int64').astype(str) + '%'


time_logged_mtd = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(time_logged_mtd=(
           'Device Active adherence (%) (WTD) (MTD)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged_mtd['time_logged_mtd'] = time_logged_mtd['time_logged_mtd'].round(0).astype('Int64').astype(str) + '%'

blank_file = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(blank_file=(
           'Blank Files (%) (WTD)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

blank_file['blank_file'] = blank_file['blank_file'].round(0).astype('Int64').astype(str) + '%'

blank_file_mtd = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(blank_file_mtd=(
           'Blank Files % (MTD)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

blank_file_mtd['blank_file_mtd'] = blank_file_mtd['blank_file_mtd'].round(0).astype('Int64').astype(str) + '%'


time_logged_store = (
    df_valid_1.groupby(['ABM', 'Store'], as_index=False)
       .agg(time_logged_wtd=(
           'Device Active adherence (%) (WTD)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged_store['time_logged_wtd'] = time_logged_store['time_logged_wtd'].round(0).astype('Int64').astype(str) + '%'

blank_file_store = (
    df_valid_1.groupby(['ABM', 'Store'], as_index=False)
       .agg(blank_file_wtd=(
           'Blank Files (%) (WTD)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

blank_file_store['blank_file_wtd'] = blank_file_store['blank_file_wtd'].round(0).astype('Int64').astype(str) + '%'

final_store = (
    time_logged_store
    #.merge(week_off, on=['ABM'], how='left')
    .merge(blank_file_store, on=['Store'], how='left'))


final = (
    total_devices
    #.merge(week_off, on=['ABM'], how='left')
    .merge(inactive, on=['ABM'], how='left')
    .merge(active, on=['ABM'], how='left')
    #.merge(same_day_not_transfer, on=['ABM'], how='left')
    #.merge(same_day_transfer, on=['ABM'], how='left')
    .merge(total_files, on=['ABM'], how='left')
    .merge(time_logged, on=['ABM'], how='left')
    .merge(blank_file, on=['ABM'], how='left')
    .merge(time_logged_mtd, on=['ABM'], how='left')
    .merge(blank_file_mtd, on=['ABM'], how='left')
    .fillna(0)
)

renamed_columns = {
    'total_devices': 'Total Device Count',
    #'week_off': 'Week Off',
    'inactive': 'Inactive Device',
    'active': 'Total Device Active',
    #'same_day_not_transfer': 'Device Active: No Worn/ not Transferred',
    #'same_day_transfer': 'Device Active: Worn / Same Day Data Transfer',
    'total_files': 'Total Files',
    'time_logged': 'Time Log check (%) (WTD)',
    'blank_file': 'Blank File (%) (WTD)',
    'time_logged_mtd': 'Time Log check (% MTD)',
    'blank_file_mtd': 'Blank File (% MTD)'
    
}

final = final.rename(columns=renamed_columns)

# Work on a copy if you don't want to touch original columns
df_copy = final_store.copy()

# Numeric versions of percentage columns
df_copy['Blank_num'] = (
    pd.to_numeric(
        df_copy['blank_file_wtd'].str.rstrip('%'),
        errors='coerce'
    )
)

df_copy['TimeLog_num'] = (
    pd.to_numeric(
        df_copy['time_logged_wtd'].str.rstrip('%'),
        errors='coerce'
    )
)

# Condition for rows you want to collect
mask = (
    #(df_copy['Device Active: No Worn/ not Transferred'] / df_copy['Total Device Active'] > 0.1) |
    (df_copy['Blank_num'] > 8) |
    (df_copy['TimeLog_num'] < 85)
)

# 🔹 New dataframe with only the "bad" rows
flagged_df = df_copy[mask].copy()

flagged_df = flagged_df.drop(columns=['Blank_num', 'TimeLog_num'])


abm_list = flagged_df['ABM_x'].dropna().unique().tolist()

emails = {
    "Aditya Mittal": "aditya.mittal@bluestone.com",
    "Ansh Gupta": "Ansh.Gupta@bluestone.com",
    "Archisha Chandna": "archisha.chandna@bluestone.com",
    "Harleen Valechani": "harleen.valechani@bluestone.com",
    "Harshul": "harshul.devarchana@bluestone.com",
    "Jeevan Babyloni": "jeevan.babyloni@bluestone.com",
    "Nikhil Sachdeva": "nikhil.sachdeva@bluestone.com",
    "Parth Tyagi": "parth.tyagi@bluestone.com",
    "Urvi Haldipur": "urvi.haldipur@bluestone.com",
}

to_emails = [emails[name] for name in abm_list if name in emails]

final = final[final['ABM'].isin(abm_list)]


totals = pd.DataFrame({
    "ABM": ["Grand Total"],
    "Total Device Count": [final["Total Device Count"].sum()],
    #"Week Off": [flagged_df["Week Off"].sum()],
    "Inactive Device": [final["Inactive Device"].sum()],
    "Total Device Active": [final["Total Device Active"].sum()],
    #"Device Active: No Worn/ not Transferred": [flagged_df["Device Active: No Worn/ not Transferred"].sum()],
    #"Device Active: Worn / Same Day Data Transfer": [flagged_df["Device Active: Worn / Same Day Data Transfer"].sum()],
    "Total Files": [final["Total Files"].sum()]
})
    
totals["Time Log check (%) (WTD)"]= pd.to_numeric(final["Time Log check (%) (WTD)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Blank File (%) (WTD)"]= pd.to_numeric(final["Blank File (%) (WTD)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'

totals["Time Log check (% MTD)"]= pd.to_numeric(final["Time Log check (% MTD)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Blank File (% MTD)"]= pd.to_numeric(final["Blank File (% MTD)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'


final = pd.concat([final, totals], ignore_index=True)
final['Total Files'] = final['Total Files'].astype('Int64')

attachment_paths = [f'C:/Users/adars/Downloads/BS_{date_query}.csv']


blank_raw = (
    df1['Blank Files (%) (WTD)']
    .astype(str)
    .str.replace('%', '', regex=False)
    .str.strip()
)
blank_num = pd.to_numeric(blank_raw, errors='coerce')

adh_raw = (
    df1['Device Active adherence (%) (WTD)']
    .astype(str)
    .str.replace('%', '', regex=False)
    .str.strip()
)
adh_num = pd.to_numeric(adh_raw, errors='coerce')

blank_flag = (blank_num > 8.0).fillna(False)
adh_flag   = (adh_num < 85.0).fillna(False)


df1['comments'] = ''
df1.loc[blank_flag, 'comments'] = 'High Blank Files'
mask_adh = adh_flag

df1.loc[mask_adh, 'comments'] = np.where(
    df1.loc[mask_adh, 'comments'] == '',
    'Low Device Adherence',
    df1.loc[mask_adh, 'comments'] + ', Low Device Adherence'
)

df1['comments'] = df1['comments'].str.lstrip(', ').str.strip()


cc_emails = ['kshitij.arora@bluestone.com','mudita.gupta@bluestone.com',
 'anubha.rustagi@bluestone.com', 'chaitanya.raheja@bluestone.com', 'shubhi.shrivastava@bluestone.com', 'harshal@goyoyo.ai',
 'nikhil@goyoyo.ai',
 'pranet@goyoyo.ai',
 'rohan@goyoyo.ai',
 'adarsh@goyoyo.ai']



cols = final.columns
rows_html = []

for _, row in final.iterrows():
    cells = []
    for col in cols:
        cell_value = row[col]
        style = "text-align:center;border:1px solid #000;"
        cells.append(f"<td style='{style}'>{cell_value}</td>")
        
    rows_html.append("<tr>" + "".join(cells) + "</tr>")

html_table_final = ( 
    "<table border='1' cellpadding='6' cellspacing='0' " 
    "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;width:100%;'>" 
    "<thead><tr>" 
    + "".join([f"<th>{c}</th>" for c in cols]) 
    + "</tr></thead><tbody>" 
    + "".join(rows_html) + "</tbody></table>" 
    )


dt = datetime.strptime(date_query, "%Y-%m-%d") 
ds = datetime.strptime(start_date_month, "%Y-%m-%d")
de = datetime.strptime(date_query_2, "%Y-%m-%d")

dt_f = f"{ordinal(dt.day)} {dt.strftime('%b')}'{dt.strftime('%y')}"
ds_f = f"{ordinal(ds.day)} {ds.strftime('%b')}'{ds.strftime('%y')}"
de_f = f"{ordinal(de.day)} {de.strftime('%b')}'{de.strftime('%y')}"

template = template
email_template = Template(template)
email_content = email_template.render(
    html_table1=html_table_final,
    date_query=dt_f,
    start_date_month = ds_f,
    date_query_2 = de_f
)



subject_template = 'BlueStone Adherence Report: {{ start_date_week }} to {{ end_date_week }}'

        # Render the subject using Jinja2
subject = Template(subject_template).render(
    start_date_week = dt_f,
    end_date_week = de_f
)

#to_emails = ['adarsh@goyoyo.ai']
#cc_emails = ['adarsh@goyoyo.ai']




csv_bytes = df_to_csv_bytes(df1)
send_html_email_gmail_api(service,'reports@goyoyo.ai',to_emails,cc_emails,subject,email_content, attachments=[(f"BS_{date_query}.csv", csv_bytes, "text", "csv")] )

   
