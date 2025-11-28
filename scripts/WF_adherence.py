# %%
import os
import base64
import datetime as dt
import io
from typing import Optional
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from jinja2 import Template
import pandas as pd
import psycopg2  # Assuming you're using PostgreSQL
from datetime import datetime, timedelta
from psycopg2 import OperationalError
import numpy as np
import base64


# Define SCOPES
SCOPES = ['https://www.googleapis.com/auth/gmail.send']  # Adjust as needed for your use case



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



def create_html_message(
    sender,
    to_emails,                         # <- fix: was `to`
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

cc_emails = ["prakhar@goyoyo.ai", "nikhil@goyoyo.ai", "harshal@goyoyo.ai", "adarsh@goyoyo.ai", "rohan@goyoyo.ai", "pranet@goyoyo.ai"]
#cc_emails = ["adarsh@goyoyo.ai"]

to_emails = [
    "adnan.kazim@wakefit.co",
    "deepak.kumar1@wakefit.co",
    "sanket.deshmukh@wakefit.co",
    "nitin.yadav@wakefit.co",
    "sakshi.mishra@wakefit.co",
    "mayank.gaurav@wakefit.co",
    "mohit.goyal@wakefit.co",
    "abhishek.dhariya@wakefit.co",
    "animesh.mondal@wakefit.co",
    "raghuveer.singh@wakefit.co",
    "nithin.rajan@wakefit.co",
    "santhosh.hd@wakefit.co",
    "karthik.s1@wakefit.co",
    "karthik.s1@wakefit.co"
]




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
        
        <p>Hi Wakefit Team,</p>

        <div class="summary-box">
            <p><strong>Report Date:</strong> {{date_query}}</p>
            <p>This report reflects data upload adherence over the last 7 days and MTD</p>
        </div>

        <p>Please find below the summary of same day data upload adherence for <span class="highlight">{{date_query}}</span>:</p>
        
        {{html_table1}}

        <div class="date-range">
            <strong>Date Ranges:</strong><br>
            <strong>WTD:</strong> {{start_date_week}} to {{date_query}}<br>
            <strong>MTD:</strong> {{start_date_month}} to {{date_query}}
        </div>

        <div class="footer">
            <p><strong>Best regards,</strong><br>YOYO AI Team</p>
        </div>
    </div>
</body>
</html>
"""



# Set the date as current date - 1
date_query = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
#date_query = pd.to_datetime('2025-11-03').strftime('%Y-%m-%d')

date_query_dt = datetime.strptime(date_query, "%Y-%m-%d")

start_of_month = date_query_dt.replace(day=1)
wtd_range_start = max(date_query_dt.replace(day=1), date_query_dt - timedelta(days=6))
wtd_range_start_dt = wtd_range_start.strftime('%Y-%m-%d')
start_date_month = start_of_month.strftime('%Y-%m-%d')
weekday_idx = date_query_dt.weekday()
start_of_week = date_query_dt - pd.Timedelta(days=weekday_idx)
end_of_week = start_of_week + pd.Timedelta(days=6)
start_date_week = start_of_week.strftime('%Y-%m-%d')
end_date_week = end_of_week.strftime('%Y-%m-%d')
diff = (date_query_dt - wtd_range_start).days
diff2 = (date_query_dt - start_of_month).days + 1

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
	b.recorded_duration_hms as "Recorded Duration"
from (WITH base AS (
    SELECT dr.name, dr.store_id
    FROM device_registration AS dr
    WHERE dr.brand_id = 6
    UNION
    SELECT d.name, d.store_id
    FROM device AS d
    LEFT JOIN store AS s ON d.store_id = s.id
    WHERE d.assigned_on <= DATE '{date_query}'
      AND COALESCE(d.unassigned_on, DATE '2099-12-31') >= DATE '{date_query}'
      AND s.brand_id = 6
)
SELECT
    u.name                                      AS "ABM",
	st.name                                     AS "Store",
	sp.name                                     AS "Staff Name",
	b.name                                      AS "Device",
	upper(sp.employee_id) AS "Employee ID",
    sp.id AS sp_id
FROM base AS b
LEFT JOIN device AS d
       ON b.name = d.name
      AND d.assigned_on <= DATE '{date_query}'
      AND COALESCE(d.unassigned_on, DATE '2099-12-31') >= DATE '{date_query}'
	  AND (d.comments IS NULL OR d.comments <> 'To be Delete Row')
LEFT JOIN sales_person AS sp ON d.sales_person_id = sp.id
LEFT JOIN store AS st        ON b.store_id = st.id
LEFT JOIN area_business_manager AS abm ON st.abm_id = abm.id
LEFT JOIN users AS u         ON abm.user_id = u.id) as a
left join (
 select 
        device,
        count(id) as total_files,
        TO_CHAR(make_interval(secs => SUM(recorded_duration_seconds)), 'HH24:MI:SS') AS recorded_duration_hms,
        sum(valid_file) as valid_files,
		count(id) - sum(valid_file) as blank_files
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
                END AS valid_file
        FROM audio_file as a
        WHERE path LIKE '%wakefit%' and date = '{date_query}' 
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

    select 
        sales_person_id AS sp_id,
        TO_CHAR(make_interval(secs => SUM(recorded_duration_seconds)), 'HH24:MI:SS') AS "Recorded Hour (MTD)"
    from (
        SELECT 
            sales_person_id,
            EXTRACT(EPOCH FROM a.duration::interval) AS recorded_duration_seconds
        FROM audio_file as a
        WHERE path LIKE '%wakefit%' and date between date_trunc('month', date '{date_query}') and '{date_query}'
		and date = createddate
		AND SUBSTRING(filename FROM 10 FOR 6) BETWEEN '093000' AND '223000') as a 
        group by 1

    
"""

# Print the query to see the actual SQL string
#print(f"Executing SQL Query:\n{query1}")

try:
    cursor.execute(query2)
    
    # Fetch the data
    rows = cursor.fetchall()
    
    # Extract column names
    column_names = [desc[0] for desc in cursor.description]
    # Create the DataFrame using data and column names
    df2 = pd.DataFrame(rows, columns=column_names)
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


query3 = f"""

    select 
        sales_person_id AS sp_id,
        TO_CHAR(make_interval(secs => SUM(recorded_duration_seconds)), 'HH24:MI:SS') AS "Recorded Hour (WTD)"
    from (
        SELECT 
            sales_person_id,
            EXTRACT(EPOCH FROM a.duration::interval) AS recorded_duration_seconds
        FROM audio_file as a
        WHERE path LIKE '%wakefit%' 
        and date between GREATEST( (date_trunc('month', DATE '{date_query}')),(DATE '{date_query}' - 6)) and '{date_query}'
		and date = createddate
		AND SUBSTRING(filename FROM 10 FOR 6) BETWEEN '093000' AND '223000') as a 
        group by 1

    
"""

# Print the query to see the actual SQL string
#print(f"Executing SQL Query:\n{query1}")

try:
    cursor.execute(query3)
    
    # Fetch the data
    rows = cursor.fetchall()
    
    # Extract column names
    column_names = [desc[0] for desc in cursor.description]
    # Create the DataFrame using data and column names
    df3 = pd.DataFrame(rows, columns=column_names)
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


finally:
    cursor.close()


df1 = (df1.merge(df2, on = ['sp_id'],how = 'left')
        .merge(df3, on = ['sp_id'],how = 'left')
)

df1['Shift Duration'] = '8:00:00'


df1['Recorded hour adherence (%)'] = np.where(pd.to_timedelta(df1['Recorded Duration']).notna(), (pd.to_timedelta(df1['Recorded Duration'])/ pd.to_timedelta(df1['Shift Duration']) * 100).round(1).astype(str) + '%', 
                                          None)   

if diff < 7:
    df1['Recorded hour adherence (WTD) (%)'] = np.where(pd.to_timedelta(df1['Recorded Hour (WTD)']).notna(), (pd.to_timedelta(df1['Recorded Hour (WTD)'])/ (pd.to_timedelta(df1['Shift Duration'])*(diff +1) ) * 100).round(1).astype(str) + '%', 
                                                  None)
else:
    df1['Recorded hour adherence (WTD) (%)'] = np.where(pd.to_timedelta(df1['Recorded Hour (WTD)']).notna(), (pd.to_timedelta(df1['Recorded Hour (WTD)'])/ (pd.to_timedelta(df1['Shift Duration'])*diff ) * 100).round(1).astype(str) + '%', 
                                                  None)
    
df1['Recorded hour adherence (MTD) (%)'] = np.where(pd.to_timedelta(df1['Recorded Hour (MTD)']).notna(), (pd.to_timedelta(df1['Recorded Hour (MTD)'])/ (pd.to_timedelta(df1['Shift Duration'])*diff2 ) * 100).round(1).astype(str) + '%', 
                                                  None)
df1['Device Active'] = np.where(df1['Recorded Duration'].isna(), 'No', 'Yes')   

df1['Blank Files %'] = np.where(df1['Device Active'] == 'Yes',((df1['Blank Files'] / df1['Total Files']) * 100).round(1).astype(str) + '%', None)


df1 = df1[['ABM', 'Store', 'Staff Name', 'Employee ID', 'Device', 'Device Active','Total Files', 'Valid Files', 'Blank Files', 'Blank Files %', 'Shift Duration', 'Recorded Duration', 'Recorded hour adherence (%)', 'Recorded Hour (WTD)', 'Recorded hour adherence (WTD) (%)', 'Recorded Hour (MTD)', 'Recorded hour adherence (MTD) (%)']]


df_valid_1 = df1[df1['Total Files'].notna()]
df_valid_2 = df1[df1['Staff Name'].notna()]

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

same_day_transfer = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(same_day_transfer=('Device Active', lambda x: (x == 'Yes').sum()))
)

same_day_not_transfer = (
    df1.groupby('ABM', as_index=False)
       .agg(same_day_not_transfer=('Device Active', lambda x: (x == 'No').sum()))
)

total_files = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(total_files=('Total Files', 'sum'))
)

active_perc = (
    total_devices
    .merge(same_day_transfer, on=['ABM'], how='left')
)

import numpy as np

active_perc['active_perc'] = np.where(
    active_perc['same_day_transfer'].notna() & active_perc['total_devices'].notna(),
    (active_perc['same_day_transfer'] / active_perc['total_devices'] * 100)
        .round(0)
        .astype('Int64')
        .astype(str) + '%',
    np.nan
)


active_perc = active_perc.drop(columns=['same_day_transfer', 'total_devices'])

time_logged = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(time_logged=(
           'Recorded hour adherence (%)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged['time_logged'] = time_logged['time_logged'].round(0).astype('Int64').astype(str) + '%'

blank_file = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(blank_file=(
           'Blank Files %',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

blank_file['blank_file'] = blank_file['blank_file'].round(0).astype('Int64').astype(str) + '%'

time_logged_wtd = (
    df1.groupby('ABM', as_index=False)
       .agg(time_logged_wtd=(
           'Recorded hour adherence (WTD) (%)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged_wtd['time_logged_wtd'] = time_logged_wtd['time_logged_wtd'].round(0).astype('Int64').astype(str) + '%'

time_logged_mtd = (
    df1.groupby('ABM', as_index=False)
       .agg(time_logged_mtd=(
           'Recorded hour adherence (MTD) (%)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged_mtd['time_logged_mtd'] = time_logged_mtd['time_logged_mtd'].round(0).astype('Int64').astype(str) + '%'

final = (
    total_devices
    .merge(inactive, on=['ABM'], how='left')
    .merge(active, on=['ABM'], how='left')
    .merge(same_day_not_transfer, on=['ABM'], how='left')
    .merge(same_day_transfer, on=['ABM'], how='left')
    .merge(total_files, on=['ABM'], how='left')
    .merge(active_perc, on=['ABM'], how='left')
    .merge(time_logged, on=['ABM'], how='left')
    .merge(blank_file, on=['ABM'], how='left')
    .merge(time_logged_wtd, on=['ABM'], how='left')
    .merge(time_logged_mtd, on=['ABM'], how='left')
    .fillna(0)
)

renamed_columns = {
    'total_devices': 'Total Devices',   
    'inactive': 'Inactive Devices',
    'active': 'Total Active Devices', 
    'same_day_not_transfer': 'Same Day Not Received',
    'same_day_transfer': 'Same Day Received',
    'total_files': 'Total Files',
    'active_perc': 'Active Device %',
    'time_logged': 'Recording Hour Adherence (%)',
    'blank_file': 'Blank Files (%)',
    'time_logged_wtd': 'Recorded Hour Adherence (WTD) (%)',
    'time_logged_mtd': 'Recorded Hour Adherence (MTD) (%)'
}

final = final.rename(columns=renamed_columns)

totals = pd.DataFrame({
    "ABM": ["Grand Total"],
    "Total Devices": [final["Total Devices"].sum()],
    "Inactive Devices": [final["Inactive Devices"].sum()],
    "Total Active Devices": [final["Total Active Devices"].sum()],
    "Same Day Not Received": [final["Same Day Not Received"].sum()],
    "Same Day Received": [final["Same Day Received"].sum()],
    "Total Files": [final["Total Files"].sum()]
})
    
totals["Active Device %"]= pd.to_numeric(final["Active Device %"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Recording Hour Adherence (%)"]= pd.to_numeric(final["Recording Hour Adherence (%)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Blank Files (%)"]= pd.to_numeric(final["Blank Files (%)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Recorded Hour Adherence (WTD) (%)"]= pd.to_numeric(final["Recorded Hour Adherence (WTD) (%)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Recorded Hour Adherence (MTD) (%)"]= pd.to_numeric(final["Recorded Hour Adherence (MTD) (%)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'

final = pd.concat([final, totals], ignore_index=True)
final['Total Files'] = final['Total Files'].astype('Int64')
df1 = df1.sort_values(
    by=['ABM', 'Store', 'Staff Name'],
    ascending=[False, False, False]
)

csv_bytes = df_to_csv_bytes(df1)
date_str = date_query  # e.g., "2025-11-05"


cols = final.columns
rows_html = []

for idx, row in final.iterrows():
    # Check if this is the Grand Total row
    is_grand_total = str(row[cols[0]]).strip() == "Grand Total"
    
    # Add class to the row if it's Grand Total
    row_class = ' class="grand-total"' if is_grand_total else ''
    
    cells = []
    for col in cols:
        cell_value = row[col]
        style = "text-align:center;border:1px solid #ddd;padding:12px 10px;"
        
        # First column (ABM name) should be left-aligned
        if col == cols[0]:
            style = "text-align:left;border:1px solid #ddd;padding:12px 10px;padding-left:20px;font-weight:600;"
        
        cells.append(f"<td style='{style}'>{cell_value}</td>")
    
    rows_html.append(f"<tr{row_class}>" + "".join(cells) + "</tr>")

html_table_final = ( 
    "<table border='0' cellpadding='0' cellspacing='0' " 
    "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;width:100%;'>" 
    "<thead><tr>" 
    + "".join([f"<th style='padding:14px 10px;text-align:center;'>{c}</th>" for c in cols]) 
    + "</tr></thead><tbody>" 
    + "".join(rows_html) + "</tbody></table>" 
)


dt = datetime.strptime(date_query, "%Y-%m-%d")
ds = datetime.strptime(start_date_week, "%Y-%m-%d")
de = datetime.strptime(end_date_week, "%Y-%m-%d")
dm = datetime.strptime(start_date_month, "%Y-%m-%d")
dw = datetime.strptime(wtd_range_start_dt, "%Y-%m-%d")

dt_f = f"{ordinal(dt.day)} {dt.strftime('%b')}'{dt.strftime('%y')}"
ds_f = f"{ordinal(ds.day)} {ds.strftime('%b')}'{ds.strftime('%y')}"
de_f = f"{ordinal(de.day)} {de.strftime('%b')}'{de.strftime('%y')}"
dm_f = f"{ordinal(dm.day)} {dm.strftime('%b')}'{dm.strftime('%y')}"
dw_f = f"{ordinal(dw.day)} {dw.strftime('%b')}'{dw.strftime('%y')}"

template = template
email_template = Template(template)
email_content = email_template.render(
    html_table1=html_table_final,
    date_query=dt_f,
    start_date_week=dw_f,
    end_date_week=de_f, 

)

date_query_dt = datetime.strptime(date_query, "%Y-%m-%d")

subject_template = 'Wakefit Adherence Report: {{ start_date_week }} to {{ end_date_week }}'

        # Render the subject using Jinja2
subject = Template(subject_template).render(
    start_date_week = ds_f,
    end_date_week = de_f
)

send_html_email_gmail_api(service,'reports@goyoyo.ai',to_emails,cc_emails,subject,email_content, attachments=[(f"WF_{date_str}.csv", csv_bytes, "text", "csv")] )

