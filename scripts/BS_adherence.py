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

cc_emails = ["prakhar@goyoyo.ai", "nikhil@goyoyo.ai", "harshal@goyoyo.ai", "adarsh@goyoyo.ai"]
#cc_emails = []

to_emails = ['adnan.kazim@wakefit.co', 'santhosh.hd@wakefit.co', 'dibyendu.panda@wakefit.co', 'ambarish.varadan@wakefit.co', 'mohit.goyal@wakefit.co']
#to_emails = ['adarsh@goyoyo.ai']

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

def find_message_id_for_sender_on_date(service, sender_email: str, date_str: str) -> Optional[str]:
   
    # --- define date range dynamically ---
    # Example: fetch mails for yesterday
    today = dt.date.today()
    target_date = today - dt.timedelta(days=0)   # change to 0 if you want "today"
    next_day = target_date + dt.timedelta(days=1)

    # Gmail query: from specific sender, exact date window
    # after: includes >= date, before: excludes < next_day
    q = f'from:{sender_email} after:{target_date.isoformat()} before:{next_day.isoformat()}'

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

def download_csv_from_sender_on_date(sender_email: str, date_str: str) -> pd.DataFrame:
    """
    High-level helper. Raises RuntimeError on failure.
    """
    svc = service_gmail_api()
    msg_id = find_message_id_for_sender_on_date(svc, sender_email, date_str)
    if not msg_id:
        raise RuntimeError(f"No message found from {sender_email} on {date_str}")
    df = get_csv_attachment_from_message(svc, msg_id)
    if df is None:
        raise RuntimeError(f"No CSV attachment found in message {msg_id}")
    return df


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

#cc_emails = ["prakhar@goyoyo.ai", "nikhil@goyoyo.ai", "harshal@goyoyo.ai", "adarsh@goyoyo.ai"]
cc_emails = ["adarsh@goyoyo.ai"]

to_emails = [
    "adarsh@goyoyo.ai"
]

template = """

<html>
<head>
    <style>
         body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1000px;
            margin: 0 auto;
            padding: 20px;
        }
        
        tr:nth-child(even) {
            background-color: #f9f9f9;
        }
        
        tr:hover {
            background-color: #f0f8ff;
        }
        
        .interaction-codes {
            font-family: 'Courier New', monospace;
            font-size: 11px;
            max-width: 250px;
        }
        
        .code-item {
            display: inline-block;
            background-color: #f0f0f0;
            border: 1px solid #ccc;
            padding: 2px 6px;
            margin: 2px;
            border-radius: 3px;
            font-size: 10px;
            white-space: nowrap;
        }
        
        .count {
            text-align: center;
            font-weight: bold;
        }
        
        .percentage {
            text-align: center;
            font-weight: bold;
        }
        
        .insight {
            background-color: #e8f4fd;
            border-left: 4px solid #007acc;
            padding: 15px;
            margin: 20px 0;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <p>Hi BlueStone,</p>

    <p>PFA the data upload adherence report for {{date_query}}.</p>
    
    {{html_table1}}

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


    <p>Regards,<br>YOYO AI.</p>
</body>
</html>

"""

# Set the date as current date - 1
date_query = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
#date_query = pd.to_datetime('2025-11-03').strftime('%Y-%m-%d')

date_query_dt = datetime.strptime(date_query, "%Y-%m-%d")
today = datetime.now()
start_of_month = date_query_dt.replace(day=1)
start_date_month = start_of_month.strftime('%Y-%m-%d')
date_2_days_ago = today - pd.Timedelta(days=2)
start_of_week = date_2_days_ago - pd.Timedelta(days=date_2_days_ago.weekday())
end_of_week = start_of_week + pd.Timedelta(days=6)
start_date_week = start_of_week.strftime('%Y-%m-%d')
end_date_week = end_of_week.strftime('%Y-%m-%d')


# ---------- USER CONFIG ----------
SENDER_EMAIL = "notifications@zohoanalytics.in"     # change to desired sender
DATE_STR = date_query                 # YYYY-MM-DD (the exact date you want to search)
# ---------------------------------

try:
    df = download_csv_from_sender_on_date(SENDER_EMAIL, DATE_STR)
    print("Downloaded CSV into DataFrame. Shape:", df.shape)
    #print(df.head())
    # Optionally save locally:
    #df.to_csv(f"downloaded_from_{SENDER_EMAIL.replace('@','_')}_{DATE_STR}.csv", index=False)
except Exception as exc:
    print("Error:", exc)


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
    WHERE dr.brand_id = 5
    UNION
    SELECT d.name, d.store_id
    FROM device AS d
    LEFT JOIN store AS s ON d.store_id = s.id
    WHERE d.assigned_on <= DATE '{date_query}'
      AND COALESCE(d.unassigned_on, DATE '2099-12-31') >= DATE '{date_query}'
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
        WHERE path LIKE '%bluestone%' and date = '{date_query}' 
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


finally:
    cursor.close()

df['Shift Duration'] = (pd.to_timedelta(df['OutTime']) - pd.to_timedelta(df['InTime']) - pd.Timedelta(hours=1, minutes=30)).apply(lambda x: '9:30:00' if pd.isna(x) or x.total_seconds() < 0 else str(x).split(' ')[-1])
df1 = df1.merge(df[['UserID','Shift Duration']], how='left',left_on=['Employee ID'], right_on= ['UserID']).drop(columns='UserID')

df1['Same Day Transfer'] = np.where(df1['Recorded Duration'].isna(), 'No', 'Yes')

df1['Shift Duration'] = np.where((df1['Shift Duration'].isna()) & (df1['Same Day Transfer'] == 'Yes'), '9:30:00', df1['Shift Duration'])
df1['Week off/ OOO'] = np.where(df1['Shift Duration'].isna(), 'Yes', 'No')

df1['Blank Files (%)'] = np.where(
    (df1['Total Files'] > 0) & (df1['Week off/ OOO'] == 'No'),
    (df1['Blank Files'] / df1['Total Files'] * 100).round(1).astype(str) + '%',
    np.where(
        (df1['Week off/ OOO'] == 'Yes'),
        None,
        ''
    )
)

df1['Device Active adherence (%)'] = np.where(
    (df1['Week off/ OOO'] == 'No') &
    (pd.to_timedelta(df1['Recorded Duration'], errors='coerce') > pd.Timedelta(0)),
    (pd.to_timedelta(df1['Recorded Duration'], errors='coerce') /
     pd.to_timedelta(df1['Shift Duration'], errors='coerce') * 100)
    .round(1)
    .astype(str) + '%',
    np.where(
        (df1['Week off/ OOO'] == 'Yes'),
        None,
        ''
    )
)

df1 = df1[['ABM', 'Store',	'Staff Name', 'Device', 'Employee ID', 'Same Day Transfer', 'Week off/ OOO', 'Total Files',	'Valid Files', 'Blank Files', 'Blank Files (%)','Recorded Duration', 'Shift Duration',  'Device Active adherence (%)']]

df_valid_1 = df1[df1['Staff Name'].notna()]
df_valid_2 = df1[df1['Staff Name'].notna() & (df1['Week off/ OOO'] == 'No') ]


total_devices = (
    df1.groupby('ABM', as_index=False)
       .agg(total_devices=('Device', 'nunique'))
)


same_day_transfer = (
    df_valid_2.groupby('ABM', as_index=False)
       .agg(same_day_transfer=('Same Day Transfer', lambda x: (x == 'Yes').sum()))
)

same_day_not_transfer = (
    df_valid_1.groupby('ABM')
       .apply(lambda g: ((g['Same Day Transfer'] == 'No') & (g['Week off/ OOO'] != 'Yes')).sum())
       .reset_index(name='same_day_not_transfer')
)

week_off = (
    df_valid_1.groupby('ABM', as_index=False)
       .agg(week_off=('Week off/ OOO', lambda x: (x == 'Yes').sum()))
)

inactive = (
    df1.groupby('ABM')
       .apply(lambda g: g['Staff Name'].isna().sum())
       .reset_index(name='inactive')
)



active = (
    total_devices
    .merge(week_off, on=['ABM'], how='left')
    .merge(inactive, on=['ABM'], how='left')
)

active['active'] = (
    active['total_devices'].fillna(0)
    - active['week_off'].fillna(0)
    - active['inactive'].fillna(0)
)

active = active.drop(columns=['inactive', 'week_off', 'total_devices'])


total_files = (
    df_valid_2.groupby('ABM', as_index=False)
       .agg(total_files=('Total Files', 'sum'))
)

time_logged = (
    df_valid_2.groupby('ABM', as_index=False)
       .agg(time_logged=(
           'Device Active adherence (%)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

time_logged['time_logged'] = time_logged['time_logged'].round(0).astype('Int64').astype(str) + '%'

blank_file = (
    df_valid_2.groupby('ABM', as_index=False)
       .agg(blank_file=(
           'Blank Files (%)',
           lambda x: (
               pd.to_numeric(x.str.replace('%', ''), errors='coerce')
               .dropna()
               .mean()
           )
       ))
)

blank_file['blank_file'] = blank_file['blank_file'].round(0).astype('Int64').astype(str) + '%'




final = (
    total_devices.merge(week_off, on=['ABM'], how='left')
    .merge(inactive, on=['ABM'], how='left')
    .merge(active, on=['ABM'], how='left')
    .merge(same_day_not_transfer, on=['ABM'], how='left')
    .merge(same_day_transfer, on=['ABM'], how='left')
    .merge(total_files, on=['ABM'], how='left')
    .merge(time_logged, on=['ABM'], how='left')
    .merge(blank_file, on=['ABM'], how='left')
    .fillna(0)
)

renamed_columns = {
    'total_devices': 'Total Device Count',
    'week_off': 'Week Off',
    'inactive': 'Inactive Device',
    'active': 'Total Device Active',
    'same_day_not_transfer': 'Device Active: No Worn/ not Transferred',
    'same_day_transfer': 'Device Active: Worn / Same Day Data Transfer',
    'total_files': 'Total Files',
    'time_logged': 'Time Log check (%)',
    'blank_file': 'Blank File (%)'
}

final = final.rename(columns=renamed_columns)


totals = pd.DataFrame({
    "ABM": ["Grand Total"],
    "Total Device Count": [final["Total Device Count"].sum()],
    "Week Off": [final["Week Off"].sum()],
    "Inactive Device": [final["Inactive Device"].sum()],
    "Total Device Active": [final["Total Device Active"].sum()],
    "Device Active: No Worn/ not Transferred": [final["Device Active: No Worn/ not Transferred"].sum()],
    "Device Active: Worn / Same Day Data Transfer": [final["Device Active: Worn / Same Day Data Transfer"].sum()],
    "Total Files": [final["Total Files"].sum()]
})
    
totals["Time Log check (%)"]= pd.to_numeric(final["Time Log check (%)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'
totals["Blank File (%)"]= pd.to_numeric(final["Blank File (%)"].str.replace('%', ''), errors='coerce').mean().round(0).astype('int').astype(str) + '%'

final = pd.concat([final, totals], ignore_index=True)
final['Total Files'] = final['Total Files'].astype('Int64')

csv_bytes = df_to_csv_bytes(df1)
date_str = date_query  # e.g., "2025-11-05"


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
ds = datetime.strptime(start_date_week, "%Y-%m-%d")
de = datetime.strptime(end_date_week, "%Y-%m-%d")

dt_f = f"{ordinal(dt.day)} {dt.strftime('%b')}'{dt.strftime('%y')}"
ds_f = f"{ordinal(ds.day)} {ds.strftime('%b')}'{ds.strftime('%y')}"
de_f = f"{ordinal(de.day)} {de.strftime('%b')}'{de.strftime('%y')}"

template = template
email_template = Template(template)
email_content = email_template.render(
    html_table1=html_table_final,
    date_query=dt_f,
)

dt = datetime.strptime(date_query, "%Y-%m-%d")
dt_f = f"{ordinal(dt.day)} {dt.strftime('%b')}'{dt.strftime('%y')}"

subject_template = 'BlueStone Adherence Report: {{ start_date_week }} to {{ end_date_week }}'

        # Render the subject using Jinja2
subject = Template(subject_template).render(
    start_date_week = ds_f,
    end_date_week = de_f
)

send_html_email_gmail_api(service,'reports@goyoyo.ai',to_emails,cc_emails,subject,email_content, attachments=[(f"WF_{date_str}.csv", csv_bytes, "text", "csv")] )

   
