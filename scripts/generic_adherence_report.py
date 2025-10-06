# %%
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from jinja2 import Template
import pandas as pd
import psycopg2  # Assuming you're using PostgreSQL
from datetime import datetime
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import base64
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import sys
import json
from pathlib import Path
import numpy as np
import math



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

def create_html_message(sender, to, subject, html_content, cc_emails):
    """Create a message with HTML content for Gmail API."""
    
    # Create multipart message
    message = MIMEMultipart('alternative')
    message['to'] = ', '.join(to_emails)
    message['from'] = sender
    message['cc'] = ', '.join(cc_emails)
    message['subject'] = subject
    
    # Create HTML part - this is crucial for formatting
    html_part = MIMEText(html_content, 'html', 'utf-8')
    message.attach(html_part)
    
    # Encode message
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode('utf-8')
    
    return {'raw': raw_message}

def send_html_email_gmail_api(service, sender_email, to_emails, cc_emails, subject, html_content):
    """Send HTML email using Gmail API."""
    
    message = create_html_message(sender_email, to_emails, subject, html_content, cc_emails)
    
    try:
        sent_message = service.users().messages().send(
            userId='me', 
            body=message
        ).execute()
        print(f'Message Id: {sent_message["id"]}')
        return sent_message
    except Exception as error:
        print(f'An error occurred: {error}')
        return None

def sort_by_total_interaction(df, ascending=False):
    total_col = ("Total", "Total Interaction")

    # if you already replaced 0 with '-', coerce to numbers for a correct sort
    key = pd.to_numeric(df[total_col], errors="coerce")

    # keep "Grand Total" pinned to bottom (optional)
    if "Grand Total" in df.index:
        main = df.drop(index="Grand Total")
        sorted_idx = key.loc[main.index].sort_values(ascending=ascending).index
        return pd.concat([main.loc[sorted_idx], df.loc[["Grand Total"]]])
    else:
        sorted_idx = key.sort_values(ascending=ascending).index
        return df.loc[sorted_idx]
      
def ordinal(n):
    return "%d%s" % (n, "tsnrhtdd"[(n//10%10!=1)*(n%10<4)*n%10::4])

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

template1 = """

<html>
<body>
    <p>Hi {{ to_name }},</p>

    <p>Warm Regards!!</p>

    <p> The average same day data upload adherence for {{date}} is <b>{{avg_adherence}}%</b> across <b>{{active_Se}}</b> SEs out of total <b>{{total_se}}</b> device wearing SEs.</p>

    <p>PFB the tabulur view of same day data upload adherence:</p>

    {{html_table}}

    <p>Record Hour Adherence (%) = Total Recorded Duration / Shift Duration * 100 </p>

    <p> Note: We aim for atleast 85% record hour adherence for each SE.</p>

    <p>Team<br>YOYO AI</p>
</body>
</html>

"""


template2 = """

<html>
<body>
    <p>Hi {{ to_name }},</p>

    <p>Warm Regards!!</p>

    <p>The same day data upload adherence for {{date}} is <b>0</b></p>

    <p> Note: We aim for atleast 85% record hour adherence for each SE.</p>

    <p>Team<br>YOYO AI</p>
</body>
</html>

"""

brand_details = {
    "Lenskart": {
        "to": [
            "rohit.chawla@lenskart.com",
            "suren@lenskart.com",
            "aakash.kathuria@lenskart.in",
            "faraza@valyoo.in"
        ],
        "main_person": "Aakash",
        "cc": ["harshal@goyoyo.ai", "rohan@goyoyo.ai", "nikhil@goyoyo.ai", "pranet@goyoyo.ai", "adarsh@goyoyo.ai"],
        "shift_duration": "8:00:00"
    },
    "Food Square": {
        "to": [
            "mayank@landcraftagro.com",
            "joe@landcraftretail.com",
            "shruti@landcraftretail.com"
        ],
        "main_person": "Mayank",
        "cc": ["harshal@goyoyo.ai", "rohan@goyoyo.ai", "nikhil@goyoyo.ai", "pranet@goyoyo.ai", "adarsh@goyoyo.ai"],
        "shift_duration": "8:00:00"
    },
    "Clovia": {
        "to": [
            "sshruti.k@clovia.com",
            "neha@clovia.com",
            "jaya@thejoop.ai",
            "shaireen.khan@clovia.com>"
        ],
        "main_person": "Neha",
        "cc": ["harshal@goyoyo.ai", "rohan@goyoyo.ai", "nikhil@goyoyo.ai", "pranet@goyoyo.ai", "adarsh@goyoyo.ai"],
        "shift_duration": "8:00:00"
    },
    "Sri Jagdamba Pearls": {
        "to": [
            "deepak.kumar@jpearls.com",
            "avanish@jpearls.com"
        ],
        "main_person": "Avanish",
        "cc": ["harshal@goyoyo.ai", "rohan@goyoyo.ai", "nikhil@goyoyo.ai", "pranet@goyoyo.ai", "adarsh@goyoyo.ai"],
        "shift_duration": "7:00:00"
    }
}

date_query = (datetime.now() - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
date_query_dt = datetime.strptime(date_query, "%Y-%m-%d")
weekday = date_query_dt.weekday()
start_of_week = date_query_dt - pd.Timedelta(days=(weekday + 1) % 7)
end_of_week = start_of_week + pd.Timedelta(days=6)
start_date_week = start_of_week.strftime('%Y-%m-%d')
end_date_week = end_of_week.strftime('%Y-%m-%d')


for brand, details in brand_details.items():
    brand_name = brand
    to_emails = details['to']
    cc_emails = details['cc']
    shift_duration = details['shift_duration']
    to_name = details['main_person']
    brand_lower = "_".join(brand.lower().split())
    
    connection = create_connection()
    # Check if the connection is still open
    if connection.closed == 0:
        cursor = connection.cursor()

    # Ensure any previously failed transaction is rolled back
    connection.rollback()

    # Set the date as current date - 2
    


    ## extracting top5 rlos on day basis##
    
    query1 = f"""


    select 
        se_name as "Staff Name",
        store_name as "Store Name",
        total_files as "Total Files",
        valid_files as "Total Valid Files",
        recorded_duration_hms as "Total Recorded Duration"
    from (select 
    sp_id,
    store_id,
    se_name,
    store_name
    from (	
        select 
            a.sp_id,
            a.store_id,
            b.name as se_name,
            c.name as store_name,
            ROW_NUMBER() OVER (
            PARTITION BY a.sp_id 
            ORDER BY (COALESCE(a.end_date, DATE '2099-12-31') = DATE '{date_query}') DESC, 
            a.start_date DESC     
            ) AS rn
        from salesperson_stores as a
        left join sales_person as b on a.sp_id = b.id
        left join store as c on a.store_id = c.id
        left join brand as d on c.brand_id = d.id
        where start_date <= '{date_query}' and COALESCE(a.end_date, '2099-12-31') >= '{date_query}' and d.name = '{brand_name}') as A
    where rn = 1) as A
    left join (
    select 
        sales_person_id,
        count(id) as total_files,
        TO_CHAR(make_interval(secs => SUM(recorded_duration_seconds)), 'HH24:MI:SS') AS recorded_duration_hms,
        sum(valid_file) as valid_files
    from (
        SELECT 
            sales_person_id,
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
        WHERE path LIKE '%{brand_lower}%' and date = '{date_query}' and date = createddate) as a 
        group by 1) as b 
        on a.sp_id = b. sales_person_id
        
    """
    #print(f"Executing SQL Query:\n{query1}")
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
        df1["Shift Duration"] = shift_duration
        df1["Recoded Hour Adherence (%)"] = (pd.to_timedelta(df1["Total Recorded Duration"]) / pd.to_timedelta(df1["Shift Duration"])*100).round(0)
        df1["Device Active"] = np.where(df1["Total Files"].notna() & (df1["Total Files"] != ""), "Yes", "No")
        df1["Total Blank Files"] = np.where(
            df1["Total Files"].notna() & (df1["Total Files"] != "") &
            df1["Total Valid Files"].notna() & (df1["Total Valid Files"] != ""),
            df1["Total Files"].astype(float) - df1["Total Valid Files"].astype(float),
            np.nan   # or 0 if you prefer
            )
        df1["Total Blank files (%)"] = np.where(
            df1["Total Files"].notna() & (df1["Total Files"] != "") &
            df1["Total Blank Files"].notna() & (df1["Total Blank Files"] != ""),
            ((df1["Total Blank Files"].astype(float) / df1["Total Files"].astype(float))*100).round(0),
            np.nan   # or 0 if you prefer
            )
        df1 = df1.applymap(lambda x: '' if pd.isna(x) else (int(x) if isinstance(x, (int, float)) and float(x).is_integer() else x))
        order = ["Store Name", "Staff Name", "Device Active", "Total Recorded Duration", "Recoded Hour Adherence (%)", "Total Files", "Total Valid Files", "Total Blank Files", "Total Blank files (%)", "Shift Duration"]
        df1 = df1[order]
        df1 = df1.sort_values(by=["Store Name"], ascending=True).reset_index(drop=True)
        
        avg_adherence = pd.to_numeric(df1["Recoded Hour Adherence (%)"].replace('', np.nan), errors="coerce").mean().round(0)
        active_Se = df1[df1["Device Active"] == "Yes"].shape[0]
        total_se = df1.shape[0]
        
    except Exception as e:
        print(f"Error encountered: {e}")
        connection.rollback()  # Rollback the transaction if an error occurs

    

    finally:
        cursor.close()

    cols = df1.columns
    rows_html = []

    for _, row in df1.iterrows():
        cells = []
        for col in cols:
            cell_value = row[col]
            style = "text-align:center;border:1px solid #000;"
            if col == "Shift Duration":
                style += "background-color:#E57373;color:#000;"       # pastel blue
                cells.append(f"<td style='{style}'>{cell_value}</td>")
            elif col == "Total Recorded Duration":
                style += "background-color:#F8C9D4;color:#000;"       # pastel green
                cells.append(f"<td style='{style}'>{cell_value}</td>")
            elif col == "Recoded Hour Adherence (%)":
                style += "background-color:#D8BFD8;color:#000;"       # pastel green
                try:
                    val = float(cell_value)
                    cell_value = f"{val:.0f}%"
                except (ValueError, TypeError):
                    # leave it as-is if it's empty, "-" or non-numeric
                    pass
                cells.append(f"<td style='{style}'>{cell_value}</td>")
            else:
                cells.append(f"<td style='{style}'>{cell_value}</td>")
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    html_table1 = ( 
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


    if (pd.to_numeric(df1["Total Files"], errors='coerce').sum() == 0):
        template = template2
        email_template = Template(template)
        email_content = email_template.render(
            date = dt_f,
            to_name = to_name
        )

    else:
        template = template1
        email_template = Template(template)
        email_content = email_template.render(
            date = dt_f,
            to_name = to_name,
            avg_adherence = int(avg_adherence) if not pd.isna(avg_adherence) else 0,
            active_Se = active_Se,
            total_se = total_se,
            html_table = html_table1
        )

    subject_template = '{{ brand_name }} Adherence Report: {{ start_date_week }} to {{ end_date_week }}'

        # Render the subject using Jinja2
    subject = Template(subject_template).render(
        date_query=dt_f,
        brand_name = brand_name.replace("_", " ").title(),
        start_date_week = ds_f,
        end_date_week = de_f
    )
    send_html_email_gmail_api(service, 'reports@goyoyo.ai', to_emails, cc_emails, subject, email_content)

