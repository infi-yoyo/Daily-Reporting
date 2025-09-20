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

#cc_emails = ["prakhar@goyoyo.ai", "nikhil@goyoyo.ai", "harshal@goyoyo.ai"]
cc_emails = []

#to_emails = ["mudita.gupta@bluestone.com"]
to_emails = ['adarsh@goyoyo.ai']

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

if connection.closed == 0:
    cursor = connection.cursor()

# Ensure any previously failed transaction is rolled back
connection.rollback()

# Set the date as current date - 2
date_query = (datetime.now() - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
date_query_dt = datetime.strptime(date_query, "%Y-%m-%d")
today = datetime.now()
start_of_month = date_query_dt.replace(day=1)
start_date_month = start_of_month.strftime('%Y-%m-%d')
date_2_days_ago = today - pd.Timedelta(days=2)
start_of_week = date_2_days_ago - pd.Timedelta(days=date_2_days_ago.weekday())
end_of_week = start_of_week + pd.Timedelta(days=6)
start_date_week = start_of_week.strftime('%Y-%m-%d')
end_date_week = end_of_week.strftime('%Y-%m-%d')


query1 = f"""

    SELECT 
	g.name as "ABM",
	count(distinct(c.id)) as "Store Count",
	count(distinct(b.sales_person_id)) as "Executive Count",
	count(a.id) as "Total Interaction", 
	COALESCE(SUM( (elem1->>'gms_pitched')::int ), 0) AS "GMS Pitched",
	COALESCE(SUM( (elem1->>'gms_sold')::int ), 0) AS "GMS Sold"
    FROM bluestone_interaction_flags as a 
    LEFT JOIN interaction_processed AS b on a.interaction_id = b.id
    LEFT JOIN store AS c ON b.store_id = c.id
    left join area_business_manager as f on c.abm_id = f.id
    left join users as g on f.user_id = g.id
    LEFT JOIN LATERAL jsonb_array_elements(a.sop_new) AS elem1 ON TRUE
    WHERE b.date = '{date_query}'  
    and cast(b.duration as integer) > 180000
    group by 1;
    
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
    df1['GMS Pitched (%)'] = np.where(df1['Total Interaction'] > 0,((df1['GMS Pitched'] / df1['Total Interaction']) * 100).round(),0).astype(int)
    df1['GMS Sold (%)'] = np.where(df1['GMS Pitched'] > 0,((df1['GMS Sold'] / df1['GMS Pitched']) * 100).round(),0).astype(int)
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


query2 = f"""

  SELECT 
	g.name as "ABM",
	count(distinct(c.id)) as "Store Count",
	count(distinct(b.sales_person_id)) as "Executive Count",
	count(a.id) as "Total Interaction", 
	COALESCE(SUM( (elem1->>'gms_pitched')::int ), 0) AS "GMS Pitched",
	COALESCE(SUM( (elem1->>'gms_sold')::int ), 0) AS "GMS Sold"
    FROM bluestone_interaction_flags as a 
    LEFT JOIN interaction_processed AS b on a.interaction_id = b.id
    LEFT JOIN store AS c ON b.store_id = c.id
    left join area_business_manager as f on c.abm_id = f.id
    left join users as g on f.user_id = g.id
    LEFT JOIN LATERAL jsonb_array_elements(a.sop_new) AS elem1 ON TRUE
    WHERE b.date between '{start_of_month}' and '{date_query}'  
    and cast(b.duration as integer) > 180000
    group by 1;
    
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
    df2['MTD GMS Pitched (%)'] = np.where(df2['Total Interaction'] > 0,((df2['GMS Pitched'] / df2['Total Interaction']) * 100).round(),0).astype(int)
    df2['MTD GMS Sold (%)'] = np.where(df2['GMS Pitched'] > 0,((df2['GMS Sold'] / df2['GMS Pitched']) * 100).round(),0).astype(int)
    df2 = df2.sort_values(by='MTD GMS Sold (%)', ascending=False)
    df3 = df2[['ABM', 'MTD GMS Pitched (%)', 'MTD GMS Sold (%)']]   
    
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs



finally:
    cursor.close()

merged_df = df3.merge(df1, on='ABM', how='left')
merged_df = merged_df.applymap(lambda x: '-' if pd.isna(x) else (int(x) if isinstance(x, (int, float)) and float(x).is_integer() else x))
merged_df = merged_df.sort_values(by='MTD GMS Sold (%)', ascending=False)
totals = pd.DataFrame({
    "ABM": ["Grand Total"],
    "Store Count": [df1["Store Count"].sum()],
    "Executive Count": [df1["Executive Count"].sum()],
    "Total Interaction": [df1["Total Interaction"].sum()],
    "GMS Pitched": [df1["GMS Pitched"].sum()],
    "GMS Sold": [df1["GMS Sold"].sum()],
    "MTD total Interaction": [df2["Total Interaction"].sum()],
    "MTD GMS Pitched": [df2["GMS Pitched"].sum()],
    "MTD GMS Sold": [df2["GMS Sold"].sum()]
    })

# Calculate percentages based on totals
totals["GMS Pitched (%)"] = round((totals["GMS Pitched"] / totals["Total Interaction"]) * 100, 0).astype(int)
totals["GMS Sold (%)"] = round((totals["GMS Sold"] / totals["Total Interaction"]) * 100, 0).astype(int)
totals["MTD GMS Pitched (%)"] = round((totals["MTD GMS Pitched"] / totals["MTD total Interaction"]) * 100, 0).astype(int)
totals["MTD GMS Sold (%)"] = round((totals["MTD GMS Sold"] / totals["MTD GMS Pitched"]) * 100, 0).astype(int)

# Append to df1
merged_df = pd.concat([merged_df, totals], ignore_index=True)
new_order = ['ABM', 'MTD GMS Pitched (%)', 'MTD GMS Sold (%)', 'Store Count', 'Executive Count', 'Total Interaction', 'GMS Pitched', 'GMS Pitched (%)', 'GMS Sold', 'GMS Sold (%)']
merged_df = merged_df[new_order]

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
    <p>Hi Bluestone,</p>

    <p>Warm Regards!!</p>

    <p>PFB the GMS pitched and GMS Sold on {{ date }} compared with MTD ( {{start_date_month}} to {{date}})</p>
    
    {{html_table1}}

    <div class="insight">
        <p>You can look for detailed analysis regarding these interactions on the dashboard</p>
        <p><strong>Link to dashboard:</strong> https://pilot.goyoyo.ai/ </p>
    </div>
    
    <p><strong>Note:</strong> These customer interactions lasted for more than three minutes.</p>

    <p>Regards,<br>Adarsh.</p>
</body>
</html>

"""

# Build HTML manually for df1
cols = merged_df.columns
rows_html = []

for _, row in merged_df.iterrows():
    is_total = str(row["ABM"]).strip().lower() == "grand total"
    cells = []
    for col in cols:
        style = "text-align:center;border:1px solid #000;"
        if is_total:
            style += "font-weight:600;"  # highlight Grand Total row

        if col == "MTD GMS Pitched (%)":
            style += "background-color:#E3F2FD;color:#000;"       # pastel blue
            cells.append(f"<td style='{style}'>{row[col]}</td>")
        elif col == "MTD GMS Sold (%)":
            style += "background-color:#E8F5E9;color:#000;"       # pastel green
            cells.append(f"<td style='{style}'>{row[col]}</td>")
        else:
            cells.append(f"<td style='{style}'>{row[col]}</td>")
    rows_html.append("<tr>" + "".join(cells) + "</tr>")

html_table1 = ( 
    "<table border='1' cellpadding='6' cellspacing='0' " 
    "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;width:100%;'>" 
    "<thead><tr>" 
    + "".join([f"<th>{c}</th>" for c in cols]) 
    + "</tr></thead><tbody>" 
    + "".join(rows_html) + "</tbody></table>" 
    )


email_template = Template(template)
email_content = email_template.render(
    #name=row['name'],  # Replace with dynamic client name if needed
    date=date_query,
    start_date_month=start_date_month,
    start_date_week = start_date_week,
    end_date_week = end_date_week,
    html_table1 = html_table1
    )


subject_template = 'BlueStone <> YOYO AI - Actionable Insights - {{ date_query }}'

# Render the subject using Jinja2
subject = Template(subject_template).render(date_query=date_query)

# Send the email
send_html_email_gmail_api(service, 'adarsh@goyoyo.ai', to_emails, cc_emails, subject, email_content)


