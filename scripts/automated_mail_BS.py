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

cc_emails = ["prakhar@goyoyo.ai", "nikhil@goyoyo.ai", "harshal@goyoyo.ai"]
#cc_emails = []

to_emails = ["mudita.gupta@bluestone.com"]
#to_emails = ['adarsh@goyoyo.ai']

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



# %%
connection = create_connection()
# Check if the connection is still open
if connection.closed == 0:
    cursor = connection.cursor()

# Ensure any previously failed transaction is rolled back
connection.rollback()

# Set the date as current date - 2
date_query = (datetime.now() - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
today = datetime.now()
start_of_month = today.replace(day=1)
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
    df1 = df1.sort_values(by='Store Count', ascending=False)
    totals = pd.DataFrame({
    "ABM": ["Grand Total"],
    "Store Count": [df1["Store Count"].sum()],
    "Executive Count": [df1["Executive Count"].sum()],
    "Total Interaction": [df1["Total Interaction"].sum()],
    "GMS Pitched": [df1["GMS Pitched"].sum()],
    "GMS Sold": [df1["GMS Sold"].sum()]
    })

# Calculate percentages based on totals
    totals["GMS Pitched (%)"] = round((totals["GMS Pitched"] / totals["Total Interaction"]) * 100, 0).astype(int)
    totals["GMS Sold (%)"] = round((totals["GMS Sold"] / totals["Total Interaction"]) * 100, 0).astype(int)

    # Append to df1
    df1 = pd.concat([df1, totals], ignore_index=True)
    
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


query2 = f"""

   select
  count(interaction_code) as "Total Interaction",
  sum(case when a.sales_outcome = 'sale_unsuccessful' then 1 else 0 end) as "Unuccessful Interactions",
  (elem1 ->> 'item_type') as "Design",
  (elem1 ->> 'price_range') as "Price Range"
  FROM bluestone_interaction_flags as a 
  LEFT JOIN interaction_processed AS b on a.interaction_id = b.id
  LEFT JOIN LATERAL jsonb_array_elements(a.new_products_tried) AS elem1 ON TRUE
  WHERE b.date = '{date_query}'  
  and cast(b.duration as integer) > 180000
  group by 3,4;
    
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
    
    price_order = ["<20K", "20K - 50K", "50K - 1L", "1L - 2L", "2L - 5L", "5L - 10L", ">10L"]

    # normalize some common variants
    norm_map = {
        "<20K": "<20K", "20K-50K": "20K - 50K", "20k-50k": "20K - 50K",
        "50K-1L": "50K - 1L", "50k-1L": "50K - 1L",
        "1L-2L": "1L - 2L", "2L-5L": "2L - 5L", "5L-10L": "5L - 10L",
        ">10L": ">10L"
    }
    df2 = df2.copy()
    df2["price_range"] = df2["Price Range"].map(lambda x: norm_map.get(str(x).strip(), x))

    # keep only valid ranges
    dfw = df2[df2["price_range"].isin(price_order)].copy()

    # pivots
    p_inter = pd.pivot_table(
        dfw, index="Design", columns="price_range",
        values="Total Interaction", aggfunc="sum", fill_value=0
    ).reindex(columns=price_order, fill_value=0)

    p_succ = pd.pivot_table(
        dfw, index="Design", columns="price_range",
        values="Unuccessful Interactions", aggfunc="sum", fill_value=0
    ).reindex(columns=price_order, fill_value=0)

    # build multiindex columns
    cols, data = [], {}
    for pr in price_order:
        cols.extend([(pr, "Total Interaction"), (pr, "Unuccessful")])
        data[(pr, "Total Interaction")] = p_inter[pr]
        data[(pr, "Unuccessful")] = p_succ[pr]

    # add totals across ranges (row total per design)
    data[("Total", "Total Interaction")] = p_inter.sum(axis=1)
    data[("Total", "Unuccessful")] = p_succ.sum(axis=1)
    cols.extend([("Total", "Total Interaction"), ("Total", "Unuccessful")])

    df_design = pd.DataFrame(data, index=p_inter.index)
    df_design = df_design.reindex(columns=pd.MultiIndex.from_tuples(cols))
    df_design.index.name = "Design"

    # add one grand total row (sum across all designs)
    grand_total = df_design.sum(numeric_only=True)
    df_design.loc["Grand Total"] = grand_total
    df_design = sort_by_total_interaction(df_design, ascending=False)
    df_design = df_design.mask(df_design.eq(0), '-')
    

    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


query3 = f"""

   select
  count(interaction_code) as "Total Interaction",
  sum(case when a.sales_outcome = 'sale_unsuccessful' then 1 else 0 end) as "Unsuccessful Interactions",
  (elem1 ->> 'material') as "Item",
  (elem1 ->> 'price_range') as "Price Range"
  FROM bluestone_interaction_flags as a 
  LEFT JOIN interaction_processed AS b on a.interaction_id = b.id
  LEFT JOIN LATERAL jsonb_array_elements(a.new_products_tried) AS elem1 ON TRUE
  WHERE b.date = '{date_query}'  
  and cast(b.duration as integer) > 180000
  group by 3,4;
    
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
    
    price_order = ["<20K", "20K - 50K", "50K - 1L", "1L - 2L", "2L - 5L", "5L - 10L", ">10L"]

# normalize some common variants
    norm_map = {
        "<20K": "<20K", "20K-50K": "20K - 50K", "20k-50k": "20K - 50K",
        "50K-1L": "50K - 1L", "50k-1L": "50K - 1L",
        "1L-2L": "1L - 2L", "2L-5L": "2L - 5L", "5L-10L": "5L - 10L",
        ">10L": ">10L"
    }
    df3 = df3.copy()
    df3["price_range"] = df3["Price Range"].map(lambda x: norm_map.get(str(x).strip(), x))

    # keep only valid ranges
    dfw = df3[df3["price_range"].isin(price_order)].copy()

    # pivots
    p_inter = pd.pivot_table(
        dfw, index="Item", columns="price_range",
        values="Total Interaction", aggfunc="sum", fill_value=0
    ).reindex(columns=price_order, fill_value=0)

    p_succ = pd.pivot_table(
        dfw, index="Item", columns="price_range",
        values="Unsuccessful Interactions", aggfunc="sum", fill_value=0
    ).reindex(columns=price_order, fill_value=0)

    # build multiindex columns
    cols, data = [], {}
    for pr in price_order:
        cols.extend([(pr, "Total Interaction"), (pr, "Unuccessful")])
        data[(pr, "Total Interaction")] = p_inter[pr]
        data[(pr, "Unuccessful")] = p_succ[pr]

    # add totals across ranges (row total per design)
    data[("Total", "Total Interaction")] = p_inter.sum(axis=1)
    data[("Total", "Unuccessful")] = p_succ.sum(axis=1)
    cols.extend([("Total", "Total Interaction"), ("Total", "Unuccessful")])

    df_item = pd.DataFrame(data, index=p_inter.index)
    df_item = df_item.reindex(columns=pd.MultiIndex.from_tuples(cols))
    df_item.index.name = "Item"

    # add one grand total row (sum across all designs)
    grand_total = df_item.sum(numeric_only=True)
    df_item.loc["Grand Total"] = grand_total
    df_item = sort_by_total_interaction(df_item, ascending=False)
    df_item = df_item.mask(df_item.eq(0), '-')
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs



finally:
    cursor.close()


# %%
template = """
<html>
<head>
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }
        
        th, td {
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
            vertical-align: top;
        }
        
        th {
            background-color: #f8f9fa;
            font-weight: bold;
            color: #495057;
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
        
        p {
            margin: 15px 0;
        }
    </style>
</head>
<body>
    <p>Hi Bluestone,</p>

    <p>Warm Regards!!</p>

    <p>PFB the insights based on the customer interactions that took place on {{ date }}</p>
    
    <p><strong>GMS</strong>
    {{html_table1}}

    <p><strong>Conversion Rate by Design</strong>
    {{html_table2}}

    <p><strong>Conversion Rate by Item</strong>
    {{html_table3}}
    
    <div class="insight">
        <p>You can look for detailed analysis regarding these interactions on the dashboard</p>
        <p><strong>Link to dashboard:</strong> https://pilot.goyoyo.ai/ </p>
    </div>
    
    <p><strong>Note:</strong> These customer interactions lasted for more than three minutes.</p>

    <p>Regards,<br>Adarsh.</p>
</body>
</html>

"""

email_template = Template(template)
email_content = email_template.render(
    #name=row['name'],  # Replace with dynamic client name if needed
    date=date_query,
    start_date_month=start_date_month,
    start_date_week = start_date_week,
    end_date_week = end_date_week,
    html_table1 = df1.to_html(index=False),
    html_table2 = df_design.reset_index().rename(columns={'index':'Design'}).to_html(index=False),
    html_table3 = df_item.reset_index().rename(columns={'index':'Item'}).to_html(index=False)
    )


subject_template = 'BlueStone <> YOYO AI - Actionable Insights - {{ date_query }}'

# Render the subject using Jinja2
subject = Template(subject_template).render(date_query=date_query)






# %%
# Send the email
send_html_email_gmail_api(service, 'adarsh@goyoyo.ai', to_emails, cc_emails, subject, email_content)
