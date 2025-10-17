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

    <strong>Adherence Report - {{date}}</strong>

    <p>The average same day data upload adherence for {{date}} is <b>{{avg_adherence}}%</b> across <b>{{active_Se}}</b> SEs out of total <b>{{total_se}}</b> device wearing SEs.</p>

    <p>PFB the tabulur view of same day data upload adherence:</p>

    {{html_table1}}

    <p> Where, D-1 stands for {{date}} and so on. Week starts from Sunday to Saturday. Week 1 starts from 1st of every month <br>

    Record Hour Adherence (%) = Total Recorded Duration / Shift Duration * 100 <br>

    Note: We aim for atleast 85% record hour adherence for each SE.</p>
    

    <strong>Actionable Insights</strong>

    <p>PFB the contribution of different reason for loss of sale for the month</p>

    {{html_table2}}

    <p>PFB the contribution of different objections for the month</p>

    {{html_table3}}

    
    <p>
        <p><b>Definition of Reason for loss of sale Categories</b><br></p>
        <strong>Customer:</strong> The sale did not take place due to cusotmer centric reason for loss of sale.<br>
        <strong>Process:</strong> The sale did not take place due to organizational processes.<br>
        <strong>People:</strong> The sale did not take place due to staff in the store.<br>
        <strong>Price:</strong> The sale did not take place due to price of the product.<br>
        <strong>Product:</strong> The sale did not take place due to product limitation.<br>
    </p>
    
    <p>
        <p><b>Definition of Objection Categories</b><br></p>
        <strong>Merchandise Issue:</strong> The customer raised objection regarding products displayed.<br>
        <strong>Price Issue:</strong> Customer objected to high price of the products.<br>
        <strong>Offers and Discount Issue:</strong> The customer objected to missing offers/discounts/payment plans.<br>
        <strong>After Sales Issue:</strong> Dissatisfaction with support after purchase, such as repair, exchange, or warranty handling.<br>
        <strong>Delivery Timeline Issue:</strong> The customer raised objection regarding delivery timeline.<br>
        <strong>Customization Issue:</strong>  Customer raised objection regarding available customization options.<br>
        <strong>Negative Past Experience:</strong> Customer was dissatisfied with past negative experience and was concerned it would happen again.<br>
        <strong>Others:</strong> Anything which does not fall in the above categories.</br>
    </p>

    <p>
        <p><b>Definition of Objection Handling</b><br></p>
        <strong>Explanation:</strong> The response primarily explains or clarifies something.<br>
        <strong>Solution:</strong> The response offers a concrete solution or action to address the objection.<br>
        <strong>Generic:</strong> The response is neither a clear explanation nor a solution.<br>
    </p>
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

    <p><strong>Actionable Insights</strong></p>

    <p>PFB the contribution of different reason for loss of sale for the month</p>

    {{html_table2}}

    <p>PFB the contribution of different objections for the month</p>

    {{html_table3}}

    <p>
        <p><b>Definition of Reason for loss of sale Categories</b><br></p>
        <strong>Customer:</strong> The sale did not take place due to cusotmer centric reason for loss of sale.<br>
        <strong>Process:</strong> The sale did not take place due to organizational processes.<br>
        <strong>People:</strong> The sale did not take place due to staff in the store.<br>
        <strong>Price:</strong> The sale did not take place due to price of the product.<br>
        <strong>Product:</strong> The sale did not take place due to product limitation.<br>
    </p>
    
    <p>
        <p><b>Definition of Objection Categories</b><br></p>
        <strong>Merchandise Issue:</strong> The customer raised objection regarding products displayed.<br>
        <strong>Price Issue:</strong> Customer objected to high price of the products.<br>
        <strong>Offers and Discount Issue:</strong> The customer objected to missing offers/discounts/payment plans.<br>
        <strong>After Sales Issue:</strong> Dissatisfaction with support after purchase, such as repair, exchange, or warranty handling.<br>
        <strong>Delivery Timeline Issue:</strong> The customer raised objection regarding delivery timeline.<br>
        <strong>Customization Issue:</strong>  Customer raised objection regarding available customization options.<br>
        <strong>Negative Past Experience:</strong> Customer was dissatisfied with past negative experience and was concerned it would happen again.<br>
        <strong>Others:</strong> Anything which does not fall in the above categories.</br>
    </p>

    <p>
        <p><b>Definition of Objection Handling</b><br></p>
        <strong>Explanation:</strong> The response primarily explains or clarifies something.<br>
        <strong>Solution:</strong> The response offers a concrete solution or action to address the objection.<br>
        <strong>Generic:</strong> The response is neither a clear explanation nor a solution.<br>
    </p>

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
date_query_dt = pd.to_datetime(date_query)
total_days = (date_query_dt - date_query_dt.replace(day=1)).days
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
    SHIFT_DURATION_SECONDS = pd.to_timedelta(shift_duration).total_seconds()

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
    e.name as "Name", 
    d.name as "Store",
    b.name as "Device",
    date,
    EXTRACT(EPOCH FROM a.duration::interval) AS recorded_duration_seconds,
    CASE 
       	WHEN a.duration IS NOT NULL  
        AND EXTRACT(EPOCH FROM a.duration::interval) > 0
        AND (cast(a.speech_duration as integer) / EXTRACT(EPOCH FROM a.duration::interval) < 0.1 or
			 cast(a.speech_duration as integer) / EXTRACT(EPOCH FROM a.duration::interval) > 1)
        THEN 1 
        ELSE 0 
        END AS "Blank File"
    from audio_file as a
    left join device as b on a.sales_person_id = b.sales_person_id and a.date >= b.assigned_on and a.date <= coalesce(b.unassigned_on, DATE '2099-12-31')
    left join salesperson_stores as c on a.sales_person_id = c.sp_id and a.date >= c.start_date and a.date <= coalesce(c.end_date, DATE '2099-12-31')
    left join store as d on c.store_id = d.id
    left join sales_person as e on a.sales_person_id = e.id
    where path like '%{brand_lower}%' 
    and date between date_trunc('month', DATE '{date_query}') and '{date_query}'
    and date = createddate
    and SUBSTRING(filename FROM 10 FOR 6) BETWEEN '093000' AND '223000'
	
        
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
        
        
    except Exception as e:
        print(f"Error encountered: {e}")
        connection.rollback()  # Rollback the transaction if an error occurs


    query2 = f"""

    SELECT
    (elem1 ->> 'category') AS "Reason for Loss of Sale",
    date
    FROM interaction_processed
    LEFT JOIN LATERAL jsonb_array_elements(reason_loss_of_sale) AS elem1 ON TRUE
    WHERE date >= DATE_TRUNC('month', DATE '{date_query}')
    AND date <= '{date_query}'
    AND path LIKE '%{brand_lower}%'
	AND sales_outcome = 'unsuccessful'
	AND cast(duration as integer) > 180000
    
    """
    #print(f"Executing SQL Query:\n{query1}")
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

    SELECT
    (elem1 ->> 'category') AS "Objection Category",
    date
    FROM interaction_processed
    LEFT JOIN LATERAL jsonb_array_elements(customer_objection_handling) AS elem1 ON TRUE
    WHERE date >= DATE_TRUNC('month', DATE '{date_query}')
    AND date <= '{date_query}'
    AND path LIKE '%{brand_lower}%'
	AND cast(duration as integer) > 180000
    
    """
    #print(f"Executing SQL Query:\n{query1}")
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

    get_week = lambda d: f"Week {((pd.Timestamp(d) - (pd.Timestamp(d).replace(day=1) - pd.Timedelta(days=((pd.Timestamp(d).replace(day=1).weekday() + 1) % 7 + 1))).normalize()).days // 7) + 1}"
    get_day_label = lambda d, today=pd.Timestamp.today().normalize(): f"D-{(today - pd.Timestamp(d).normalize()).days}" if 1 <= (today - pd.Timestamp(d).normalize()).days <= 4 else "D-X"
    df1['recorded_duration_seconds'] = df1['recorded_duration_seconds'].astype(float)
    df1['week'] = df1['date'].apply(get_week)
    df1['day_label'] = df1['date'].apply(get_day_label)
    df2['week'] = df2['date'].apply(get_week)
    df2['day_label'] = df2['date'].apply(get_day_label)
    df2['Contribution (%)'] = (df2['Reason for Loss of Sale'].map(df2['Reason for Loss of Sale'].value_counts(normalize=True).mul(100).round(1)))
    df3['week'] = df3['date'].apply(get_week)
    df3['day_label'] = df3['date'].apply(get_day_label)
    df3['Contribution (%)'] = (df3['Objection Category'].map(df3['Objection Category'].value_counts(normalize=True).mul(100).round(1)))
    

    if df1.empty:
        blank_perc = pd.DataFrame(columns=['Store', 'Name', 'Device', 'Blank_Files', 'Total_Files', 'Blank_Percentage'])
    else:
        blank = (
            df1.groupby(['Store', 'Name', 'Device'])
            .agg(Blank_Files=('Blank File', 'sum'))
            .reset_index()
        )

        total = (
            df1.groupby(['Store','Name', 'Device'])
            .agg(Total_Files=('Blank File', 'count'))
            .reset_index()
        )

        blank_perc = blank.merge(total, on=['Store', 'Name', 'Device'], how='outer')

        blank_perc['Blank_Percentage'] = (
            (blank_perc['Blank_Files'] / blank_perc['Total_Files'])
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0)
            * 100
        )

        blank_perc['Blank_Percentage'] = (
            pd.to_numeric(blank_perc['Blank_Percentage'], errors='coerce')
            .round(1)
            .fillna(0)
            .astype(str) + '%'
        )


    mtd = (
    df1.groupby(['Store', 'Name', 'Device'])
      .agg(MTD_seconds=('recorded_duration_seconds', 'sum'))
      .reset_index()
)

    mtd['active_days'] = total_days


    mtd['MTD (hrs)'] = (
        (mtd['MTD_seconds'] // 3600).astype(int).astype(str) + ':' +
        ((mtd['MTD_seconds'] % 3600) // 60).astype(int).astype(str).str.zfill(2) + ':' +
        (mtd['MTD_seconds'] % 60).round().astype(int).astype(str).str.zfill(2)
    )

    mtd['MTD Adherence %'] = (
        (mtd['MTD_seconds'] / (mtd['active_days'] * SHIFT_DURATION_SECONDS) * 100)
        .round(1).astype(str) + '%'
    )

    mtd_rlos = (
    df2.groupby('Reason for Loss of Sale')
      .size()
      .reset_index(name='MTD')
    )

    mtd_obj = (
    df3.groupby('Objection Category')
      .size()
      .reset_index(name='MTD')
    )

    week_pivot = (
        df1.groupby(['Store','Name', 'Device', 'week'])['recorded_duration_seconds']
        .sum()
        .reset_index()
        .pivot(index=['Store','Name', 'Device'], columns='week', values='recorded_duration_seconds')
        .fillna(0)
        .reset_index()
    )


    for c in week_pivot.columns[3:]:
        week_pivot[c] = (week_pivot[c] / (7 * SHIFT_DURATION_SECONDS) * 100).round(1).astype(str) + '%'

    week_counts_rlos = (
    df2.groupby(['Reason for Loss of Sale', 'week'])
      .size()
      .unstack(fill_value=0)
      .reset_index()
    )

    week_counts_obj = (
    df3.groupby(['Objection Category', 'week'])
      .size()
      .unstack(fill_value=0)
      .reset_index()
    )

    day_pivot = (
        df1.groupby(['Store','Name', 'Device', 'day_label'])['recorded_duration_seconds']
        .sum()
        .reset_index()
        .pivot(index=['Store',	'Name', 'Device'], columns='day_label', values='recorded_duration_seconds')
        .fillna(0)
        .reset_index()
    )


    for c in [col for col in day_pivot.columns if col.startswith('D-')]:
        day_pivot[c] = (day_pivot[c] / SHIFT_DURATION_SECONDS * 100).round(1).astype(str) + '%'

    day_counts_rlos = (
    df2.groupby(['Reason for Loss of Sale', 'day_label'])
      .size()
      .unstack(fill_value=0)
      .reset_index()
    )

    day_counts_obj = (
    df3.groupby(['Objection Category', 'day_label'])
      .size()
      .unstack(fill_value=0)
      .reset_index()
    )

    contribution_rlos = (
        df2.groupby('Reason for Loss of Sale')['Contribution (%)'].max().reset_index()
        )
    
    contribution_rlos['Contribution (%)'] = (
    contribution_rlos['Contribution (%)'].round(1).astype(float).astype(str) + '%'
    )

    contribution_obj = (
        df3.groupby('Objection Category')['Contribution (%)'].max().reset_index()
        )
    
    contribution_obj['Contribution (%)'] = (
    contribution_obj['Contribution (%)'].round(1).astype(float).astype(str) + '%'
    )

    final = (
        mtd.merge(blank_perc, on=['Store','Name', 'Device'], how='left')
        .merge(week_pivot, on=['Store','Name', 'Device'], how='left')
        .merge(day_pivot, on=['Store','Name', 'Device'], how='left')
        .fillna(0)
    )

    week_cols = sorted([c for c in final.columns if str(c).startswith('Week')])

    day_cols = [f'D-{i}' for i in range(1, 5)]
    for c in day_cols:
        if c not in final.columns:
            final[c] = "-"

    final = final[['Store','Name', 'Device', 'MTD (hrs)', 'MTD Adherence %', 'Blank_Percentage'] + week_cols + day_cols]
    final.rename(columns={'Blank_Percentage': 'Blank Files (%)'}, inplace=True)
    df_adherence = final.copy()


    final = (
    mtd_rlos.merge(contribution_rlos, on='Reason for Loss of Sale', how='left')
       .merge(week_counts_rlos, on='Reason for Loss of Sale', how='left')
       .merge(day_counts_rlos, on='Reason for Loss of Sale', how='left')
       .fillna(0)
    )

    week_cols = [c for c in final.columns if c.startswith('Week')]
    day_cols = [f'D-{i}' for i in range(1, 5)]
    for c in day_cols:
        if c not in final.columns:
            final[c] = "-"
    final = final[['Reason for Loss of Sale', 'MTD', 'Contribution (%)'] + week_cols + day_cols]
    df_rlos = final.copy()

    final = (
    mtd_obj.merge(contribution_obj, on='Objection Category', how='left')
       .merge(week_counts_obj, on='Objection Category', how='left')
       .merge(day_counts_obj, on='Objection Category', how='left')
       .fillna(0)
    )

    week_cols = [c for c in final.columns if c.startswith('Week')]
    day_cols = [f'D-{i}' for i in range(1, 5)]
    for c in day_cols:
        if c not in final.columns:
            final[c] = "-"
    final = final[['Objection Category', 'MTD', 'Contribution (%)'] + week_cols + day_cols]
    df_obj = final.copy()

    df_obj['Objection Category'] = (df_obj['Objection Category'].str.replace('_', ' ', regex=False).str.title())
    
    d1_numeric = pd.to_numeric(df_adherence["D-1"].astype(str).str.replace('%', ''), errors="coerce")
    avg_adherence = round(d1_numeric.mean(), 0) if not d1_numeric.empty else 0
    active_Se = int((d1_numeric > 0).sum()) if not d1_numeric.empty else 0
    total_se = int(df_adherence.shape[0]) if not df_adherence.empty else 0

    cols = df_adherence.columns
    rows_html = []

    for _, row in df_adherence.iterrows():
        cells = []
        for col in cols:
            cell_value = row[col]
            style = "text-align:center;border:1px solid #000;"
            cells.append(f"<td style='{style}'>{cell_value}</td>")
            
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    html_table_adherence = ( 
        "<table border='1' cellpadding='6' cellspacing='0' " 
        "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;width:100%;'>" 
        "<thead><tr>" 
        + "".join([f"<th>{c}</th>" for c in cols]) 
        + "</tr></thead><tbody>" 
        + "".join(rows_html) + "</tbody></table>" 
        )
    
    cols = df_rlos.columns
    rows_html = []

    for _, row in df_rlos.iterrows():
        cells = []
        for col in cols:
            cell_value = row[col]
            style = "text-align:center;border:1px solid #000;"
            cells.append(f"<td style='{style}'>{cell_value}</td>")
            
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    html_table_rlos = ( 
        "<table border='1' cellpadding='6' cellspacing='0' " 
        "style='border-collapse:collapse;font-family:Arial,sans-serif;font-size:13px;width:100%;'>" 
        "<thead><tr>" 
        + "".join([f"<th>{c}</th>" for c in cols]) 
        + "</tr></thead><tbody>" 
        + "".join(rows_html) + "</tbody></table>" 
        )
    
    cols = df_obj.columns
    rows_html = []

    for _, row in df_obj.iterrows():
        cells = []
        for col in cols:
            cell_value = row[col]
            style = "text-align:center;border:1px solid #000;"
            cells.append(f"<td style='{style}'>{cell_value}</td>")
            
        rows_html.append("<tr>" + "".join(cells) + "</tr>")

    html_table_obj = ( 
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


    if active_Se == 0:
        template = template2
        email_template = Template(template)
        email_content = email_template.render(
            date = dt_f,
            to_name = to_name,
            html_table2 =  html_table_rlos, 
            html_table3 = html_table_obj 
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
            html_table1 = html_table_adherence,
            html_table2 =  html_table_rlos, 
            html_table3 = html_table_obj
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
        

