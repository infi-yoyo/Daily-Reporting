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

#to_emails = ['mudita.gupta@bluestone.com']
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
b.audio_url,
b.interaction_code,
b.start_time,
b.end_time,
b.date,
c.name AS se_name,
d.name AS store_name,
(elem->>'phone_number') AS phone_number,
(elem->>'exchange_context') AS exchange_context,
(elem1->>'category') AS category,
(elem1->>'sub_category') AS sub_category,
(elem1->>'reason') AS reason,
a.customer_objection_handling
FROM bluestone_interaction_flags AS a
LEFT JOIN interaction_processed AS b ON a.interaction_id = b.id
LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
LEFT JOIN store AS d ON b.store_id = d.id
LEFT JOIN LATERAL jsonb_array_elements(b.phone_number_discussion) AS elem ON TRUE
LEFT JOIN LATERAL jsonb_array_elements(a.reason_loss_of_sale) AS elem1 ON TRUE
WHERE b.date = '{date_query}'
AND CAST(b.duration AS INTEGER) > 180000
AND a.sales_outcome = 'sale_unsuccessful'
AND elem->>'phone_number_exchange' in ('1')
AND EXISTS (
SELECT 1
FROM jsonb_array_elements(a.reason_loss_of_sale) AS elem2
WHERE elem2->>'sub_category' IN ('Pending Follow up action', 'Deferred decision making', 'Technical/Process Issues')
);
    
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
    reason_summary = df1['sub_category'].value_counts().to_dict()
    actionable_insights = {}
    for reason, count in reason_summary.items():
        # Get the interaction codes for each reason
        interaction_codes = df1[df1['sub_category'] == reason]['interaction_code'].tolist()
        actionable_insights[reason] = {
            'count': count,
            'interaction_codes': ', '.join(interaction_codes)
        }
       
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs

query2 = f"""

SELECT
count (a.id) as interaction_count
FROM bluestone_interaction_flags AS a
LEFT JOIN interaction_processed AS b ON a.interaction_id = b.id
LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
LEFT JOIN store AS d ON b.store_id = d.id
WHERE b.date = '{date_query}'
AND CAST(b.duration AS INTEGER) > 180000
AND a.sales_outcome = 'sale_unsuccessful';
    
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
    df2 = pd.DataFrame(rows, columns=column_names)
    
       
    
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs


query3 = f"""

SELECT
count(b.interaction_code) as Count,
(elem1->>'sub_category') AS Category
FROM bluestone_interaction_flags AS a
LEFT JOIN interaction_processed AS b ON a.interaction_id = b.id
LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
LEFT JOIN store AS d ON b.store_id = d.id
LEFT JOIN LATERAL jsonb_array_elements(a.reason_loss_of_sale) AS elem1 ON TRUE
WHERE b.date between '{start_date_month}' and '{date_query}'
AND CAST(b.duration AS INTEGER) > 180000
AND a.sales_outcome = 'sale_unsuccessful'
group by 2
order by 1 desc
limit 3
    
"""

# Print the query to see the actual SQL string
#print(f"Executing SQL Query:\n{query3}")

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

query4 = f"""

SELECT
count(b.interaction_code) as Count,
(elem1->>'sub_category') AS Category
FROM bluestone_interaction_flags AS a
LEFT JOIN interaction_processed AS b ON a.interaction_id = b.id
LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
LEFT JOIN store AS d ON b.store_id = d.id
LEFT JOIN LATERAL jsonb_array_elements(a.reason_loss_of_sale) AS elem1 ON TRUE
WHERE b.date between '{start_date_week}' and '{date_query}'
AND CAST(b.duration AS INTEGER) > 180000
AND a.sales_outcome = 'sale_unsuccessful'
group by 2
order by 1 desc
limit 3
    
"""

# Print the query to see the actual SQL string
#print(f"Executing SQL Query:\n{query3}")

try:
    cursor.execute(query4)
    
    # Fetch the data
    rows = cursor.fetchall()
    
    # Extract column names
    column_names = [desc[0] for desc in cursor.description]
    # Create the DataFrame using data and column names
    df4 = pd.DataFrame(rows, columns=column_names)
        
except Exception as e:
    print(f"Error encountered: {e}")
    connection.rollback()  # Rollback the transaction if an error occurs

finally:
    cursor.close()


# %%
total_interactions = df2['interaction_count'][0]

total_interactions_phone_number = len(df1)

# Calculate the percentages for Pending Follow up action, Deferred Decision Making, and Technical Glitch
pending_count = actionable_insights.get('Pending Follow up action', {}).get('count', 0)
pending_perc = round((pending_count / total_interactions) * 100,1) if total_interactions > 0 else 0

deferred_count = actionable_insights.get('Deferred decision making', {}).get('count', 0)
deferred_perc = round((deferred_count / total_interactions) * 100, 1) if total_interactions > 0 else 0

glitch_count = actionable_insights.get('Technical Glitch', {}).get('count', 0)
glitch_perc = round((glitch_count / total_interactions) * 100, 1) if total_interactions > 0 else 0

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
    <p>Hi Wakefit,</p>

    <p>Warm Regards!!</p>

    <p>On {{ date }}, there were {{ total_interactions }} unsuccessful interactions, in which customers shared their phone numbers in {{ total_interactions_phone_number }} interactions. Interaction codes for customer-centric reasons for loss of sale are:</p>

    <table>
        <thead>
            <tr>
                <th>Reason for Loss of Sale</th>
                <th>Count</th>
                <th>%</th>
                <th>Interaction Codes</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>Pending Follow Up Action</strong></td>
                <td class="count">{{ pending_count }}</td>
                <td class="percentage">{{ pending_perc }}%</td>
                <td class="interaction-codes">{{ pending_interactions }}</td>
            </tr>
            <tr>
                <td><strong>Deferred Decision Making</strong></td>
                <td class="count">{{ deferred_count }}</td>
                <td class="percentage">{{ deferred_perc }}%</td>
                <td class="interaction-codes">{{ deferred_interactions }}</td>
            </tr>
            <tr>
                <td><strong>Technical Glitch</strong></td>
                <td class="count">{{ glitch_count }}</td>
                <td class="percentage">{{ glitch_perc }}%</td>
                <td class="interaction-codes">{{ glitch_interactions }}</td>
            </tr>
        </tbody>
    </table>
    
    <div class="insight">
        <p><strong>Actionable Insight:</strong> Calling these phone numbers will act as a follow-up to convert these lost sales.</p>
    </div>

    <p><strong>Also, PFB the top 3 Reason for loss of sale for WTD and MTD </strong></p>

    <p><strong>Current ongoing week ({{start_date_week}} to {{ end_date_week }}):</strong></p>
    {{html_table2}}
    
    <p><strong>Current ongoing Month ({{start_date_month}} to {{ date }}):</strong></p>
    {{html_table1}}
    
    <p><strong>Note:</strong> These customer interactions lasted for more than three minutes.</p>

    <p>Regards,<br>Adarsh.</p>
</body>
</html>

"""

# %%
#data = {
#   'name': ['Adnan', 'Santhosh', 'Rishabh', 'Dibyendu'],
#   'email': ['adnan.kazim@wakefit.co', 'santhosh.hd@wakefit.co', 'rishabh.sethi@wakefit.co', 'dibyendu.panda@wakefit.co']
#}

#df = pd.DataFrame(data)

#df

# %%

email_template = Template(template)
email_content = email_template.render(
    #name=row['name'],  # Replace with dynamic client name if needed
    date=date_query,
    start_date_month=start_date_month,
    start_date_week = start_date_week,
    end_date_week = end_date_week,
    total_interactions=total_interactions,
    total_interactions_phone_number = total_interactions_phone_number,
    pending_count= pending_count,
    pending_perc = pending_perc,
    pending_interactions=actionable_insights.get('Pending Follow up action', {}).get('interaction_codes', ''),
    deferred_count= deferred_count,
    deferred_perc = deferred_perc,
    deferred_interactions=actionable_insights.get('Deferred decision making', {}).get('interaction_codes', ''),
    glitch_count= glitch_count,
    glitch_perc = glitch_perc,
    glitch_interactions=actionable_insights.get('Technical/Process Issues', {}).get('interaction_codes', ''),
    html_table1 = df3.to_html(index=False),
    html_table2 = df4.to_html(index=False)
)

subject_template = 'Daily Performance Report - {{ date_query }}'

# Render the subject using Jinja2
subject = Template(subject_template).render(date_query=date_query)






# %%
# Send the email
send_html_email_gmail_api(service, 'adarsh@goyoyo.ai', to_emails, cc_emails, subject, email_content)
