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
        
        .definitions {
            background-color: #f8f9fa;
            border: 1px solid #dee2e6;
            padding: 20px;
            margin: 25px 0;
            border-radius: 6px;
        }
        
        .definitions h2 {
            color: #495057;
            margin-top: 0;
            margin-bottom: 15px;
            font-size: 18px;
        }
        
        .definitions p {
            margin: 10px 0;
            font-size: 14px;
        }
        
        p {
            margin: 15px 0;
        }
    </style>
</head>
<body>
    <p>Hi {{ brand }},</p>

    <p>Warm Regards!!</p>

    <p>On {{ date }}, there were {{ total_interactions }} interactions, in which {{ total_unsuccessful_interactions }} did not convert. </p>

    <p>PFB the contribution of different reason for loss of sale for the {{date}}</p>

    <table>
        <thead>
            <tr>
                <th>Reason for Loss of Sale</th>
                <th>Count</th>
                <th>% </th>
                <th>Interaction Codes</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td><strong>Customer</strong></td>
                <td class="count">{{ customer_count }}</td>
                <td class="percentage">{{ customer_perc }}%</td>
                <td class="interaction-codes">{{ customer_interactions }}</td>
            </tr>
            <tr>
                <td><strong>Product</strong></td>
                <td class="count">{{ product_count }}</td>
                <td class="percentage">{{ product_perc }}%</td>
                <td class="interaction-codes">{{ product_interactions }}</td>
            </tr>
            <tr>
                <td><strong>Process</strong></td>
                <td class="count">{{ process_count }}</td>
                <td class="percentage">{{ process_perc }}%</td>
                <td class="interaction-codes">{{ process_interactions }}</td>
            </tr>
            <tr>
                <td><strong>Price</strong></td>
                <td class="count">{{ price_count }}</td>
                <td class="percentage">{{ price_perc }}%</td>
                <td class="interaction-codes">{{ price_interactions }}</td>
            </tr>
            <tr>
                <td><strong>Salesperson</strong></td>
                <td class="count">{{ salesperson_count }}</td>
                <td class="percentage">{{ salesperson_perc }}%</td>
                <td class="interaction-codes">{{ salesperson_interactions }}</td>
            </tr>
        </tbody>
    </table>

    <div class="definitions">
        <h2><b>Definition of RLOS Categories</b></h2>
        <p><b>Customer:</b> The sale did not happen due to customer reasons (e.g., currently just browsing, end user not present, unspecified requirements, service request visit).</p>
        <p><b>Product:</b> The sale did not conclude due to product issues (e.g., size not available, not comfortable, lack of variety, specific product not available).</p>
        <p><b>Process:</b> The sale did not conclude due to organizational processes (e.g., high delivery time, system issues, unacceptable policy, mismatch between online and offline).</p>
        <p><b>Price:</b> The sale was not concluded due to price issues (e.g., budget mismatch, cheaper options, unacceptable payment structure).</p>
        <p><b>Salesperson:</b> The sale did not conclude due to salesperson fault (e.g., not showing relevant products, unable to explain offers/discounts, lacking product knowledge).</p>
    </div>

    <p><strong>Also, PFB the Reason for loss of sale for WTD ({{start_date_week}} to {{ end_date_week }}) and MTD ({{start_date_month}} to {{ date }}) </strong></p>

    {{html_table}}

    <p><strong>Regarding Objection:</strong> On {{ date }}, {{ objection_count }} objections were raised. PFB the objections raised and their handling</p>

    {{html_table2}}

    <div class="definitions">
        <h2><b>Definition of Objection Categories</b></h2>
        <p><b>Merchandise Issue:</b> The customer raised objection regarding products displayed.</p>
        <p><b>Price Issue:</b> Customer objected to high price of the products.</p>
        <p><b>Offers and Discount Issue:</b> The customer objected to missing offers/discounts/payment plans.</p>
        <p><b>After Sales Issue:</b> Dissatisfaction with support after purchase, such as repair, exchange, or warranty handling.</p>
        <p><b>Delivery Timeline Issue:</b> The customer raised objection regarding delivery timeline.</p>
        <p><b>Customization Issue:</b>  Customer raised objection regarding available customization options.</p>
        <p><b>Negative Past Experience:</b> Customer was dissatisfied with past negative experience and was concerned it would happen again.</p>
        <p><b>Others:</b> Anything which does not fall in the above categories.</p>
    </div>

    <div class="definitions">
        <h2><b>Definition of Objection Handling</b></h2>
        <p><b>Explanation:</b> The response primarily explains or clarifies something.</p>
        <p><b>Solution:</b> The response offers a concrete solution or action to address the objection.</p>
        <p><b>Generic:</b> The response is neither a clear explanation nor a solution.</p>
    </div>

	<p><strong>For detailed analysis, kindly log in to the dashboard by clicking here: https://pilot.goyoyo.ai/</strong></p>
 
    <p><strong>Note:</strong> These customer interactions lasted for more than three minutes.</p>

    <p>Regards,<br>Adarsh.</p>
</body>
</html>

"""

brands = ["lenskart"]

for brand in brands:
   
    print(brand)
    
    cc_emails = ["prakhar@goyoyo.ai", "nikhil@goyoyo.ai", "harshal@goyoyo.ai"]
    #cc_emails = ["adarsh@goyoyo.ai"]
    if brand == "lenskart":
        to_emails = ['aakash.kathuria@lenskart.in', 'suren@lenskart.com']
        #to_emails = ["adarsh@goyoyo.ai"]

    connection = create_connection()
    # Check if the connection is still open
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


    ## extracting top5 rlos on day basis##
    
    query1 = f"""

    SELECT
    b.interaction_code,
    (elem1->>'category') AS category
    FROM interaction_processed AS b 
    LEFT JOIN LATERAL jsonb_array_elements(b.reason_loss_of_sale) AS elem1 ON TRUE
    WHERE b.date = '{date_query}'
    AND CAST(b.duration AS INTEGER) > 180000
    AND b.sales_outcome = 'unsuccessful'
    and path like '%{brand}%';
        
    """
    print(f"Executing SQL Query:\n{query1}")
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
        reason_summary = df1['category'].value_counts().to_dict()
        actionable_insights = {}
        for reason, count in reason_summary.items():
            # Get the interaction codes for each reason
            interaction_codes = df1[df1['category'] == reason]['interaction_code'].tolist()
            actionable_insights[reason] = {
                'count': count,
                'interaction_codes': ', '.join(interaction_codes)
            }
        
        
    except Exception as e:
        print(f"Error encountered: {e}")
        connection.rollback()  # Rollback the transaction if an error occurs

    # extracting total interaction by sales outcome
    
    query2 = f"""

    SELECT
    count (b.interaction_code) as interaction_count, sales_outcome
    FROM interaction_processed AS b
    LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
    LEFT JOIN store AS d ON b.store_id = d.id
    WHERE b.date = '{date_query}'
    AND CAST(b.duration AS INTEGER) > 180000
    and path like '%{brand}%'
    group by 2;
        
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


    # extracting top3 rlos on month basis

    query3 = f"""

    SELECT
    CASE elem1->>'category'
        WHEN 'Customer'    THEN 'Customer'
        WHEN 'Product'     THEN 'Product'
        WHEN 'Process'     THEN 'Process'
        WHEN 'Price'       THEN 'Price'
        WHEN 'Salesperson' THEN 'Salesperson'
    END as category,
    count(b.interaction_code) as Count
    FROM interaction_processed AS b 
    LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
    LEFT JOIN store AS d ON b.store_id = d.id
    LEFT JOIN LATERAL jsonb_array_elements(b.reason_loss_of_sale) AS elem1 ON TRUE
    WHERE b.date between '{start_date_month}' and '{date_query}'
    and path like '%{brand}%'
    AND CAST(b.duration AS INTEGER) > 180000
    AND b.sales_outcome = 'unsuccessful'
    group by 1
    order by 2 desc
        
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

    # extracting top3 rlos on week basis
    
    query4 = f"""

    SELECT
    CASE elem1->>'category'
        WHEN 'Customer'    THEN 'Customer'
        WHEN 'Product'     THEN 'Product'
        WHEN 'Process'     THEN 'Process'
        WHEN 'Price'       THEN 'Price'
        WHEN 'Salesperson' THEN 'Salesperson'
    END as category,
    count(b.interaction_code) as Count
    FROM interaction_processed AS b
    LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
    LEFT JOIN store AS d ON b.store_id = d.id
    LEFT JOIN LATERAL jsonb_array_elements(b.reason_loss_of_sale) AS elem1 ON TRUE
    WHERE b.date between '{start_date_week}' and '{date_query}'
    and path like '%{brand}%'
    AND CAST(b.duration AS INTEGER) > 180000
    AND b.sales_outcome = 'unsuccessful'
    group by 1
    order by 2 desc
   
        
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

    query5 = f"""

    SELECT
    (elem1->>'category') as obj_category,
	(elem1->>'handling') as obj_handling,
	interaction_code
    FROM interaction_processed AS b
    LEFT JOIN sales_person AS c ON b.sales_person_id = c.id
    LEFT JOIN store AS d ON b.store_id = d.id
    LEFT JOIN LATERAL jsonb_array_elements(b.customer_objection_handling) AS elem1 ON TRUE
    WHERE b.date = '{date_query}'
    and path like '%{brand}%'
    AND CAST(b.duration AS INTEGER) > 180000
	and (elem1->>'category') is not null 
	and (elem1->>'category') not in ('not_applicable')
        
    """
    
    # Print the query to see the actual SQL string
    #print(f"Executing SQL Query:\n{query3}")

    try:
        cursor.execute(query5)
        
        # Fetch the data
        rows = cursor.fetchall()
        
        # Extract column names
        column_names = [desc[0] for desc in cursor.description]
        # Create the DataFrame using data and column names
        df6 = pd.DataFrame(rows, columns=column_names)
            
    except Exception as e:
        print(f"Error encountered: {e}")
        connection.rollback()  # Rollback the transaction if an error occurs


    finally:
        cursor.close()


    total_interactions = df2["interaction_count"].sum()

    total_unsuccessful_interactions = df2.loc[df2['sales_outcome'] == 'unsuccessful', 'interaction_count'].sum()

    total_objections = df6["interaction_code"].count()

    categories = ["Customer", "Product", "Process", "Price", "Salesperson"]

    df5 = pd.merge(df4, df3, on="category", how="outer")
    df5 = df5.rename(columns={"count_x": "WTD", "count_y": "MTD"})
    df5 = df5.set_index("category").reindex(categories).reset_index()
    df5[["MTD", "WTD"]] = df5[["MTD", "WTD"]].fillna(0).astype(int)

    results = {}

    for cat in categories:
        data = actionable_insights.get(cat, {})
        count = data.get("count", 0)
        perc = round((count / total_unsuccessful_interactions) * 100, 1) if total_unsuccessful_interactions > 0 else 0
        interactions = data.get("interaction_codes", "")
        
        results[cat.lower()] = {
            "count": count,
            "perc": perc,
            "interactions": interactions
        }
    
    objection_categories = [
    "merchandise_issue",
    "price_issue", 
    "others",
    "offers_and_discount_issue",
    "after_sales_issue",
    "customization_issue",
    "delivery_timeline_issue",
    "negative_past_experience"
    ]

    # Create mapping from category codes to display names
    category_mapping = {
        "merchandise_issue": "Merchandise Issue",
        "price_issue": "Price Issue",
        "others": "Others",
        "offers_and_discount_issue": "Offers and Discount Issue",
        "after_sales_issue": "After Sales Issue",
        "customization_issue": "Customization Issue",
        "delivery_timeline_issue": "Delivery Timeline Issue",
        "negative_past_experience": "Negative Past Experience"
    }

    # Group df6 by obj_category and obj_handling to get counts and interaction codes
    grouped = df6.groupby(['obj_category', 'obj_handling']).agg({
        'interaction_code': ['count', lambda x: ', '.join(x.astype(str))]
    }).reset_index()

    # Flatten column names
    grouped.columns = ['obj_category', 'obj_handling', 'count', 'interactions']

    # Create the new dataframe structure
    new_df_data = []

    for category in objection_categories:
        row = {'Objection Category': category_mapping[category]}  # Use mapped display name
        
        # Filter data for current category
        category_data = grouped[grouped['obj_category'] == category]
        
        # Initialize all counts to 0
        row['Explanation count'] = 0
        row['Explanation - Interactions'] = ''
        row['Solution Count'] = 0
        row['Solution - Interactions'] = ''
        row['Generic Count'] = 0
        row['Generic - Interactions'] = ''
        
        # Fill in actual data if category exists
        for _, group_row in category_data.iterrows():
            handling_type = group_row['obj_handling']
            count = group_row['count']
            interactions = group_row['interactions']
            
            if handling_type == 'Explanation':
                row['Explanation count'] = count
                row['Explanation - Interactions'] = interactions
            elif handling_type == 'Solution':
                row['Solution Count'] = count
                row['Solution - Interactions'] = interactions
            elif handling_type == 'Generic':
                row['Generic Count'] = count
                row['Generic - Interactions'] = interactions
        
        new_df_data.append(row)

    # Create the new dataframe
    df7 = pd.DataFrame(new_df_data)

    

    
    email_template = Template(template)
    email_content = email_template.render(
        #name=row['name'],  # Replace with dynamic client name if needed
        brand = brand.replace("_", " ").title(),
        date = date_query,
        start_date_month = start_date_month,
        start_date_week = start_date_week,
        end_date_week = end_date_week,
        total_interactions = total_interactions,
        total_unsuccessful_interactions = total_unsuccessful_interactions,
        customer_count = results["customer"]["count"],
        customer_perc = results["customer"]["perc"],
        customer_interactions = results["customer"]["interactions"],
        product_count = results["product"]["count"],
        product_perc = results["product"]["perc"],
        product_interactions = results["product"]["interactions"],
        process_count = results["process"]["count"],
        process_perc = results["process"]["perc"],
        process_interactions = results["process"]["interactions"],
        price_count = results["price"]["count"],
        price_perc = results["price"]["perc"],
        price_interactions = results["price"]["interactions"],
        salesperson_count = results["salesperson"]["count"],
        salesperson_perc = results["salesperson"]["perc"],
        salesperson_interactions = results["salesperson"]["interactions"],
        objection_count = total_objections,
        html_table = df5.to_html(index=False),
        html_table2 = df7.to_html(index=False)
    )

    subject_template = 'Lenskart <> YOYO AI - Actionable Insights - {{ date_query }}'

    # Render the subject using Jinja2
    subject = Template(subject_template).render(date_query=date_query)

    send_html_email_gmail_api(service, 'adarsh@goyoyo.ai', to_emails, cc_emails, subject, email_content)
    
