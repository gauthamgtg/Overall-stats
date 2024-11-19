from curses.ascii import alt
from datetime import date, datetime, timedelta
from urllib.error import URLError
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import psycopg2
from functools import wraps
import pandas as pd
import hmac
import boto3
import json

client = boto3.client(
    "secretsmanager",
    region_name=st.secrets["AWS_DEFAULT_REGION"],
    aws_access_key_id=st.secrets["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=st.secrets["AWS_SECRET_ACCESS_KEY"]
)

def get_secret(secret_name):
    # Retrieve the secret value
    response = client.get_secret_value(SecretId=secret_name)
    return json.loads(response["SecretString"])

# Replace 'your-secret-name' with the actual secret name in AWS Secrets Manager
secret = get_secret("G-streamlit-KAT")
db = secret["db"]
name = secret["name"]
passw = secret["passw"]
server = secret["server"]
port = secret["port"]



st.set_page_config( page_title = "Spend Stats",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded")

# st.toast('Successfully connected to the database!!', icon='😍')

st.write("Successfully connected to the database!")

def redshift_connection(dbname, user, password, host, port):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:

                connection = psycopg2.connect(
                    dbname=dbname,
                    user=user,
                    password=password,
                    host=host,
                    port=port
                )

                cursor = connection.cursor()

                print("Connected to Redshift!")

                result = func(*args, connection=connection, cursor=cursor, **kwargs)

                cursor.close()
                connection.close()

                print("Disconnected from Redshift!")

                return result

            except Exception as e:
                print(f"Error: {e}")
                return None

        return wrapper

    return decorator

query = '''
SELECT euid,ad_account,date(payment_date)as dt, total_amount,receiver_id,currency,
gateway_charge,adspend_amount,processing_fee,tax,convenience_fee,'enterprise' as flag
from payment_trans_details

union all

SELECT user_id as euid,ad_account_id,date(created_at)as dt, total_amount,payment_id as receiver_id,currency,
gateway_processing_fee as gateway_charge,final_adspend_amount as adspend_amount,overage_fee as processing_fee,tax,
0 as convenience_fee,'zocket.ai' as flag
FROM
(
SELECT 
			poa.id, 
			poa.total_amount, 
			poa.tax,
			poa.overage_fee,
			DATE(DATEADD(minute, 330, poa.created_at)) AS created_at, 
			pt.gateway_processing_fee, 
			poa.ad_account_id, 
			poa.business_id,
			poa.user_id,
			poa.final_adspend_amount, 
			pt.currency,
			poa.api_status,
			DATE(DATEADD(minute, 330, pt.payment_date)) AS payment_date,
			bu.email,
			bu.name,
			faa.name,
			pt.payment_id
		FROM zocket_global.wallet_payment_order_adspend poa
		INNER JOIN zocket_global.wallet_payment_transactions pt ON poa.Payment_transaction_id = pt.id
		JOIN zocket_global.business_users bu on bu.id = poa.user_id
		JOIN zocket_global.fb_ad_accounts faa on faa.ad_account_id = concat('act_', poa.ad_account_id)
		WHERE poa.status = '1'
		ORDER BY poa.id DESC 
)

'''

@st.cache_data(ttl=86400)  # 86400 seconds = 24 hours
@redshift_connection(db,name,passw,server,port)
def execute_query(connection, cursor,query):

    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    result = pd.DataFrame(cursor.fetchall(), columns=column_names)

    return result

df = execute_query(query=query)

st.dataframe(df,use_container_width=True)

df['euid'] = df['euid'].fillna('Unknown')
df['ad_account'] = df['ad_account'].fillna('Unknown')
df['total_amount'] = df['total_amount'].fillna(0)
df['receiver_id'] = df['receiver_id'].fillna('Unknown')
#filling missing values of currency with inr
df['currency'] = df['currency'].fillna('INR')
df['currency'] = df['currency'].str.upper()
df['gateway_charge'] = df['gateway_charge'].fillna(0)
df['adspend_amount'] = df['adspend_amount'].fillna(0)
df['processing_fee'] = df['processing_fee'].fillna(0)
df['tax'] = df['tax'].fillna(0)
df['convenience_fee'] = df['convenience_fee'].fillna(0)
#cleaning
df['dt'] = pd.to_datetime(df['dt'])
df['total_amount'] = df['total_amount'].astype(float)
df['gateway_charge'] = df['gateway_charge'].astype(float)
df['adspend_amount'] = df['adspend_amount'].astype(float)
df['processing_fee'] = df['processing_fee'].astype(float)
df['tax'] = df['tax'].astype(float)
df['convenience_fee'] = df['convenience_fee'].astype(float)


#fixing currency column


non_usd_currencies = df['currency'].unique()
non_usd_currencies = [currency for currency in non_usd_currencies if currency != 'USD']

    # Create a dictionary to store the conversion rates entered by the user
conversion_rates = {}

# Predefine default values for specific currencies
default_values = {
                    'EUR': 1.08,
                    'GBP': 1.30,
                    'AUD': 0.66,
                    'INR': 0.012,
                    'THB': 0.029,
                    'KRW': 0.00072,
                    'CAD' : 0.72,
                    'BRL' :0.18,
                    'TRY':0.029,
                    'VND':0.000040,
                    'AED':0.27,
                    'RON': 0.22,
                    'ZAR':0.057,
                    'NOK':0.092,
                    'SAR':0.27,
                    'MXN':0.050
                }

def convert_to_usd(row, column_name):
    if row['currency'] == 'USD':
        return row[column_name]
    elif row['currency'] in conversion_rates:
        return row[column_name] * conversion_rates[row['currency']]
    return row[column_name]

# Create the 'spend_in_usd' column
df['total_amount_in_usd'] = df.apply(lambda row: convert_to_usd(row, 'total_amount'), axis=1)
df['gateway_charge_in_usd'] = df.apply(lambda row: convert_to_usd(row, 'gateway_charge'), axis=1)
df['adspend_amount_in_usd'] = df.apply(lambda row: convert_to_usd(row, 'adspend_amount'), axis=1)
df['processing_fee_in_usd'] = df.apply(lambda row: convert_to_usd(row, 'processing_fee'), axis=1)
df['tax_in_usd'] = df.apply(lambda row: convert_to_usd(row, 'tax'), axis=1)
df['convenience_fee_in_usd'] = df.apply(lambda row: convert_to_usd(row, 'convenience_fee'), axis=1)

#title
st.title('AdSpend Paid Stats')

# Display input boxes for each unique currency code other than 'USD'
st.write("Enter conversion rates for the following currencies:")

# Create columns dynamically based on the number of currencies
cols = st.columns(5)  # Adjust the number of columns (3 in this case)

# Iterate over non-USD currencies and display them in columns
for idx, currency in enumerate(non_usd_currencies):
    default_value = default_values.get(currency, 1.0)  # Use default value if defined, otherwise 1.0
    with cols[idx % 5]:  # Rotate through the columns
        conversion_rates[currency] = st.number_input(
            f"{currency} to USD:", value=default_value, min_value=0.0, step=0.001, format="%.3f"
        )

col1,col2,col3 = st.columns(3)

with col1:
    grouping = st.selectbox('Choose Grouping', ['Year', 'Month', 'Week', 'Date'], index=1)

with col2:
    model = st.selectbox('Choose Model', ['All','enterprise', 'zocket.ai'], index=0)

with col3:
    currency= st.selectbox('Choose Currency', ['All','USD', 'INR'], index=0)

# Assuming your 'dt' column is already in date format (e.g., YYYY-MM-DD)
if grouping == 'Year':
    df.loc[:, 'grouped_date'] = df['dt'].apply(lambda x: x.strftime('%Y'))  # Year format as 2024
elif grouping == 'Month':
    df.loc[:, 'grouped_date'] = df['dt'].apply(lambda x: x.strftime('%b-%y'))  # Month format as Jan-24
elif grouping == 'Week':
    df.loc[:, 'grouped_date'] = df['dt'].apply(lambda x: f"{x.strftime('%Y')} - week {x.isocalendar()[1]}")  # Week format as 2024 - week 24
else:
    df.loc[:, 'grouped_date'] = df['dt']  # Just use the date as is (in date format)

if model == 'All':
    df = df
else:
    df = df[df['flag'] == model]

if currency=='USD':
    df = df[df['currency'] != 'INR']
elif currency=='INR':
    df = df[df['currency'] == 'INR']
else:
    df = df
    
grouped_df = df.groupby(['grouped_date']).agg({'total_amount_in_usd': 'sum', 'adspend_amount_in_usd': 'sum', 'convenience_fee_in_usd': 'sum', 'gateway_charge_in_usd': 'sum', 'processing_fee_in_usd': 'sum', 'tax_in_usd': 'sum'}).reset_index()

grouped_df.set_index('grouped_date', inplace=True)
grouped_df_T = grouped_df.T
# Sort the columns by date
if grouping == 'Year':
    grouped_df_T = grouped_df_T[sorted(grouped_df_T.columns, key=lambda x: pd.to_datetime(x, format='%Y'), reverse=True)]
elif grouping == 'Month':
    grouped_df_T = grouped_df_T[sorted(grouped_df_T.columns, key=lambda x: pd.to_datetime(x, format='%b-%y'), reverse=True)]
elif grouping == 'Week':
    grouped_df_T = grouped_df_T[sorted(grouped_df_T.columns, key=lambda x: (int(x.split(' - week ')[0]), int(x.split(' - week ')[1])), reverse=True)]
else:  # Date
    grouped_df_T = grouped_df_T[sorted(grouped_df_T.columns, key=lambda x: pd.to_datetime(x), reverse=True)]

st.dataframe(grouped_df_T,use_container_width=True)

#get usd to inr conversion rate
st.write("Enter conversion rates for the USD TO INR:")

conversion_rate = st.number_input("USD to INR conversion rate:", value=84.0, min_value=0.0, step=0.001, format="%.3f")

grouped_df['total_amount_inr'] = grouped_df['total_amount_in_usd'] * conversion_rate
grouped_df['adspend_amount_inr'] = grouped_df['adspend_amount_in_usd'] * conversion_rate
grouped_df['convenience_fee_inr'] = grouped_df['convenience_fee_in_usd'] * conversion_rate
grouped_df['gateway_charge_inr'] = grouped_df['gateway_charge_in_usd'] * conversion_rate
grouped_df['processing_fee_inr'] = grouped_df['processing_fee_in_usd'] * conversion_rate
grouped_df['tax_inr'] = grouped_df['tax_in_usd'] * conversion_rate  

grouped_df_inr = df.groupby(['grouped_date']).agg({'total_amount_inr': 'sum', 'adspend_amount_inr': 'sum', 'convenience_fee_inr': 'sum', 'gateway_charge_inr': 'sum', 'processing_fee_inr': 'sum', 'tax_inr': 'sum'}).reset_index()

grouped_df_inr.set_index('grouped_date', inplace=True)
grouped_df_inr = grouped_df_inr.T


st.dataframe(grouped_df_inr,use_container_width=True)
