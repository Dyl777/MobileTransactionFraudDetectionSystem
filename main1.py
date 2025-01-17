import pandas as pd
import mysql.connector
from mysql.connector import Error
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import random
from faker import Faker
import hashlib


# Utility function to generate random dates with specified precision
def generate_random_dates(n, precision="millisecond", start_year=2000, end_year=2025):
    """
    Generate random dates with a given precision within a randomly generated start and end date range.

    Args:
    n (int): Number of dates to generate.
    precision (str): Precision of the dates ('millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', 'year').
    start_year (int): Minimum year for the random date range.
    end_year (int): Maximum year for the random date range.

    Returns:
    list: A list of generated dates with the specified precision.
    """
    # Generate a random start date and end date
    random_start = datetime(random.randint(start_year, end_year), random.randint(1, 12), random.randint(1, 28))
    random_end = random_start + timedelta(days=random.randint(1, 365 * (end_year - start_year)))

    # Ensure the end date doesn't exceed the maximum allowed year
    random_end = min(random_end, datetime(end_year, 12, 31))

    # Convert start and end dates to pandas Timestamps
    start = pd.Timestamp(random_start)
    end = pd.Timestamp(random_end)

    if precision == "millisecond":
        return [start + timedelta(milliseconds=random.randint(0, int((end - start).total_seconds() * 1000))) for _ in range(n)]
    elif precision == "second":
        return [start + timedelta(seconds=random.randint(0, int((end - start).total_seconds()))) for _ in range(n)]
    elif precision == "minute":
        return [start + timedelta(minutes=random.randint(0, int((end - start).total_seconds() // 60))) for _ in range(n)]
    elif precision == "hour":
        return [start + timedelta(hours=random.randint(0, int((end - start).total_seconds() // 3600))) for _ in range(n)]
    elif precision == "day":
        return [start + timedelta(days=random.randint(0, (end - start).days)) for _ in range(n)]
    elif precision == "week":
        return [start + timedelta(weeks=random.randint(0, (end - start).days // 7)) for _ in range(n)]
    elif precision == "month":
        return pd.date_range(start=start, end=end, periods=n).to_list()
    elif precision == "year":
        years = [random.randint(start.year, end.year) for _ in range(n)]
        return [datetime(year, random.randint(1, 12), random.randint(1, 28)) for year in years]
    else:
        raise ValueError("Invalid precision. Choose from 'millisecond', 'second', 'minute', 'hour', 'day', 'week', 'month', or 'year'.")


# Initialize Faker
faker = Faker()

# Function to generate random dates
def generate_dates(n):
    return [faker.date_time_between(start_date='-10y', end_date='now') for _ in range(n)]

# Generate Fact Table: Transactions
def generate_transactions(n=1000):
    # Generate unique values for certain fields
    channels = ["mobile_app", "USSD", "agent"]
    agent_ids = [faker.uuid4() for _ in range(100)]
    account_ids = [faker.uuid4() for _ in range(100)]
    
    # Define regions and cities in Cameroon
    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    # Create the transactions dataframe
    transactions = pd.DataFrame({
        "Transaction_ID": range(1, n + 1),
        "Transaction_amount": np.random.uniform(10, 10000, n).round(2),
        "Transaction_type": np.random.choice(["money_transfer", "bill_payment", "airtime_purchase", "other"], n),
        "Channel": np.random.choice(channels, n),
        "Source_account": [faker.uuid4() for _ in range(n)],
        "Destination_account": [faker.uuid4() for _ in range(n)],
        "Subscriber_ID": [faker.uuid4() for _ in range(n)],
        "Transaction_status": np.random.choice(["successful", "failed"], n, p=[0.9, 0.1]),
        "Anomaly_score": np.random.uniform(0, 1, n),
        "Account_ID": np.random.choice(account_ids, n),
        "Agent_ID": np.random.choice(agent_ids, n),
        "Time": generate_dates(n),
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    # Generate Time_Foreign_ID
    transactions["Time_Foreign_ID"] = transactions.apply(
    lambda row: hashlib.sha256(
        f"{row['Transaction_ID']}_{row['Channel']}_{row['Subscriber_ID']}_{row['Account_ID']}_{row['Agent_ID']}_{row['Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
)

    return transactions


# Generate Dimension Table: Accounts
def generate_accounts(n=100):
    # Generate random ages between 10 and 100
    ages = [random.randint(10, 100) for _ in range(n)]

    # Define regions and cities in Cameroon
    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    # Define age groups based on the age
    age_groups = [
        "Adolescent" if age <= 12 else
        "Teenager" if 13 <= age <= 19 else
        "Early-20s" if 20 <= age <= 23 else
        "Mid-20s" if 24 <= age <= 26 else
        "Late-20s" if 27 <= age <= 29 else
        "Early-30s" if 30 <= age <= 33 else
        "Mid-30s Adult" if 34 <= age <= 36 else
        "Late-30s" if 37 <= age <= 39 else
        "40s" if 40 <= age <= 49 else
        "50s" if 50 <= age <= 59 else
        "60s" if 60 <= age <= 69 else
        "70s" if 70 <= age <= 79 else
        "80s" if 80 <= age <= 89 else
        "90s-100s"
        for age in ages
    ]

    # Generate accounts DataFrame
    accounts = pd.DataFrame({
        #"Account_ID": [faker.uuid4() for _ in range(n)],
        "Account_ID": [faker.uuid4() for _ in range(n)],
        "Account_number": [faker.random_number(digits=10, fix_len=True) for _ in range(n)],
        "Account_type": np.random.choice(["individual", "business"], n),
        "Account_status": np.random.choice(["active", "inactive", "blocked"], n),
        "Creation_Time": generate_dates(n),
        #"Creation_Time_Foreign_ID": [faker.uuid4() for _ in range(n)],  # Placeholder for foreign key
        "Account_holder_name": [faker.name() for _ in range(n)],
        "Account_holder_email": [faker.email() for _ in range(n)],
        "Account_holder_address": [faker.address() for _ in range(n)],
        "Site_Name": [faker.domain_name() for _ in range(n)],
        "Associated_Network_name": np.random.choice(["MTN", "ORANGE", "CAMTEL"], n),
        "Associated_NID_NUM": [faker.uuid4() for _ in range(n)],
        "Account_user_gender": np.random.choice(["M", "F"], n),
        "Account_user_age": ages,
        "Account_user_age_group": age_groups,
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    accounts["Creation_Time_Foreign_ID"] = accounts.apply(
    lambda row: hashlib.sha256(
        f"{row['Site_Name']}_{row['Account_ID']}_{row['Account_type']}_{row['Account_number']}_{row['Creation_Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
)

    return accounts



# Generate Dimension Table: Subscribers
def generate_subscribers(n=100):
    # Generate random ages between 10 and 100
    ages = [random.randint(10, 100) for _ in range(n)]

    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    # Define age groups based on the age
    age_groups = [
        "Adolescent" if age <= 12 else
        "Teenager" if 13 <= age <= 19 else
        "Early-20s" if 20 <= age <= 23 else
        "Mid-20s" if 24 <= age <= 26 else
        "Late-20s" if 27 <= age <= 29 else
        "Early-30s" if 30 <= age <= 33 else
        "Mid-30s Adult" if 34 <= age <= 36 else
        "Late-30s" if 37 <= age <= 39 else
        "40s" if 40 <= age <= 49 else
        "50s" if 50 <= age <= 59 else
        "60s" if 60 <= age <= 69 else
        "70s" if 70 <= age <= 79 else
        "80s" if 80 <= age <= 89 else
        "90s-100s"
        for age in ages
        ]

    subscribers = pd.DataFrame({
        "Subscriber_ID": [faker.uuid4() for _ in range(n)],
        "Subscriber_name": [faker.name() for _ in range(n)],
        "Subscriber_phone": [faker.phone_number() for _ in range(n)],
        "Subscriber_email": [faker.email() for _ in range(n)],
        "Subscriber_address": [faker.address() for _ in range(n)],
        "Subscriber_type": np.random.choice(["individual", "business"], n),
        "Subscriber_registration_date": generate_dates(n),
        #"Subscriber_registration_date_ID": ,
        "Subscriber_network_name": np.random.choice(["MTN", "ORANGE", "CAMTEL"], n),
        "Subscriber_network_ID": [faker.uuid4() for _ in range(n)],
        "Subscriber_expiry_date": generate_dates(n),
        #"Subscriber_expiry_date_Foreign_ID": ,
        "Associated_NID_NUM": [faker.uuid4() for _ in range(n)],
        "Subscriber_user_gender": np.random.choice(["M", "F"], n),
        "Subscriber_user_age": ages,
        "Subscriber_user_age_group": age_groups,
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    subscribers["Subscriber_registration_date_ID"] = subscribers.apply(
    lambda row: hashlib.sha256(
        f"{row['Subscriber_ID']}_{row['Subscriber_type']}_{row['Subscriber_name']}_{row['Subscriber_registration_date'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    subscribers["Subscriber_expiry_date_Foreign_ID"] = subscribers.apply(
    lambda row: hashlib.sha256(
        f"{row['Subscriber_ID']}_{row['Subscriber_type']}_{row['Subscriber_name']}_{row['Subscriber_registration_date'].strftime('%Y-%m-%d %H:%M:%S')}_{row['Subscriber_expiry_date'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return subscribers


# Generate Dimension Table: Call Logs
def generate_call_logs(n=500):

     # Define regions and cities in Cameroon
    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    call_start_times = generate_dates(n)#generate_random_dates(n)
    call_end_times = [
        start_time + timedelta(seconds=random.randint(1, 3600)) for start_time in call_start_times
    ]  # Call end times are always after start times

    call_logs = pd.DataFrame({
        "Call_ID": [faker.uuid4() for _ in range(n)],
        "Call_Duration": np.random.randint(1, 3600, n),
        "Receiver_Num": [faker.phone_number() for _ in range(n)],
        "Sender_Num": [faker.phone_number() for _ in range(n)],
        #"Date_Time_Foreign_ID" : ,
        #"Date_Time" : ,  #calculated from call_start & call_end times
        "Call_Start_time": call_start_times, #in years/month/day/hours/minute/second/millisecond
        "Call_End_Time": call_end_times,
        "Duration": [
            (end_time - start_time).total_seconds() for start_time, end_time in zip(call_start_times, call_end_times)
        ],
        "Date_Time": call_start_times,
        "Subscriber_ID": [faker.uuid4() for _ in range(n)],
        "Call_Status": np.random.choice(["Completed", "Failed"], n),
        "Ongoing_Call_Status": np.random.choice([True, False], n),
        "Call_Type": np.random.choice(["Outgoing", "Incoming", "Missed"], n),
        "Platform_Name": np.random.choice(["WhatsApp", "Messenger", "Discord"], n),
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    call_logs["Date_Time_Foreign_ID"] = call_logs.apply(
    lambda row: hashlib.sha256(
        f"{row['Call_ID']}_{row['Subscriber_ID']}_{row['Call_Type']}_{row['Receiver_Num']}_{row['Sender_Num']}_{row['Date_Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return call_logs



# Generate Dimension Table: Messages
def generate_messages(n=500):

    # Define regions and cities in Cameroon
    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    messages = pd.DataFrame({
        "Message_ID": [faker.uuid4() for _ in range(n)],
        "Sender_ID": [faker.uuid4() for _ in range(n)],
        "Receiver_ID": [faker.uuid4() for _ in range(n)],
        "Receiver_Num": [faker.phone_number() for _ in range(n)],
        "Sender_Num": [faker.phone_number() for _ in range(n)],
        "Message_Type": np.random.choice(["SMS", "MMS", "VoIP"], n),
        "Time": generate_dates(n),
        "Sender_Name": [faker.name() for _ in range(n)],
        "Receiver_Name": [faker.name() for _ in range(n)],
        "Message_Kind": np.random.choice(["Text", "Image", "Voice", "Video"], n),
        "Content": [faker.text() for _ in range(n)], #This is the data whether in Image, voice, video compressed as text
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    # Generate Time_Foreign_ID
    messages["Time_Foreign_ID"] = messages.apply(
    lambda row: hashlib.sha256(
        f"{row['Message_ID']}_{row['Sender_ID']}_{row['Receiver_ID']}_{row['Message_Type']}_{row['Message_Kind']}_{row['Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return messages


def generate_cameroon_coordinates(n):
    """
    Generate random latitude and longitude pairs within the bounds of Cameroon.
    Args:
    n (int): Number of coordinates to generate.
    Returns:
    list: List of latitude, longitude strings.
    """
    return [
        f"{random.uniform(2, 13):.6f}, {random.uniform(8, 16):.6f}" for _ in range(n)
    ]

# Generate Dimension Table: ISP Traffic
def generate_isp_traffic(n=500):

    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    isp_traffic =  pd.DataFrame({
        "Traffic_ID": [faker.uuid4() for _ in range(n)],
        "Subscriber_ID": [faker.uuid4() for _ in range(n)],
        "IP_Address": [faker.ipv4() for _ in range(n)],
        "URL_visited": [faker.url() for _ in range(n)],
        "Time": generate_dates(n),
        "Protocol": np.random.choice(["HTTP", "HTTPS", "TOR_NODES"], n),
        "Data_Transferred": np.random.randint(100, 1000000, n),
        "Geo_Location": generate_cameroon_coordinates(n),
        #"Geo_Location": [f"{faker.latitude()}, {faker.longitude()}" for _ in range(n)],
        "Traffic_Status": np.random.choice(["Allowed", "Blocked"], n),
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    # Generate Time_Foreign_ID
    isp_traffic["Time_Foreign_ID"] = isp_traffic.apply(
    lambda row: hashlib.sha256(
        f"{row['Traffic_ID']}_{row['Subscriber_ID']}_{row['IP_Address']}_{row['URL_visited']}_{row['Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return isp_traffic

# Generate Crypto Ledgers, crypto transactions may still be traced by IP.
def generate_crypto_ledgers(n=500):
    crypto_ldgs = pd.DataFrame({
        "Transaction_ID": [faker.uuid4() for _ in range(n)],
        "Wallet_Address": [faker.uuid4() for _ in range(n)],
        "Public_Address_Sender": [faker.uuid4() for _ in range(n)],
        "Sender_IP_Address_ToBlockChain": [faker.ipv4() for _ in range(n)],
        "Timestamp": [faker.date_time_between(start_date='-5y', end_date='now') for _ in range(n)],
        "Transaction_Amount": np.random.uniform(0.001, 50, n).round(8),
        "Currency_Type": np.random.choice(["BTC", "ETH", "LTC", "USDT", "DOGE"], n),
        "Status": np.random.choice(["Completed", "Failed"], n)
    })

    # Generate Time_Foreign_ID
    crypto_ldgs["TimeStamp_Foreign_ID"] = crypto_ldgs.apply(
    lambda row: hashlib.sha256(
        f"{row['Transaction_ID']}_{row['Wallet_Address']}_{row['Sender_IP_Address_ToBlockChain']}_{row['Currency_Type']}_{row['Timestamp'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return crypto_ldgs


# IMEI Generator (Custom)
def generate_imei():
    def luhn_checksum(number):
        def digits_of(n):
            return [int(d) for d in str(n)]
        digits = digits_of(number)
        odd_sum = sum(digits[-1::-2])
        even_sum = sum(sum(digits_of(2 * d)) for d in digits[-2::-2])
        return (odd_sum + even_sum) % 10

    def calculate_luhn(number):
        checksum = luhn_checksum(number * 10)
        return 0 if checksum == 0 else 10 - checksum

    imei_base = random.randint(10**13, 10**14 - 1)  # Generate 14 random digits
    checksum = calculate_luhn(imei_base)
    return f"{imei_base}{checksum}"


#def generate_datautilsrandom_dates(n, start_date="2023-01-01", end_date="2023-12-31"):
    #start = datetime.strptime(start_date, "%Y-%m-%d")
    #end = datetime.strptime(end_date, "%Y-%m-%d")
    #return [start + (end - start) * random.random() for _ in range(n)]


# Generate SIM Info
def generate_sim_info(n=500):
    _start_times = generate_dates(n)
    _end_times = [
    start_time + relativedelta(months=random.randint(1, 12)) for start_time in _start_times
     ]
    
    siminf = pd.DataFrame({
        "SIM_ID": [faker.uuid4() for _ in range(n)],
        "Subscriber_ID": [faker.uuid4() for _ in range(n)],
        "IMEI": [generate_imei() for _ in range(n)],
        "ICCID": [faker.uuid4() for _ in range(n)],
        "Activation_Date": _start_times,
        "Expiry_Date": _end_times,
        "Subscriber_Name": [faker.name() for _ in range(n)]
    })

    # Generate Time_Foreign_ID
    siminf["Activation_Date_Foreign_ID"] = siminf.apply(
    lambda row: hashlib.sha256(
        f"{row['SIM_ID']}_{row['Subscriber_ID']}_{row['IMEI']}_{row['ICCID']}_{row['Activation_Date'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    siminf["Expiry_Date_Foreign_ID"] = siminf.apply(
    lambda row: hashlib.sha256(
        f"{row['SIM_ID']}_{row['Subscriber_ID']}_{row['IMEI']}_{row['ICCID']}_{row['Activation_Date'].strftime('%Y-%m-%d %H:%M:%S')}_{row['Expiry_Date'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return siminf

# Generate Device Info
def generate_device_info(n=500):
    return pd.DataFrame({
        "Device_ID": [faker.uuid4() for _ in range(n)],
        "Subscriber_ID": [faker.uuid4() for _ in range(n)],
        "Device_Type": np.random.choice(["Smartphone", "Tablet", "Laptop"], n),
        "Device_Brand_Name": np.random.choice(["iPhone16", "iPhone15", "iPhone14", "iPhone13", "iPhone12", "iPhone11", "iPhone16 ProMax", "iPhone15 ProMax", "iPhone14 ProMax", "iPhone13 ProMax", "iPhone12 ProMax", "iPhone11 ProMax", "Samsung Galaxy Pro", "Motorola", "Nokia"], n),
        #"OS_Version": np.random.choice(["Android 11", "iOS 15", "iOS 16", "Windows 10", "macOS Ventura"], n),
        "IMEI": [generate_imei() for _ in range(n)],
        "Manufacturer": np.random.choice(["Apple", "Samsung", "Huawei", "Dell", "HP"], n),
        "Model": [faker.bothify(text="Model-##??") for _ in range(n)],
        "App_ID": [faker.uuid4() for _ in range(n)]
    })

# Generate App Info
def generate_app_info(n=500):
    #call_start_times = generate_random_dates(n)
    #call_end_times = [
        #start_time + timedelta(seconds=random.randint(1, 3600)) for start_time in call_start_times
    #]
    _start_times = generate_dates(n) #generate_datautilsrandom_dates(n)
    _end_times = [
        start_time + timedelta(hours=random.randint(1, 24)) for start_time in _start_times
    ] 
    appinfo = pd.DataFrame({
        "App_ID": [faker.uuid4() for _ in range(n)],
        "App_Usage": np.random.randint(1, 100, n), #indicates app usage in relative to other apps as they were ON
        #"Time_period": [faker.date_between(start_date='-10y', end_date='now') for _ in range(n)],
        "Time_period_Start_time": _start_times, #in years/month/day/hours/minute/second/millisecond
        "Time_period_End_Time": _end_times,
        "Duration_Usage_Period_Hours": [
            ((end_time - start_time).total_seconds() / 3600) for start_time, end_time in zip(_start_times, _end_times)
        ],
        "Percentage_Use": np.random.randint(1, 100, n), #indicates battery usage
        "Date_Time": _start_times,
        "Background_Traffic": np.random.randint(100, 5000, n),
        "Internet_Traffic": np.random.randint(1000, 10000, n),
        "Cache_Size": np.random.randint(10, 500, n),
        "App_Data_Size": np.random.randint(100, 2000, n)
    })

    appinfo["Date_Time_Foreign_ID"] = appinfo.apply(
    lambda row: hashlib.sha256(
        f"{row['App_ID']}_{row['App_Usage']}_{row['Percentage_Use']}_{row['Background_Traffic']}_{row['Internet_Traffic']}_{row['App_Data_Size']}_{row['Cache_Size']}_{row['Date_Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return appinfo

# Generate Social Media Logs
def generate_social_media_logs(n=500):
    ages = [random.randint(10, 100) for _ in range(n)]

    # Define age groups based on the age
    age_groups = [
        "Adolescent" if age <= 12 else
        "Teenager" if 13 <= age <= 19 else
        "Early-20s" if 20 <= age <= 23 else
        "Mid-20s" if 24 <= age <= 26 else
        "Late-20s" if 27 <= age <= 29 else
        "Early-30s" if 30 <= age <= 33 else
        "Mid-30s Adult" if 34 <= age <= 36 else
        "Late-30s" if 37 <= age <= 39 else
        "40s" if 40 <= age <= 49 else
        "50s" if 50 <= age <= 59 else
        "60s" if 60 <= age <= 69 else
        "70s" if 70 <= age <= 79 else
        "80s" if 80 <= age <= 89 else
        "90s-100s"
        for age in ages
        ]
    
    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    sc_md = pd.DataFrame({
        "PostType": np.random.choice(["Ad", "Event", "Survey"], n),
        "Post_Content": [faker.text(max_nb_chars=200) for _ in range(n)],
        "Associated_ISP_Network": np.random.choice(["MTN", "ORANGE", "CAMTEL"], n),
        "Associated_SocialMedia_Name": np.random.choice(["Twitter", "Facebook", "LinkedIn"], n),
        "UserRole": np.random.choice(["Commenter", "Poster"], n),
        "Time": generate_dates(n),
        "Email": [faker.email() for _ in range(n)],
        "User_Name": [faker.user_name() for _ in range(n)],
        "User_gender": np.random.choice(["M", "F"], n),
        "User_age": ages,
        "User_age_group": age_groups,
        "Content_Associated_Media": [faker.md5() for _ in range(n)],
        "Comment_Info_On_Post": [faker.text(max_nb_chars=100) for _ in range(n)],
        "User_Comment_On_Post": [faker.text(max_nb_chars=100) for _ in range(n)],
        "Post_Id": [faker.uuid4() for _ in range(n)],
        "AI_Content_Similarity_Score": np.random.uniform(0, 1, n).round(2),
        "Fraud_detection_score": np.random.uniform(0, 1, n).round(2),
        "Associated_Page": [faker.domain_name() for _ in range(n)],
        "Associated_Group": [faker.word() for _ in range(n)],
        "Likes": np.random.randint(0, 1000, n),
        "Comments": np.random.randint(0, 500, n),
        "Hate": np.random.randint(0, 100, n),
        "Loves": np.random.randint(0, 300, n),
        "Shares": np.random.randint(0, 200, n),
        "Views": np.random.randint(100, 10000, n),
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    sc_md["Time_Foreign_ID"] = sc_md.apply(
    lambda row: hashlib.sha256(
        f"{row['Post_Id']}_{row['PostType']}_{row['Post_Content']}_{row['Associated_SocialMedia_Name']}_{row['UserRole']}_{row['Email']}_{row['Time'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return sc_md


# Time Dimension. This is a derived table. Its data will not be generated, but gotten from the other tables
# through SQL queries
def generate_time(n=365):
    dates = [faker.date_between(start_date='-1y', end_date='now') for _ in range(n)]
    local_seasons = ["Dry" if (d.month in [12, 1, 2, 6, 7, 8]) else "Rainy" for d in dates]
    foreign_seasons = ["Summer" if (d.month in [6, 7, 8, 9]) else "Winter" if (d.month in [10, 11, 12]) else "Spring" if (d.month in [1, 2]) else "Autumn" for d in dates]
    days_of_week = [d.strftime("%A") for d in dates]
    return pd.DataFrame({
        "Time_ID": range(1, n + 1), #fact_table_dim_id + Date
        "Date": dates,
        "Month": [d.month for d in dates],
        "Quarter": [(d.month - 1) // 3 + 1 for d in dates],
        "Year": [d.year for d in dates],
        "Local_Season": local_seasons,
        "Foreign_Season": foreign_seasons,
        "Day_of_the_Week": days_of_week
    })

# Generate Agents Dimension
def generate_agents(n=100):
    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }
    ages = [random.randint(10, 100) for _ in range(n)]

    # Define age groups based on the age
    age_groups = [
        "Adolescent" if age <= 12 else
        "Teenager" if 13 <= age <= 19 else
        "Early-20s" if 20 <= age <= 23 else
        "Mid-20s" if 24 <= age <= 26 else
        "Late-20s" if 27 <= age <= 29 else
        "Early-30s" if 30 <= age <= 33 else
        "Mid-30s Adult" if 34 <= age <= 36 else
        "Late-30s" if 37 <= age <= 39 else
        "40s" if 40 <= age <= 49 else
        "50s" if 50 <= age <= 59 else
        "60s" if 60 <= age <= 69 else
        "70s" if 70 <= age <= 79 else
        "80s" if 80 <= age <= 89 else
        "90s-100s"
        for age in ages
        ]

    gen_agts = pd.DataFrame({
        "Agent_ID": [faker.uuid4() for _ in range(n)],
        "Name": [faker.name() for _ in range(n)],
        "Gender": np.random.choice(["M", "F"], n),
        "Age": ages,
        "Age_group": age_groups,
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)],
        "Number": [faker.phone_number() for _ in range(n)],
        "Creation_Date": generate_dates(n)#[faker.date_between(start_date='-5y', end_date='now') for _ in range(n)]
    })

    gen_agts["Creation_Time_Foreign_ID"] = gen_agts.apply(
    lambda row: hashlib.sha256(
        f"{row['Agent_ID']}_{row['Number']}_{row['Region']}_{row['Creation_Date'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
)

    return gen_agts

# Generate Audit Logs Dimension
def generate_audit_logs(n=500):
    adt_lgs = pd.DataFrame({
        "Audit_ID": [faker.uuid4() for _ in range(n)],
        "Account_ID": [faker.uuid4() for _ in range(n)],
        "Action": np.random.choice(["Update", "Delete", "Insert", "View"], n),
        "Action_Date": [faker.date_time_between(start_date='-1y', end_date='now') for _ in range(n)]
    })
    
    adt_lgs["Creation_Time_Foreign_ID"] = adt_lgs.apply(
    lambda row: hashlib.sha256(
        f"{row['Audit_ID']}_{row['Account_ID']}_{row['Action']}_{row['Action_Date'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return adt_lgs

# Regional Stats Dimension, This data already exists, this is just the script to generate it
#def generate_regional_stats():
    #regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    #return pd.DataFrame({
        #"Region": regions,
        #"Population": np.random.randint(500000, 3000000, len(regions)),
        #"GDP_Contribution": np.random.uniform(1, 15, len(regions)).round(2),
        #"Literacy_Rate": np.random.uniform(50, 90, len(regions)).round(2)
    #})

# Generate Support Logs Dimension
def generate_support_logs(n=500):

    regions = ["Central", "Littoral", "Northwest", "Southwest", "Far North", "North", "Adamaoua", "West", "East", "South"]
    cities = {
        "Central": ["Yaounde"],
        "Littoral": ["Douala"],
        "Northwest": ["Bamenda"],
        "Southwest": ["Buea", "Limbe"],
        "Far North": ["Maroua"],
        "North": ["Garoua"],
        "Adamaoua": ["Ngaoundere"],
        "West": ["Bafoussam"],
        "East": ["Bertoua"],
        "South": ["Ebolowa"]
    }

    _start_times = generate_dates(n) #generate_random_dates(n)
    _end_times = [
        start_time + timedelta(hours=random.randint(1, 24)) for start_time in _start_times
    ] 
    gen_spp_lgs = pd.DataFrame({
        "Log_ID": [faker.uuid4() for _ in range(n)],
        "Account_ID": [faker.uuid4() for _ in range(n)],
        "Agent_ID": [faker.uuid4() for _ in range(n)],
        "Institute_ID": [faker.uuid4() for _ in range(n)],
        "Institute_Name": np.random.choice(["MTN", "ORANGE", "Camtel", "UBA", "Ecobank"], n),
        "Issue_Type": np.random.choice(["Network Issue", "Billing Issue", "Service Request"], n),
        "Description": [faker.sentence(nb_words=10) for _ in range(n)],
        "Date_Issued": _start_times,
        "Date_Resolved": _end_times,
        "Duration_to_Resolve": [((end_time - start_time).total_seconds() / 3600) for start_time, end_time in zip(_start_times, _end_times)],
        "Region": np.random.choice(regions, n),  # Select random region
        "City": [faker.random.choice(cities[region]) for region in np.random.choice(regions, n)]
    })

    gen_spp_lgs["Foreign_Issued_Date_ID"] = gen_spp_lgs.apply(
    lambda row: hashlib.sha256(
        f"{row['Agent_ID']}_{row['Account_ID']}_{row['Institute_Name']}_{row['Issue_Type']}_{row['Date_Issued'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    gen_spp_lgs["Foreign_Resolved_Date_ID"] = gen_spp_lgs.apply(
    lambda row: hashlib.sha256(
        f"{row['Agent_ID']}_{row['Account_ID']}_{row['Institute_Name']}_{row['Issue_Type']}_{row['Date_Issued'].strftime('%Y-%m-%d %H:%M:%S')}_{row['Date_Resolved'].strftime('%Y-%m-%d %H:%M:%S')}"
        .encode('utf-8')
    ).hexdigest(),
    axis=1
    )

    return gen_spp_lgs


# Generate all tables
tables = {
    "Transactions": generate_transactions(),
    "Accounts": generate_accounts(),
    "Subscribers": generate_subscribers(),
    #"Time": generate_time(),
    #"Channels": generate_channels(),
    "CallLogs": generate_call_logs(),
    "Messages": generate_messages(),
    "ISPTraffic": generate_isp_traffic(),
    "CryptoLedgers": generate_crypto_ledgers(),
    "SIMInfo": generate_sim_info(),
    "DeviceInfo": generate_device_info(),
    "AppInfo": generate_app_info(),
    "SocialMediaLogs": generate_social_media_logs(),
    "SupportLogs": generate_support_logs(),
    "AuditLogs": generate_audit_logs(),
    "Agents": generate_agents()
}

# Save each table to CSV
for table_name, df in tables.items():
    file_name = f"{table_name.lower().replace(' ', '_')}.csv"
    df.to_csv(file_name, index=False)
    print(f"Saved {table_name} as {file_name}")


def load_datetime_columns():
    """Load all tables and extract datetime columns with their foreign keys"""
    datetime_data = []
    
    # Load each table and extract datetime columns with their foreign keys
    tables_config = {
        'transactions.csv': {
            'datetime_col': 'Time',
            'foreign_key_col': 'Time_Foreign_ID'
        },
        'accounts.csv': {
            'datetime_col': 'Creation_Time',
            'foreign_key_col': 'Creation_Time_Foreign_ID'
        },
        'subscribers.csv': {
            'datetime_col': 'Subscriber_registration_date',
            'foreign_key_col': 'Subscriber_expiry_date_Foreign_ID'
        },
        'calllogs.csv': {
            'datetime_col': 'Date_Time',
            'foreign_key_col': 'Date_Time_Foreign_ID'
        },
        'messages.csv': {
            'datetime_col': 'Time',
            'foreign_key_col': 'Time_Foreign_ID'
        },
        'isptraffic.csv': {
            'datetime_col': 'Time',
            'foreign_key_col': 'Time_Foreign_ID'
        },
        'cryptoledgers.csv': {
            'datetime_col': 'Timestamp',
            'foreign_key_col': 'TimeStamp_Foreign_ID'
        },
        'siminfo.csv': {
            'datetime_col': 'Activation_Date',
            'foreign_key_col': 'Expiry_Date_Foreign_ID'
        },
        'appinfo.csv': {
            'datetime_col':  'Date_Time',
            'foreign_key_col': 'Date_Time_Foreign_ID'
        },
        'socialmedialogs.csv': {
            'datetime_col': 'Time',
            'foreign_key_col': 'Time_Foreign_ID'
        },
        'agents.csv': {
            'datetime_col': 'Creation_Date',
            'foreign_key_col': 'Creation_Time_Foreign_ID'
        },
        'auditlogs.csv': {
            'datetime_col': 'Action_Date',
            'foreign_key_col': 'Creation_Time_Foreign_ID'
        },
        'supportlogs.csv': {
            'datetime_col': 'Date_Issued',
            'foreign_key_col': 'Foreign_Resolved_Date_ID'
        }
    }
    
    for filename, config in tables_config.items():
        try:
            df = pd.read_csv(filename)
            
            # Handle single datetime column
            if 'datetime_col' in config:
                datetime_col = config['datetime_col']
                foreign_key_col = config['foreign_key_col']
                
                temp_data = pd.DataFrame({
                    'datetime': pd.to_datetime(df[datetime_col]),
                    'foreign_key': df[foreign_key_col]
                })
                datetime_data.append(temp_data)
            
            # Handle multiple datetime columns
            elif 'datetime_cols' in config:
                for dt_col, fk_col in zip(config['datetime_cols'], config['foreign_key_cols']):
                    temp_data = pd.DataFrame({
                        'datetime': pd.to_datetime(df[dt_col]),
                        'foreign_key': df[fk_col]
                    })
                    datetime_data.append(temp_data)
            
        except FileNotFoundError:
            print(f"Warning: {filename} not found")
            continue
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue
    
    return pd.concat(datetime_data, ignore_index=True)

def create_time_dimension(datetime_data):
    """Create time dimension table from consolidated datetime data"""
    # Get unique dates and their foreign keys
    datetime_data = datetime_data.drop_duplicates()
    
    # Sort by datetime
    datetime_data = datetime_data.sort_values('datetime')
    
    # Create time dimension dataframe
    time_dim = pd.DataFrame({
        'Time_ID': datetime_data['foreign_key'],
        'Date': datetime_data['datetime'].dt.date,
        'DateTime': datetime_data['datetime'],
        'Year': datetime_data['datetime'].dt.year,
        'Month': datetime_data['datetime'].dt.month,
        'Day': datetime_data['datetime'].dt.day,
        'Hour': datetime_data['datetime'].dt.hour,
        'Minute': datetime_data['datetime'].dt.minute,
        'Second': datetime_data['datetime'].dt.second,
        'Quarter': datetime_data['datetime'].dt.quarter,
        'WeekDay': datetime_data['datetime'].dt.day_name(),
        'WeekDayNum': datetime_data['datetime'].dt.dayofweek,
        'WeekOfYear': datetime_data['datetime'].dt.isocalendar().week,
        'DayOfYear': datetime_data['datetime'].dt.dayofyear,
        'IsWeekend': datetime_data['datetime'].dt.dayofweek.isin([5, 6])
    })
    
    # Add seasons
    time_dim['Local_Season'] = time_dim.apply(
        lambda x: "Dry" if x['Month'] in [12, 1, 2, 6, 7, 8] else "Rainy", 
        axis=1
    )
    
    time_dim['Foreign_Season'] = time_dim.apply(
        lambda x: "Summer" if x['Month'] in [6, 7, 8, 9]
        else "Winter" if x['Month'] in [10, 11, 12]
        else "Spring" if x['Month'] in [1, 2]
        else "Autumn",
        axis=1
    )
    
    # Add time-of-day classification
    time_dim['TimeOfDay'] = time_dim.apply(
        lambda x: "Night" if 0 <= x['Hour'] < 6
        else "Morning" if 6 <= x['Hour'] < 12
        else "Afternoon" if 12 <= x['Hour'] < 17
        else "Evening" if 17 <= x['Hour'] < 20
        else "Night",
        axis=1
    )
    
    return time_dim

def generate_sample_time_datasets():
    print("Loading datetime data from all tables...")
    datetime_data = load_datetime_columns()
    print(f"Found {len(datetime_data)} datetime records")
    
    print("\nCreating time dimension table...")
    time_dimension = create_time_dimension(datetime_data)
    print(f"Created time dimension with {len(time_dimension)} unique entries")
    
    print("\nSaving time dimension table...")
    time_dimension.to_csv('time_dimension.csv', index=False)
    
    # Print sample statistics
    print("\nTime dimension statistics:")
    print(f"Date range: {time_dimension['Date'].min()} to {time_dimension['Date'].max()}")
    print(f"Number of years: {time_dimension['Year'].nunique()}")
    print(f"Records per season:")
    print(time_dimension['Local_Season'].value_counts())
    
    return time_dimension


#generate_sample_time_datasets() #generate time dimensions from other tables


# Load datasets from CSV files
datasets = {
    "Transactions": pd.read_csv("Transactions.csv"),
    "Accounts": pd.read_csv("Accounts.csv"),
    "Subscribers": pd.read_csv("Subscribers.csv"),
    "CallLogs": pd.read_csv("CallLogs.csv"),
    "ISPTraffic": pd.read_csv("ISPTraffic.csv"),
    "SIMInfo": pd.read_csv("SIMInfo.csv"),
    "Agents": pd.read_csv("Agents.csv"),
    "SupportLogs": pd.read_csv("SupportLogs.csv"),
    "AuditLogs": pd.read_csv("AuditLogs.csv"),
    "SocialMediaLogs": pd.read_csv("SocialMediaLogs.csv"),
    "CryptoLedgers": pd.read_csv("CryptoLedgers.csv"),
    "Time_Dimension": pd.read_csv("Time_Dimension.csv"),
    "AppInfo": pd.read_csv("AppInfo.csv"),
    "Messages": pd.read_csv("Messages.csv")
    # Add additional datasets here...
}

# Define relationships between tables
relationships = [
    {"primary_table": "Accounts", "foreign_table": "Transactions", "primary_key": "Account_ID", "foreign_key": "Account_ID"},
    {"primary_table": "Agents", "foreign_table": "Transactions", "primary_key": "Agent_ID", "foreign_key": "Agent_ID"},
    {"primary_table": "Subscribers", "foreign_table": "Transactions", "primary_key": "Subscriber_ID", "foreign_key": "Subscriber_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "Transactions", "primary_key": "Time_ID", "foreign_key": "Time_Foreign_ID"},
    {"primary_table": "Subscribers", "foreign_table": "CallLogs", "primary_key": "Subscriber_ID", "foreign_key": "Subscriber_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "CallLogs", "primary_key": "Time_ID", "foreign_key": "Date_Time_Foreign_ID"},
    {"primary_table": "Subscribers", "foreign_table": "ISPTraffic", "primary_key": "Subscriber_ID", "foreign_key": "Subscriber_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "ISPTraffic", "primary_key": "Time_ID", "foreign_key": "Time_Foreign_ID"},
    {"primary_table": "Subscribers", "foreign_table": "SIMInfo", "primary_key": "Subscriber_ID", "foreign_key": "Subscriber_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "SIMInfo", "primary_key": "Time_ID", "foreign_key": "Expiry_Date_Foreign_ID"},
    {"primary_table": "Accounts", "foreign_table": "SupportLogs", "primary_key": "Account_ID", "foreign_key": "Account_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "SupportLogs", "primary_key": "Time_ID", "foreign_key": "Foreign_Resolved_Date_ID"},
    {"primary_table": "Agents", "foreign_table": "SupportLogs", "primary_key": "Agent_ID", "foreign_key": "Agent_ID"},
    {"primary_table": "Accounts", "foreign_table": "AuditLogs", "primary_key": "Account_ID", "foreign_key": "Account_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "AuditLogs", "primary_key": "Time_ID", "foreign_key": "Creation_Time_Foreign_ID"},
    {"primary_table": "Accounts", "foreign_table": "SocialMediaLogs", "primary_key": "Account_holder_email", "foreign_key": "Email"},
    {"primary_table": "Subscribers", "foreign_table": "SocialMediaLogs", "primary_key": "Subscriber_email", "foreign_key": "Email"},
    {"primary_table": "Time_Dimension", "foreign_table": "SocialMediaLogs", "primary_key": "Time_ID", "foreign_key": "Time_Foreign_ID"},
    {"primary_table": "Transactions", "foreign_table": "CryptoLedgers", "primary_key": "Transaction_ID", "foreign_key": "Transaction_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "CryptoLedgers", "primary_key": "Time_ID", "foreign_key": "TimeStamp_Foreign_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "Accounts", "primary_key": "Time_ID", "foreign_key": "Creation_Time_Foreign_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "Subscribers", "primary_key": "Time_ID", "foreign_key": "Subscriber_expiry_date_Foreign_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "Agents", "primary_key": "Time_ID", "foreign_key": "Creation_Time_Foreign_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "AppInfo", "primary_key": "Time_ID", "foreign_key": "Date_Time_Foreign_ID"},
    {"primary_table": "Time_Dimension", "foreign_table": "Messages", "primary_key": "Time_ID", "foreign_key": "Time_Foreign_ID"},
     # Add more relationships as needed...
]

primary_keys = {
    "Transactions": "Transaction_ID",
    "Accounts": "Account_ID",
    "Subscribers": "Subscriber_ID",
    "Agents": "Agent_ID",
    "AuditLogs": "Audit_ID",
    "CallLogs": "Call_ID",
    "SupportLogs": "Log_ID",
    "AppInfo": "App_ID",
    "Messages": "Message_ID",
    "SimInfo": "SIM_ID",
    #"Time_Dimension": "Time_ID",
    "ISPTraffic": "Traffic_ID",
    "DeviceInfo": "Device_ID",
    "SocialMediaLogs": "Post_Id",
    "CryptoLedgers": "Public_Address_Sender"
}

def add_dummy_primary_keys(datasets, primary_keys):
    """
    Adds a dummy primary key column to all DataFrames in the datasets dictionary.
    Additionally, creates a dummy entry for the primary key with equivalent dummy values for all fields.

    Args:
        datasets (dict): Dictionary of tables (DataFrames).
    """
    for table_name, df in datasets.items():
        primary_key = primary_keys.get(table_name)
        if not primary_key:
            print(f"Warning: No primary key defined for table '{table_name}'. Skipping.")
            continue

        # Create a dummy entry for the primary key
        dummy_entry = {}
        for col in df.columns:
            if col == primary_key:
                dummy_entry[col] = f"dummy_{table_name}_ID"
            elif df[col].dtype == 'object':
                dummy_entry[col] = f"dummy_{col}"
            elif df[col].dtype in ['int64', 'float64']:
                dummy_entry[col] = 0
            elif np.issubdtype(df[col].dtype, np.datetime64):
                dummy_entry[col] = pd.Timestamp("1970-01-01 00:00:00")
            else:
                dummy_entry[col] = None  # Default for unrecognized types

        # Append the dummy entry to the DataFrame
        df = pd.concat([df, pd.DataFrame([dummy_entry])], ignore_index=True)
        print(f"Added dummy entry with primary key 'dummy_{table_name}_ID' to {table_name}")

        # Update the dataset with the modified DataFrame
        datasets[table_name] = df

    return datasets
 

def enforce_foreign_key_integrity(datasets, relationships):
    for rel in relationships:
        primary_table = rel["primary_table"]
        foreign_table = rel["foreign_table"]
        primary_key = rel["primary_key"]
        foreign_key = rel["foreign_key"]

        if primary_table not in datasets:
            print(f"Warning: Primary table '{primary_table}' not loaded. Skipping relationship.")
            continue
        if foreign_table not in datasets:
            print(f"Warning: Foreign table '{foreign_table}' not loaded. Skipping relationship.")
            continue

        primary_df = datasets[primary_table]
        foreign_df = datasets[foreign_table]

        # Find missing foreign key entries
        missing_foreign_keys = foreign_df[~foreign_df[foreign_key].isin(primary_df[primary_key])][foreign_key].unique()
        
        if missing_foreign_keys.size > 0:
            print(f"Adding {len(missing_foreign_keys)} dummy entries to {primary_table} to enforce referential integrity for {foreign_key} in {foreign_table}.")
            
            # Generate dummy entries
            dummy_rows = pd.DataFrame({primary_key: missing_foreign_keys})
            for col in primary_df.columns:
                if col != primary_key:
                    if primary_df[col].dtype == 'object':
                        dummy_rows[col] = f"dummy_{col}"
                    elif primary_df[col].dtype in ['int64', 'float64']:
                        dummy_rows[col] = 0
                    elif np.issubdtype(primary_df[col].dtype, np.datetime64):
                        dummy_rows[col] = pd.Timestamp("1970-01-01 00:00:00")

            # Add dummy rows to the primary table
            datasets[primary_table] = pd.concat([primary_df, dummy_rows], ignore_index=True)

            # Update the foreign table to match dummy rows if applicable
            # Update the foreign table to match dummy rows if applicable
            invalid_mask = ~foreign_df[foreign_key].isin(primary_df[primary_key])
            invalid_count = invalid_mask.sum()

            # Generate enough dummy values to match the invalid entries
            dummy_values = list(dummy_rows[primary_key])
            while len(dummy_values) < invalid_count:
                dummy_values.extend(dummy_rows[primary_key])  # Repeat dummy values if necessary
            dummy_values = dummy_values[:invalid_count]

            # Assign dummy values to invalid rows
            foreign_df.loc[invalid_mask, foreign_key] = dummy_values

#dummy_values
add_dummy_primary_keys(datasets, primary_keys)

generate_sample_time_datasets()

# Enforce referential integrity
enforce_foreign_key_integrity(datasets, relationships)

# Save updated datasets
for table_name, df in datasets.items():
    df.to_csv(f"{table_name}.csv", index=False)


# Check foreign key consistency
def check_foreign_keys(primary_df, foreign_df, primary_key, foreign_key):
    """
    Checks if all entries in a foreign key column exist in the primary key column of another DataFrame.

    Args:
    primary_df (pd.DataFrame): DataFrame containing the primary key column.
    foreign_df (pd.DataFrame): DataFrame containing the foreign key column.
    primary_key (str): Column name of the primary key.
    foreign_key (str): Column name of the foreign key.

    Returns:
    pd.DataFrame: Rows from foreign_df where the foreign key doesn't exist in primary_df.
    """
    invalid_entries = foreign_df[~foreign_df[foreign_key].isin(primary_df[primary_key])]
    return invalid_entries

# Run checks for all relationships
for rel in relationships:
    primary_table = rel["primary_table"]
    foreign_table = rel["foreign_table"]
    primary_key = rel["primary_key"]
    foreign_key = rel["foreign_key"]

    primary_df = datasets[primary_table]
    foreign_df = datasets[foreign_table]

    # Check for invalid foreign key entries
    invalid_entries = check_foreign_keys(primary_df, foreign_df, primary_key, foreign_key)
    
    if not invalid_entries.empty:
        print(f"Invalid {foreign_key} entries in {foreign_table} (referencing {primary_table}.{primary_key}):")
        print(invalid_entries)
        print("\n")
    else:
        print(f"All {foreign_key} entries in {foreign_table} are valid.\n")


def create_database_and_tables():
    try:
        # Connect to MySQL server
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
        )
        cursor = connection.cursor()

        # Create a new database
        cursor.execute("CREATE DATABASE IF NOT EXISTS fraud_detection")
        cursor.execute("USE fraud_detection")

        # Define tables with primary and foreign keys
        table_definitions = {
            "Time_Dimensions": """
                CREATE TABLE IF NOT EXISTS Time_Dimension (
                    Time_ID VARCHAR(255) PRIMARY KEY,
                    Date DATE NOT NULL,
                    DateTime DATETIME NOT NULL,
                    Year INT NOT NULL,
                    Month INT NOT NULL,
                    Day INT NOT NULL,
                    Hour INT,
                    Minute INT,
                    Second INT,
                    Quarter INT NOT NULL,
                    WeekDay VARCHAR(20) NOT NULL,
                    WeekDayNum INT NOT NULL,
                    WeekOfYear INT NOT NULL,
                    DayOfYear INT NOT NULL,
                    IsWeekend BOOLEAN NOT NULL,
                    Local_Season VARCHAR(10) NOT NULL,
                    Foreign_Season VARCHAR(10) NOT NULL,
                    TimeOfDay VARCHAR(10) NOT NULL
                 )
             """,
            "Accounts": """
                CREATE TABLE IF NOT EXISTS Accounts (
                    Account_ID VARCHAR(36) PRIMARY KEY,
                    Account_number BIGINT NOT NULL,
                    Account_type VARCHAR(20),
                    Account_status VARCHAR(20),
                    Creation_Time DATETIME,
                    Account_holder_name VARCHAR(255),
                    Account_holder_email VARCHAR(255),
                    Account_holder_address TEXT,
                    Site_Name VARCHAR(255),
                    Associated_Network_name VARCHAR(50),
                    Associated_NID_NUM VARCHAR(36),
                    Account_user_gender VARCHAR(1),
                    Account_user_age INT,
                    Account_user_age_group VARCHAR(50),
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Creation_Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Creation_Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            "Subscribers": """
                CREATE TABLE IF NOT EXISTS Subscribers (
                    Subscriber_ID VARCHAR(36) PRIMARY KEY,
                    Subscriber_name VARCHAR(255),
                    Subscriber_phone VARCHAR(50),
                    Subscriber_email VARCHAR(255),
                    Subscriber_address TEXT,
                    Subscriber_type VARCHAR(20),
                    Subscriber_registration_date DATETIME,
                    Subscriber_expiry_date DATETIME,
                    Subscriber_network_name VARCHAR(50),
                    Subscriber_network_ID VARCHAR(36),
                    Associated_NID_NUM VARCHAR(36),
                    Subscriber_user_gender VARCHAR(1),
                    Subscriber_user_age INT,
                    Subscriber_user_age_group VARCHAR(50),
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Subscriber_registration_date_ID VARCHAR(255),
                    Subscriber_expiry_date_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Subscriber_expiry_date_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            "Transactions": """
                CREATE TABLE IF NOT EXISTS Transactions (
                    Transaction_ID VARCHAR(36) PRIMARY KEY,
                    Transaction_amount FLOAT,
                    Transaction_type VARCHAR(50),
                    Channel VARCHAR(50),
                    Source_account VARCHAR(36),
                    Destination_account VARCHAR(36),
                    Subscriber_ID VARCHAR(36),
                    Transaction_status VARCHAR(20),
                    Anomaly_score FLOAT,
                    Account_ID VARCHAR(36),
                    Agent_ID VARCHAR(36),
                    Time DATETIME,
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID),
                    FOREIGN KEY (Account_ID) REFERENCES Accounts(Account_ID),
                    FOREIGN KEY (Subscriber_ID) REFERENCES Subscribers(Subscriber_ID)
                )
            """,
            "CallLogs": """
                CREATE TABLE IF NOT EXISTS CallLogs (
                    Call_ID VARCHAR(36) PRIMARY KEY,
                    Call_Duration INT,
                    Receiver_Num VARCHAR(50),
                    Sender_Num VARCHAR(50),
                    Call_Start_time DATETIME,
                    Call_End_Time DATETIME,
                    Duration FLOAT,
                    Date_Time DATETIME,
                    Subscriber_ID VARCHAR(36),
                    Call_Status VARCHAR(20),
                    Ongoing_Call_Status BOOLEAN,
                    Call_Type VARCHAR(20),
                    Platform_Name VARCHAR(50),
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Date_Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Date_Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID),
                    FOREIGN KEY (Subscriber_ID) REFERENCES Subscribers(Subscriber_ID)
                )
            """,
            "Messages": """
                CREATE TABLE IF NOT EXISTS Messages (
                    Message_ID VARCHAR(36) PRIMARY KEY,
                    Sender_ID VARCHAR(36),
                    Receiver_ID VARCHAR(36),
                    Receiver_Num VARCHAR(50),
                    Sender_Num VARCHAR(50),
                    Message_Type VARCHAR(20),
                    Time DATETIME,
                    Sender_Name VARCHAR(255),
                    Receiver_Name VARCHAR(255),
                    Message_Kind VARCHAR(50),
                    Content TEXT,
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID) ON DELETE CASCADE ON UPDATE CASCADE
                )
            """,
            "ISPTraffic": """
                CREATE TABLE IF NOT EXISTS ISPTraffic (
                    Traffic_ID VARCHAR(36) PRIMARY KEY,
                    Subscriber_ID VARCHAR(36),
                    IP_Address VARCHAR(50),
                    URL_visited TEXT,
                    Time DATETIME,
                    Protocol VARCHAR(20),
                    Data_Transferred INT,
                    Geo_Location VARCHAR(100),
                    Traffic_Status VARCHAR(20),
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID),
                    FOREIGN KEY (Subscriber_ID) REFERENCES Subscribers(Subscriber_ID)
                )
            """,
            "CryptoLedgers": """
                CREATE TABLE IF NOT EXISTS CryptoLedgers (
                    Transaction_ID VARCHAR(36) PRIMARY KEY,
                    Wallet_Address VARCHAR(36),
                    Public_Address_Sender VARCHAR(36),
                    Sender_IP_Address_ToBlockChain VARCHAR(50),
                    Timestamp DATETIME,
                    Transaction_Amount FLOAT,
                    Currency_Type VARCHAR(10),
                    Status VARCHAR(20),
                    TimeStamp_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (TimeStamp_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            "SIMInfo": """
                CREATE TABLE IF NOT EXISTS SIMInfo (
                    SIM_ID VARCHAR(36) PRIMARY KEY,
                    Subscriber_ID VARCHAR(36),
                    IMEI VARCHAR(50),
                    ICCID VARCHAR(50),
                    Activation_Date DATETIME,
                    Expiry_Date DATETIME,
                    Subscriber_Name VARCHAR(255),
                    Activation_Date_Foreign_ID VARCHAR(255),
                    Expiry_Date_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Subscriber_ID) REFERENCES Subscribers(Subscriber_ID),
                    FOREIGN KEY (Expiry_Date_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            "DeviceInfo": """
                CREATE TABLE IF NOT EXISTS DeviceInfo (
                    Device_ID VARCHAR(36) PRIMARY KEY,
                    Subscriber_ID VARCHAR(36),
                    Device_Type VARCHAR(50),
                    Device_Brand_Name VARCHAR(100),
                    IMEI VARCHAR(50),
                    Manufacturer VARCHAR(50),
                    Model VARCHAR(50),
                    App_ID VARCHAR(36)
                )
            """,
            "AppInfo": """
                CREATE TABLE IF NOT EXISTS AppInfo (
                    App_ID VARCHAR(36) PRIMARY KEY,
                    App_Usage INT,
                    Time_period_Start_time DATETIME,
                    Time_period_End_Time DATETIME,
                    Duration_Usage_Period_Hours FLOAT,
                    Percentage_Use INT,
                    Date_Time DATETIME,
                    Background_Traffic INT,
                    Internet_Traffic INT,
                    Cache_Size INT,
                    App_Data_Size INT,
                    Date_Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Date_Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            "SocialMediaLogs": """
                CREATE TABLE IF NOT EXISTS SocialMediaLogs (
                    Post_Id VARCHAR(36) PRIMARY KEY,
                    PostType VARCHAR(20),
                    Post_Content TEXT,
                    Associated_ISP_Network VARCHAR(50),
                    Associated_SocialMedia_Name VARCHAR(50),
                    UserRole VARCHAR(20),
                    Time DATETIME,
                    Email VARCHAR(255),
                    User_Name VARCHAR(255),
                    User_gender VARCHAR(1),
                    User_age INT,
                    User_age_group VARCHAR(50),
                    Content_Associated_Media VARCHAR(50),
                    Comment_Info_On_Post TEXT,
                    User_Comment_On_Post TEXT,
                    AI_Content_Similarity_Score FLOAT,
                    Fraud_detection_score FLOAT,
                    Associated_Page VARCHAR(255),
                    Associated_Group VARCHAR(50),
                    Likes INT,
                    Comments INT,
                    Hate INT,
                    Loves INT,
                    Shares INT,
                    Views INT,
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            
            "Agents": """
                CREATE TABLE IF NOT EXISTS Agents (
                    Agent_ID VARCHAR(36) PRIMARY KEY,
                    Name VARCHAR(255),
                    Gender VARCHAR(1),
                    Age INT,
                    Age_group VARCHAR(50),
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Number VARCHAR(50),
                    Creation_Date DATETIME,
                    Creation_Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Creation_Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """,
            "SupportLogs": """
                CREATE TABLE IF NOT EXISTS SupportLogs (
                    Log_ID VARCHAR(36) PRIMARY KEY,
                    Account_ID VARCHAR(36),
                    Agent_ID VARCHAR(36),
                    Institute_ID VARCHAR(36),
                    Institute_Name VARCHAR(50),
                    Issue_Type VARCHAR(50),
                    Description TEXT,
                    Date_Issued DATETIME,
                    Date_Resolved DATETIME,
                    Duration_to_Resolve FLOAT,
                    Region VARCHAR(50),
                    City VARCHAR(50),
                    Foreign_Issued_Date_ID VARCHAR(255),
                    Foreign_Resolved_Date_ID VARCHAR(255),
                    FOREIGN KEY (Account_ID) REFERENCES Accounts(Account_ID),
                    FOREIGN KEY (Agent_ID) REFERENCES Agents(Agent_ID),
                    FOREIGN KEY (Foreign_Resolved_Date_ID) REFERENCES Time_Dimension(Time_ID) 
                )
            """,
            "AuditLogs": """
                CREATE TABLE IF NOT EXISTS AuditLogs (
                    Audit_ID VARCHAR(36) PRIMARY KEY,
                    Account_ID VARCHAR(36),
                    Action VARCHAR(20),
                    Action_Date DATETIME,
                    Creation_Time_Foreign_ID VARCHAR(255),
                    FOREIGN KEY (Account_ID) REFERENCES Accounts(Account_ID),
                    FOREIGN KEY (Creation_Time_Foreign_ID) REFERENCES Time_Dimension(Time_ID)
                )
            """
            # Add more table definitions as needed
        }

        # Create tables
        for table_name, create_query in table_definitions.items():
            cursor.execute(create_query)
            print(f"Table {table_name} created.")

        connection.commit()

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Call the function to create the database and tables
create_database_and_tables()

def insert_data_to_mysql(table_name, dataframe):
    """
    Inserts data from a DataFrame into a specified MySQL table using batch processing.
    Args:
        table_name (str): Name of the MySQL table.
        dataframe (pd.DataFrame): DataFrame containing the data to insert.
    """
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='fraud_detection'
        )
        cursor = connection.cursor()
        
        # Try to increase max_allowed_packet size
        try:
            cursor.execute("SET GLOBAL max_allowed_packet=67108864")  # 64MB
        except Error as config_error:
            print(f"Warning: Could not set max_allowed_packet: {config_error}")
        
        # Generate placeholders for data insertion
        placeholders = ", ".join(["%s"] * len(dataframe.columns))
        columns = ", ".join(dataframe.columns)
        
        # Prepare the INSERT INTO statement
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert the DataFrame to a list of tuples
        data = dataframe.where(pd.notnull(dataframe), None).values.tolist()
        
        # Insert data in smaller batches
        batch_size = 500  # Reduced batch size
        total_batches = (len(data) + batch_size - 1) // batch_size
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            try:
                cursor.executemany(sql, batch)
                connection.commit()
                print(f"Inserted batch {(i//batch_size) + 1}/{total_batches} into {table_name} ({len(batch)} rows)")
            except Error as batch_error:
                print(f"Error inserting batch {(i//batch_size) + 1}: {batch_error}")
                connection.rollback()

    except Error as e:
        print(f"Error inserting data into {table_name}: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def process_csv_files_in_same_directory():
    """
    Processes a predefined list of CSV files and inserts their data into MySQL tables.
    """
    files_to_read = [
        "time_dimension", "accounts", "subscribers", "transactions", 
        "calllogs", "messages", "isptraffic", "cryptoledgers", 
        "siminfo", "deviceinfo", "appinfo", "socialmedialogs", 
        "agents", "supportlogs", "auditlogs"
    ]
    
    try:
        for file_name in files_to_read:
            try:
                # Load the CSV file into a DataFrame
                print(f"Reading file: {file_name}.csv")
                dataframe = pd.read_csv(file_name + ".csv")
                print(f"Processing file: {file_name} with {len(dataframe)} rows")
                
                # Insert data into the corresponding MySQL table
                insert_data_to_mysql(file_name, dataframe)
                print(f"Completed processing {file_name}\n")
                
            except FileNotFoundError:
                print(f"File not found: {file_name}.csv")
            except pd.errors.EmptyDataError:
                print(f"Empty file: {file_name}.csv")
            except Exception as file_error:
                print(f"Error processing file {file_name}: {file_error}")
                
    except Exception as e:
        print(f"Error in main processing loop: {e}")

# Process all CSV files in the same directory
process_csv_files_in_same_directory()