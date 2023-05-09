import pandas as pd
import os
import json
import psycopg2
import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px

# !git clone https://github.com/PhonePe/pulse.git  --> Cloning the data from GitHub

# Making connection between postgres and python interface
conn = psycopg2.connect(host="localhost", user="postgres", password="Smile!098", port=5432, database="phonepe")
cur = conn.cursor()


def aggregated_transaction():
    c_one = {"State": [], "Year": [], "Quarter": [], "Transaction_type": [], "Transaction_amount": [],
             "Transaction_count": []}
    agg_trans = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\aggregated\transaction\country\india\state"
    agg_trans_state = os.listdir(agg_trans)
    for i in agg_trans_state:
        state_list_path = agg_trans + "\\" + i
        state_list_at = os.listdir(state_list_path)
        for j in state_list_at:
            year_list_path = state_list_path + "\\" + j
            year_list_at = os.listdir(year_list_path)
            for k in year_list_at:
                quarter_at = year_list_path + "\\" + k
                file = open(quarter_at, "r")
                data = json.load(file)
                for Z in data["data"]["transactionData"]:
                    name = Z["name"]
                    count = Z["paymentInstruments"][0]["count"]
                    amount = Z["paymentInstruments"][0]["amount"]
                    c_one["State"].append(i)
                    c_one["Year"].append(j)
                    c_one["Quarter"].append(int(k.strip(".json")))
                    c_one["Transaction_type"].append(name)
                    c_one["Transaction_count"].append(count)
                    c_one["Transaction_amount"].append(amount)
    agg_transaction_df = pd.DataFrame(c_one)

    cur.execute("CREATE TABLE AGG_TRANS (STATE VARCHAR(50), YEAR INT, QUARTER INT, TRANSACTION_TYPE VARCHAR(50), "
                "TRANSACTION_COUNT INT, TRANSACTION_AMOUNT REAL)")

    a = "INSERT INTO AGG_TRANS (STATE, YEAR, QUARTER, TRANSACTION_TYPE, TRANSACTION_COUNT, TRANSACTION_AMOUNT) " \
        "VALUES (%s, %s, %s, %s, %s, %s)"
    for index, i in agg_transaction_df.iterrows():
        result_1 = (i["State"], i["Year"], i["Quarter"], i["Transaction_type"],
                    i["Transaction_count"], i["Transaction_amount"])
        cur.execute(a, result_1)
    conn.commit()


def aggregated_user():
    c_two = {"State": [], "Year": [], "Quarter": [], "registered_users": [], "users_by_device": [],
             "users_by_device_count": []}
    agg_user = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\aggregated\user\country\india\state"
    agg_user_state = os.listdir(agg_user)
    for i in agg_user_state:
        state_list_path = agg_user + "\\" + i
        state_list_au = os.listdir(state_list_path)
        for j in state_list_au:
            year_list_path = state_list_path + "\\" + j
            year_list_au = os.listdir(year_list_path)
            for k in year_list_au:
                quarter_au = year_list_path + "\\" + k
                file = open(quarter_au, "r")
                data = json.load(file)
                if data["data"]["usersByDevice"] is not None:
                    for Z in data["data"]["usersByDevice"]:
                        device = Z["brand"]
                        count = Z["count"]
                        reg_users = data["data"]["aggregated"]["registeredUsers"]
                        c_two["State"].append(i)
                        c_two["Year"].append(j)
                        c_two["Quarter"].append(int(k.strip(".json")))
                        c_two["users_by_device"].append(device)
                        c_two["users_by_device_count"].append(count)
                        c_two["registered_users"].append(reg_users)
    agg_user_df = pd.DataFrame(c_two)

    cur.execute("CREATE TABLE AGG_USER (STATE VARCHAR(50), YEAR INT, QUARTER INT, REGISTERED_USERS INT, "
                "USERS_DEVICE VARCHAR(15),USERS_DEVICE_COUNT INT)")

    a = "INSERT INTO AGG_USER (STATE, YEAR, QUARTER, REGISTERED_USERS, USERS_DEVICE, USERS_DEVICE_COUNT) " \
        "VALUES (%s, %s, %s, %s, %s, %s)"
    for index, i in agg_user_df.iterrows():
        result_2 = (i["State"], i["Year"], i["Quarter"], i["registered_users"],
                    i["users_by_device"], i["users_by_device_count"])
        cur.execute(a, result_2)
    conn.commit()


def map_transaction():
    c_three = {"State": [], "District": [], "Year": [], "Quarter": [], "Count": [], "Amount": []}
    map_trans = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\map\transaction\hover\country\india\state"
    map_trans_state = os.listdir(map_trans)
    for i in map_trans_state:
        state_list_path = map_trans + "\\" + i
        state_list_mt = os.listdir(state_list_path)
        for j in state_list_mt:
            year_list_path = state_list_path + "\\" + j
            year_list_mt = os.listdir(year_list_path)
            for k in year_list_mt:
                quarter_mt = year_list_path + "\\" + k
                file = open(quarter_mt, "r")
                data = json.load(file)
                for Z in data["data"]["hoverDataList"]:
                    district = Z["name"]
                    count = Z["metric"][0]["count"]
                    amount = Z["metric"][0]["amount"]
                    c_three["State"].append(i)
                    c_three["Year"].append(j)
                    c_three["Quarter"].append(int(k.strip(".json")))
                    c_three["District"].append(district)
                    c_three["Count"].append(count)
                    c_three["Amount"].append(amount)
    map_transaction_df = pd.DataFrame(c_three)

    cur.execute("CREATE TABLE MAP_TRANS (STATE VARCHAR(50), DISTRICT VARCHAR(50), "
                "YEAR INT, QUARTER INT, COUNT INT, AMOUNT REAL)")

    a = "INSERT INTO MAP_TRANS (STATE, DISTRICT, YEAR, QUARTER, COUNT, AMOUNT) " \
        "VALUES (%s, %s, %s, %s, %s, %s)"
    for index, i in map_transaction_df.iterrows():
        result_3 = (i["State"], i["District"], i["Year"], i["Quarter"],
                    i["Count"], i["Amount"])
        cur.execute(a, result_3)
    conn.commit()


def map_user():
    c_four = {"State": [], "Quarter": [], "Year": [], "District": [], "Users": []}
    m_user = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\map\user\hover\country\india\state"
    map_user_state = os.listdir(m_user)
    for i in map_user_state:
        state_list_path = m_user + "\\" + i
        state_list_mu = os.listdir(state_list_path)
        for j in state_list_mu:
            year_list_path = state_list_path + "\\" + j
            year_list_mu = os.listdir(year_list_path)
            for k in year_list_mu:
                quarter_mu = year_list_path + "\\" + k
                file = open(quarter_mu, "r")
                data = json.load(file)
                if data["data"]["hoverData"].items() is not None:
                    for dist, r_user in data["data"]["hoverData"].items():
                        c_four["District"].append(dist)
                        c_four["Users"].append(r_user["registeredUsers"])
                        c_four["State"].append(i)
                        c_four["Year"].append(j)
                        c_four["Quarter"].append(int(k.strip(".json")))
    map_user_df = pd.DataFrame(c_four)

    cur.execute("CREATE TABLE MAP_USER (STATE VARCHAR(50), QUARTER INT ,YEAR INT, DISTRICT VARCHAR(50), USERS INT)")

    a = "INSERT INTO MAP_USER (STATE, QUARTER, YEAR, DISTRICT, USERS) " \
        "VALUES (%s, %s, %s, %s, %s)"
    for index, i in map_user_df.iterrows():
        result_4 = (i["State"], i["Quarter"], i["Year"],
                    i["District"], i["Users"])
        cur.execute(a, result_4)
    conn.commit()


def top_transaction():
    c_five = {"State": [], "Year": [], "Quarter": [], "District": [], "Count": [], "Amount": []}
    top_trans = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\top\transaction\country\india\state"
    top_trans_state = os.listdir(top_trans)
    for i in top_trans_state:
        state_list_path = top_trans + "\\" + i
        state_list_tt = os.listdir(state_list_path)
        for j in state_list_tt:
            year_list_path = state_list_path + "\\" + j
            year_list_tt = os.listdir(year_list_path)
            for k in year_list_tt:
                quarter_tt = year_list_path + "\\" + k
                file = open(quarter_tt, "r")
                data = json.load(file)
                if data["data"]["districts"] is not None:
                    for Z in data["data"]["districts"]:
                        district = Z["entityName"]
                        count = Z["metric"]["count"]
                        amount = Z["metric"]["amount"]
                        c_five["State"].append(i)
                        c_five["Year"].append(j)
                        c_five["Quarter"].append(int(k.strip(".json")))
                        c_five["District"].append(district)
                        c_five["Count"].append(count)
                        c_five["Amount"].append(amount)
    top_trans_df = pd.DataFrame(c_five)

    cur.execute("CREATE TABLE TOP_TRANS (STATE VARCHAR(50), YEAR INT, QUARTER INT, "
                "DISTRICT VARCHAR(50), COUNT INT, AMOUNT REAL)")

    a = "INSERT INTO TOP_TRANS (STATE, YEAR, QUARTER, DISTRICT, COUNT, AMOUNT) " \
        "VALUES (%s, %s, %s, %s, %s, %s)"
    for index, i in top_trans_df.iterrows():
        result_5 = (i["State"], i["Year"], i["Quarter"],
                    i["District"], i["Count"], i["Amount"])
        cur.execute(a, result_5)
    conn.commit()


def top_transaction_pincode():
    c_six = {"State": [], "Year": [], "Quarter": [], "Pincode": [], "Count": [], "Amount": []}
    top_trans = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\top\transaction\country\india\state"
    top_trans_state = os.listdir(top_trans)
    for i in top_trans_state:
        state_list_path = top_trans + "\\" + i
        state_list_ttp = os.listdir(state_list_path)
        for j in state_list_ttp:
            year_list_path = state_list_path + "\\" + j
            year_list_ttp = os.listdir(year_list_path)
            for k in year_list_ttp:
                quarter_ttp = year_list_path + "\\" + k
                file = open(quarter_ttp, "r")
                data = json.load(file)
                if data["data"]["pincodes"] is not None:
                    for Z in data["data"]["pincodes"]:
                        pin = Z["entityName"]
                        count = Z["metric"]["count"]
                        amount = Z["metric"]["amount"]
                        c_six["State"].append(i)
                        c_six["Year"].append(j)
                        c_six["Quarter"].append(int(k.strip(".json")))
                        c_six["Pincode"].append(pin)
                        c_six["Count"].append(count)
                        c_six["Amount"].append(amount)
    top_trans_df = pd.DataFrame(c_six)

    cur.execute("CREATE TABLE TOP_TRANS_PINCODE (STATE VARCHAR(50), YEAR INT, QUARTER INT, "
                "PINCODE VARCHAR(10), COUNT INT, AMOUNT REAL)")

    a = "INSERT INTO TOP_TRANS_PINCODE (STATE, YEAR, QUARTER, PINCODE, COUNT, AMOUNT) " \
        "VALUES (%s, %s, %s, %s, %s, %s)"
    for index, i in top_trans_df.iterrows():
        result_6 = (i["State"], i["Year"], i["Quarter"],
                    i["Pincode"], i["Count"], i["Amount"])
        cur.execute(a, result_6)
    conn.commit()


def top_user():
    c_seven = {"State": [], "Year": [], "Quarter": [], "District": [], "Registered_users": []}
    t_user = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\top\user\country\india\state"
    top_user_state = os.listdir(t_user)
    for i in top_user_state:
        state_list_path = t_user + "\\" + i
        state_list_tu = os.listdir(state_list_path)
        for j in state_list_tu:
            year_list_path = state_list_path + "\\" + j
            year_list_tu = os.listdir(year_list_path)
            for k in year_list_tu:
                quarter_tu = year_list_path + "\\" + k
                file = open(quarter_tu, "r")
                data = json.load(file)
                if data["data"]["districts"] is not None:
                    for Z in data["data"]["districts"]:
                        district = Z["name"]
                        reg_user = Z["registeredUsers"]
                        c_seven["State"].append(i)
                        c_seven["Year"].append(j)
                        c_seven["Quarter"].append(int(k.strip(".json")))
                        c_seven["District"].append(district)
                        c_seven["Registered_users"].append(reg_user)
    top_user_df = pd.DataFrame(c_seven)

    cur.execute("CREATE TABLE TOP_USER (STATE VARCHAR(50), YEAR INT, QUARTER INT, "
                "DISTRICT VARCHAR(50), REGISTERED_USERS INT)")

    a = "INSERT INTO TOP_USER (STATE, YEAR, QUARTER, DISTRICT, REGISTERED_USERS) " \
        "VALUES (%s, %s, %s, %s, %s)"
    for index, i in top_user_df.iterrows():
        result_7 = (i["State"], i["Year"], i["Quarter"],
                    i["District"], i["Registered_users"])
        cur.execute(a, result_7)
    conn.commit()


def top_user_pincode():
    c_eight = {"State": [], "Year": [], "Quarter": [], "Pincode": [], "Registered_users": []}
    t_user = r"C:\Users\iswar\PycharmProjects\iswaryaProject\pulse\data\top\user\country\india\state"
    top_user_state = os.listdir(t_user)
    for i in top_user_state:
        state_list_path = t_user + "\\" + i
        state_list_tup = os.listdir(state_list_path)
        for j in state_list_tup:
            year_list_path = state_list_path + "\\" + j
            year_list_tup = os.listdir(year_list_path)
            for k in year_list_tup:
                quarter_tup = year_list_path + "\\" + k
                file = open(quarter_tup, "r")
                data = json.load(file)
                if data["data"]["pincodes"] is not None:
                    for Z in data["data"]["pincodes"]:
                        pin = Z["name"]
                        reg_user = Z["registeredUsers"]
                        c_eight["State"].append(i)
                        c_eight["Year"].append(j)
                        c_eight["Quarter"].append(int(k.strip(".json")))
                        c_eight["Pincode"].append(pin)
                        c_eight["Registered_users"].append(reg_user)
    top_user_df = pd.DataFrame(c_eight)

    cur.execute("CREATE TABLE TOP_USER_PINCODE (STATE VARCHAR(50), YEAR INT, QUARTER INT, "
                "PINCODE VARCHAR(10), REGISTERED_USERS INT)")

    a = "INSERT INTO TOP_USER_PINCODE (STATE, YEAR, QUARTER, PINCODE, REGISTERED_USERS) " \
        "VALUES (%s, %s, %s, %s, %s)"
    for index, i in top_user_df.iterrows():
        result_8 = (i["State"], i["Year"], i["Quarter"],
                    i["Pincode"], i["Registered_users"])
        cur.execute(a, result_8)
    conn.commit()


def create_table():
    aggregated_transaction()
    aggregated_user()
    map_transaction()
    map_user()
    top_transaction()
    top_transaction_pincode()
    top_user()
    top_user_pincode()


# create_table() --> Required table already exists in the database, the create_table() function has been commented out

state_in_sql = {"Andaman & Nicobar": "andaman-&-nicobar-islands", "Andhra Pradesh": "andhra-pradesh",
                "Arunachal Pradesh": "arunachal-pradesh",
                "Assam": "assam", "Bihar": "bihar", "Chandigarh": "chandigarh", "Chhattisgarh": "chhattisgarh",
                "Dadra & Nagar-Haweli & Daman & Diu": "dadra-&-nagar-haveli-&-daman-&-diu", "Delhi": "delhi",
                "Goa": "goa", "Gujarat": "gujarat", "Haryana": "haryana",
                "Himachal Pradesh": "himachal-pradesh", "Jammu & Kashmir": "jammu-&-kashmir", "Jharkhand": "jharkhand",
                "Karnataka": "karnataka",
                "Kerala": "kerala", "Ladak": "ladakh", "Lakshadweep": "lakshadweep", "Madhya Pradesh": "madhya-pradesh",
                "Maharastra": "maharashtra",
                "Manipur": "manipur", "Meghalaya": "meghalaya", "Mizoram": "mizoram", "Nagaland": "nagaland",
                "Odisha": "odisha",
                "Pudhuchery": "puducherry", "Punjab": "punjab", "Rajasthan": "rajasthan", "Sikkim": "sikkim",
                "TamilNadu": "tamil-nadu", "Telangana": "telangana",
                "Tiripura": "tripura", "Uttarkhand": "uttarakhand", "Uttar Pradesh": "uttar-pradesh",
                "West Bengal": "west-bengal"}

st.set_page_config(layout="wide")

with st.sidebar:
    opt = option_menu(
        menu_title="Menu",
        options=["Home", "Aggregated Analytics", "Top Analytics", "Insights"],
        icons=["house", "bar-chart", "pie-chart", "geo-alt"],
        menu_icon="cast",
        default_index=0)

if opt == "Home":
    st.title("Phonepe Pulse data Visualization and Exploration, Smile!098A user friendly tool using Streamlit and Plotly")
    st.write("Welcome to our Phonepe data analysis webpage! We have analyzed 5 years of Phonepe user data from "
             "2018 to 2022 in India, across all four quarters of each year, and have identified the states, districts, "
             "and pincodes with the highest transaction volume. Our analysis provides insights into the usage "
             "patterns of Phonepe users across the country.")
    st.write("By examining this data, we are able to pinpoint the specific regions in India where Phonepe usage is "
             "most prevalent. Our analysis includes state-level data as well as district and pincode-level data, so"
             " you can get a comprehensive understanding of how Phonepe is being used across the country.")
    st.write("Our website provides easy-to-use visualizations that allow you to explore the data and gain insights "
             "into the transaction patterns of Phonepe users. Whether you're a business owner looking to expand your "
             "customer base, or an individual interested in understanding your own usage patterns, our website has "
             "something for you. So why wait? Start exploring our data analysis today and gain valuable insights into "
             "the world of Phonepe transactions!")

elif opt == "Aggregated Analytics":
    st.title("Phonepe Pulse Data Visualization")
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        st.subheader("State")
        state_list = ["All", "Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
                      "Chandigarh", "Chhattisgarh", "Dadra & Nagar-Haweli & Daman & Diu", "Delhi", "Goa",
                      "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka",
                      "Kerala", "Ladak", "Lakshadweep", "Madhya Pradesh", "Maharastra", "Manipur", "Meghalaya",
                      "Mizoram", "Nagaland", "Odisha", "Pudhuchery", "Punjab", "Rajasthan", "Sikkim",
                      "TamilNadu", "Telangana", "Tiripura", "Uttarkhand", "Uttar Pradesh", "West Bengal"]
        state = st.selectbox(label="Select state", options=state_list)
        if state != "All":
            state = state_in_sql[state]
    with col2:
        st.subheader("Year")
        year_list = ["All", 2018, 2019, 2020, 2021, 2022]
        year = st.selectbox(label="Select year", options=year_list)
    with col3:
        st.subheader("Quarter")
        quarter_list = ["All", 1, 2, 3, 4]
        quarter = st.selectbox(label="Select quarter", options=quarter_list)

    search = st.button("Search")
    if search:
        # agg trans chart
        if state == "All" and year == "All" and quarter == "All":
            cur.execute("SELECT * FROM AGG_TRANS")
            a_t = cur.fetchall()
        elif state != "All" and year == "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE STATE = '{state}'")
            a_t = cur.fetchall()
        elif state == "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE YEAR = {year}")
            a_t = cur.fetchall()
        elif state == "All" and year == "All" and quarter != "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE QUARTER = {quarter}")
            a_t = cur.fetchall()
        elif state != "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE STATE = '{state}' AND YEAR = {year}")
            a_t = cur.fetchall()
        elif state == "All" and year != "All" and quarter != "All":
            cur.execute(f"SELECT * FROM AGG_TRANS WHERE YEAR = {year} AND QUARTER = {quarter}")
            a_t = cur.fetchall()
        else:
            cur.execute(
                f"SELECT * FROM AGG_TRANS WHERE STATE = '{state}' AND YEAR = {year} AND QUARTER = {quarter}")
            a_t = cur.fetchall()
        df_AT = pd.DataFrame(a_t, columns=["State", "Year", "Quarter", "Trans_type", "Trans_count", "Trans_amount"])
        fig = px.bar(df_AT, x="Trans_type", y="Trans_amount", color="Trans_type",
                     labels={"Trans_type": "Transaction type", "Trans_amount": "Transaction amount"},
                     title="Aggregated Transaction")
        st.plotly_chart(fig)

        # agg user chart
        if state == "All" and year == "All" and quarter == "All":
            cur.execute("SELECT * FROM AGG_USER")
            a_u = cur.fetchall()
        elif state != "All" and year == "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_USER WHERE STATE = '{state}'")
            a_u = cur.fetchall()
        elif state == "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_USER WHERE YEAR = {year}")
            a_u = cur.fetchall()
        elif state == "All" and year == "All" and quarter != "All":
            cur.execute(f"SELECT * FROM AGG_USER WHERE QUARTER = {quarter}")
            a_u = cur.fetchall()
        elif state != "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT * FROM AGG_USER WHERE STATE = '{state}' AND YEAR = {year}")
            a_u = cur.fetchall()
        elif state == "All" and year != "All" and quarter != "All":
            cur.execute(f"SELECT * FROM AGG_USER WHERE YEAR = {year} AND QUARTER = {quarter}")
            a_u = cur.fetchall()
        else:
            cur.execute(f"SELECT * FROM AGG_USER WHERE STATE = '{state}' AND YEAR = {year} "
                        f"AND QUARTER = {quarter}")
            a_u = cur.fetchall()
        df_AU = pd.DataFrame(a_u,
                             columns=["State", "Year", "Quarter", "Reg_users", "Users_device", "User_device_count"])
        fig_1 = px.bar(df_AU, x="Users_device", y="User_device_count", color="Users_device",
                       labels={"Users_device": "Users Device", "User_device_count": "User device by count"},
                       title="Aggregated User")
        st.plotly_chart(fig_1)

elif opt == "Top Analytics":
    st.title("Phonepe Pulse Data Visualization")
    col1, col2 = st.columns([2, 2])
    with col1:
        st.subheader("Year")
        year_list = ["All", 2018, 2019, 2020, 2021, 2022]
        year = st.selectbox(label="Select year", options=year_list)
    with col2:
        st.subheader("Quarter")
        quarter_list = ["All", 1, 2, 3, 4]
        quarter = st.selectbox(label="Select quarter", options=quarter_list)

    page_names = ["State", "District", "Pincode"]
    page = st.radio("select", page_names, horizontal=True, label_visibility="hidden")
    res1 = st.button("Search")

    if page == "State" and res1:
        if year == "All" and quarter == "All":
            cur.execute(
                "SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS GROUP BY "
                "STATE ORDER BY SUM(AMOUNT) DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title="Top ten state-wise transactions for the last five years and all four quarters")
            st.plotly_chart(fig)

            cur.execute("SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER GROUP BY STATE "
                        "ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg_users"])
            fig_1 = px.pie(df_u, values="Reg_users", names="State", color="State",
                           title="Top ten state-wise users for the last five years and all four quarters")
            st.plotly_chart(fig_1)

        elif year != "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS "
                        f"WHERE YEAR={year} GROUP BY STATE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title=f"Top ten state-wise transactions for {year} year and all four quarters ")
            st.plotly_chart(fig)

            cur.execute(f"SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER WHERE YEAR={year} "
                        f"GROUP BY STATE ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg_users"])
            fig_1 = px.pie(df_u, values="Reg_users", names="State", color="State",
                           title=f"Top ten state-wise users for {year} year and all four quarters")
            st.plotly_chart(fig_1)

        elif year == "All" and quarter != "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS "
                        f"WHERE QUARTER={quarter} GROUP BY STATE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title=f"Top ten state-wise transactions for all years and {quarter} quarter")
            st.plotly_chart(fig)

            cur.execute(f"SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER WHERE "
                        f"QUARTER={quarter} GROUP BY STATE ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg Users"])
            fig_1 = px.pie(df_u, values="Reg Users", names="State", color="State",
                           title=f"Top ten state-wise users for all years and {quarter} quarter")
            st.plotly_chart(fig_1)

        else:
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_TRANS WHERE YEAR={year} AND QUARTER={quarter} GROUP BY STATE ORDER BY TOTAL_AMOUNT "
                        f"DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
            fig = px.pie(df, values="Total Amount", names="State", color="State",
                         title=f"Top ten state-wise transactions for year {year} and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT STATE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS "
                        f"FROM TOP_USER WHERE YEAR={year} AND QUARTER={quarter} GROUP BY STATE ORDER BY "
                        f"TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["State", "Reg_users"])
            fig_1 = px.pie(df_u, values="Reg_users", names="State", color="State",
                           title=f"Top ten state-wise users for year {year} and quarter {quarter}")
            st.plotly_chart(fig_1)

    if page == "District" and res1:
        if year == "All" and quarter == "All":
            cur.execute("SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS "
                        "GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title="Top ten district-wise transactions for all the five years and four quarters")
            st.plotly_chart(fig)

            cur.execute("SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_USER "
                        "GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title="Top ten district-wise users for all the five years and four quarters")
            st.plotly_chart(fig_u)

        elif year != "All" and quarter == "All":
            cur.execute(f"SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS "
                        f"WHERE YEAR={year} GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title=f"Top ten district-wise transactions for {year} year and all four quarters")
            st.plotly_chart(fig)

            cur.execute(f"SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_USER WHERE "
                        f"YEAR={year} GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title=f"Top ten district-wise users for {year} year and all four quarters")
            st.plotly_chart(fig_u)

        elif year == "All" and quarter != "All":
            cur.execute(f"SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS "
                        f"WHERE QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title=f"Top ten district-wise transactions for all the five years and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_USER WHERE "
                        f"QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title=f"Top ten district-wise users for all the five years and quarter {quarter}")
            st.plotly_chart(fig_u)

        else:
            cur.execute(f"SELECT DISTRICT, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM TOP_TRANS "
                        f"WHERE YEAR={year} AND QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_AMOUNT "
                        f"DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["District", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="District",
                         title=f"Top ten district-wise transactions for year {year} and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT DISTRICT, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS  FROM TOP_USER WHERE "
                        f"YEAR={year} AND QUARTER={quarter} GROUP BY DISTRICT ORDER BY TOTAL_REG_USERS "
                        f"DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["District", "Total Reg users"])
            fig_u = px.pie(df_u, values="Total Reg users", names="District",
                           title=f"Top ten district-wise users for {year} year and quarter {quarter} ")
            st.plotly_chart(fig_u)

    if page == "Pincode" and res1:
        if year == "All" and quarter == "All":
            cur.execute("SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        "TOP_TRANS_PINCODE GROUP BY PINCODE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for all the five years and four quarters")
            st.plotly_chart(fig)

            cur.execute("SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER_PINCODE "
                        "GROUP BY PINCODE ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for all the five years and four quarters")
            st.plotly_chart(fig_u)

        elif year != "All" and quarter == "All":
            cur.execute(f"SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_TRANS_PINCODE WHERE YEAR={year} GROUP BY PINCODE ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for {year} year and all four quarters")
            st.plotly_chart(fig)

            cur.execute(f"SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER_PINCODE "
                        f"WHERE YEAR={year} GROUP BY PINCODE  ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for {year} year and all four quarters")
            st.plotly_chart(fig_u)

        elif year == "All" and quarter != "All":
            cur.execute(f"SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_TRANS_PINCODE WHERE QUARTER={quarter} GROUP BY PINCODE ORDER BY TOTAL_AMOUNT "
                        f"DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for all the five years and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER_PINCODE "
                        f"WHERE QUARTER={quarter} GROUP BY PINCODE  ORDER BY TOTAL_REG_USERS DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for all the five years and quarter {quarter} ")
            st.plotly_chart(fig_u)

        else:
            cur.execute(f"SELECT PINCODE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"TOP_TRANS_PINCODE WHERE YEAR={year} AND QUARTER={quarter} GROUP BY PINCODE "
                        f"ORDER BY TOTAL_AMOUNT DESC LIMIT 10")
            fetch = cur.fetchall()
            df = pd.DataFrame(fetch, columns=["Pincode", "Total_Amount", "Total Count"])
            fig = px.pie(df, values="Total_Amount", names="Pincode",
                         title=f"Top ten pincode-wise transactions for year  {year} and quarter {quarter}")
            st.plotly_chart(fig)

            cur.execute(f"SELECT PINCODE, SUM(REGISTERED_USERS) AS TOTAL_REG_USERS FROM TOP_USER_PINCODE "
                        f"WHERE YEAR={year} AND QUARTER={quarter} GROUP BY PINCODE  ORDER BY TOTAL_REG_USERS "
                        f"DESC LIMIT 10")
            fetch_u = cur.fetchall()
            df_u = pd.DataFrame(fetch_u, columns=["Pincode", "Total_reg_users"])
            fig_u = px.pie(df_u, values="Total_reg_users", names="Pincode",
                           title=f"Top ten pincode-wise transactions for year {year} and quarter {quarter}")
            st.plotly_chart(fig_u)
else:
    st.title("Phonepe Pulse Data Visualization")
    col1, col2, col3 = st.columns([3, 2, 2])
    with col1:
        st.subheader("State")
        state_list = ["All", "Andaman & Nicobar", "Andhra Pradesh", "Arunachal Pradesh", "Assam", "Bihar",
                      "Chandigarh", "Chhattisgarh", "Dadra & Nagar-Haweli & Daman & Diu", "Delhi", "Goa",
                      "Gujarat", "Haryana", "Himachal Pradesh", "Jammu & Kashmir", "Jharkhand", "Karnataka",
                      "Kerala", "Ladak", "Lakshadweep", "Madhya Pradesh", "Maharastra", "Manipur", "Meghalaya",
                      "Mizoram", "Nagaland", "Odisha", "Pudhuchery", "Punjab", "Rajasthan", "Sikkim",
                      "TamilNadu", "Telangana", "Tiripura", "Uttarkhand", "Uttar Pradesh", "West Bengal"]
        state = st.selectbox(label="Select state", options=state_list)
        if state != "All":
            state = state_in_sql[state]
    with col2:
        st.subheader("Year")
        year_list = ["All", 2018, 2019, 2020, 2021, 2022]
        year = st.selectbox(label="Select year", options=year_list)
    with col3:
        st.subheader("Quarter")
        quarter_list = ["All", 1, 2, 3, 4]
        quarter = st.selectbox(label="Select quarter", options=quarter_list)

    result = st.button("Search")
    if result:
        state_name = {'All': 'All', 'andaman-&-nicobar-islands': 'Andaman & Nicobar',
                      'andhra-pradesh': 'Andhra Pradesh',
                      'arunachal-pradesh': 'Arunachal Pradesh', 'assam': 'Assam', 'bihar': 'Bihar',
                      'chandigarh': 'Chandigarh',
                      'chhattisgarh': 'Chhattisgarh',
                      'dadra-&-nagar-haveli-&-daman-&-diu': 'Dadra and Nagar Haveli and Daman and Diu',
                      'delhi': 'Delhi', 'goa': 'Goa', 'gujarat': 'Gujarat',
                      'haryana': 'Haryana', 'himachal-pradesh': 'Himachal Pradesh',
                      'jammu-&-kashmir': 'Jammu & Kashmir',
                      'jharkhand': 'Jharkhand',
                      'karnataka': 'Karnataka', 'kerala': 'Kerala', 'ladakh': 'Ladakh', 'lakshadweep': 'Lakshadweep',
                      'madhya-pradesh': 'Madhya Pradesh',
                      'maharashtra': 'Maharashtra', 'manipur': 'Manipur', 'meghalaya': 'Meghalaya',
                      'mizoram': 'Mizoram',
                      'nagaland': 'Nagaland',
                      'odisha': 'Odisha', 'puducherry': 'Puducherry', 'punjab': 'Punjab', 'rajasthan': 'Rajasthan',
                      'sikkim': 'Sikkim',
                      'tamil-nadu': 'Tamil Nadu', 'telangana': 'Telangana', 'tripura': 'Tripura',
                      'uttar-pradesh': 'Uttar Pradesh',
                      'uttarakhand': 'Uttarakhand', 'west-bengal': 'West Bengal'}

        if state == "All" and year == "All" and quarter == "All":
            cur.execute("SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT "
                        "FROM MAP_TRANS GROUP BY STATE")
            fetch = cur.fetchall()

        elif state != "All" and year == "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE STATE='{state}' GROUP BY STATE")
            fetch = cur.fetchall()

        elif state == "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE YEAR={year} GROUP BY STATE")
            fetch = cur.fetchall()

        elif state == "All" and year == "All" and quarter != "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE QUARTER={quarter} GROUP BY STATE")
            fetch = cur.fetchall()

        elif state != "All" and year != "All" and quarter == "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE STATE='{state}' AND YEAR={year} GROUP BY STATE")
            fetch = cur.fetchall()

        elif state == "All" and year != "All" and quarter != "All":
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE YEAR={year} AND QUARTER={quarter} GROUP BY STATE")
            fetch = cur.fetchall()

        else:
            cur.execute(f"SELECT STATE, SUM(AMOUNT) AS TOTAL_AMOUNT, SUM(COUNT) AS TOTAL_COUNT FROM "
                        f"MAP_TRANS WHERE STATE='{state}' AND YEAR={year} AND QUARTER={quarter} GROUP BY "
                        f"STATE")
            fetch = cur.fetchall()

        df = pd.DataFrame(fetch, columns=["State", "Total Amount", "Total count"])
        df["State"] = df["State"].replace(state_name)
        fig = px.choropleth(df,
                            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112"
                                    "/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey="properties.ST_NM",
                            locations="State",
                            color="Total Amount",
                            hover_data=["State", "Total Amount", "Total count"],
                            color_continuous_scale="Bluyl")
        fig.update_geos(fitbounds='locations', visible=False)
        fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
        fig.show()
