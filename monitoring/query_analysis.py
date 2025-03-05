# Import python packages
import altair as alt
import streamlit as st
import datetime
import pandas as pd
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
import plotly.express as px
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import to_date
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from monitoring.utils import is_running_local, get_active_session, filter_date_time_frame

st.set_page_config(layout="wide")

# DEFINE DATE RANGE INPUTS
date_from_default = datetime.now() - relativedelta(years=1)
date_from = st.sidebar.date_input(label="From", value=date_from_default)
date_to = st.sidebar.date_input(label="To", value="today")

# INITIALIZE SNOWFLAKE SESSION
if __name__ == '__page__':
    if is_running_local():
        import os
        # RUN LOCALLY
        CONNECTION_PARAMETERS = {
            "account": os.environ["SF_ACCOUNT"],
            "user": os.environ["SF_USER"],
            "password": os.environ["SF_PASSWORD"],
            "role": "SYSADMIN",
            "database": "DBT_DEMO",
            "warehouse": "STANDARD_XS",
            "schema": "PUBLIC",
        }

        session = Session.builder.configs(CONNECTION_PARAMETERS).create()

    else: 
        # RUN ON SNOWFLAKE 
        session = get_active_session()


# DEFINE DATABASE AND SCHEMA
database = "monitoring"
schema = "monitoring_schema"


# ================================
# DATA QUERY FUNCTIONS
# ================================

# Function to Fetch Data from Snowflake
def get_data(query):
    """Executes a SQL query and returns the result as a Pandas DataFrame."""
    return pd.DataFrame(session.sql(query).collect())

# Queries by number of Times Executed and Execution Time
@st.cache_data(ttl=3600)
def query_num_of_executions():
    """Queries by number of Times Executed and Execution Time"""
    query = f'select * from table({database}.{schema}.query_num_of_executions())'
    return get_data(query)


# Qeries by Execution Buckets over the Past 5 months
@st.cache_data(ttl=3600)
def query_execution_time_groups():
    """Qeries by Execution Buckets over the Past 5 months"""
    query = f'select * from table({database}.{schema}.query_execution_time_groups())'
    return get_data(query)


# Qeries by Execution Buckets over the Past 5 months
@st.cache_data(ttl=3600)
def query_duration():
    """Qeries by Execution Buckets over the Past 5 months"""
    query = f'select * from table({database}.{schema}.query_duration())'
    return get_data(query)


# 50 longest queries
@st.cache_data(ttl=3600)
def longest_queries():
    """50 longest queries"""
    query = f'select * from table({database}.{schema}.top_50_longest_queries())'
    return get_data(query)

################################################################ STREAMLIT UI ################################################################

st.title("Query analysis")

# ================================
# DISPLAY NUMBER OF EXECUTIONS
# ================================
q_num_exec = query_num_of_executions()
q_num_exec_1 = q_num_exec.groupby(by=['DATE', 'WAREHOUSE_NAME']).sum().reset_index()
q_num_exec_filtered = filter_date_time_frame(q_num_exec_1, "query_num_of_executions", date_from, date_to)

st.bar_chart(q_num_exec_filtered, x = 'DATE', y = 'NUMBER_OF_QUERIES', color = 'WAREHOUSE_NAME', stack=True)

st.subheader("Repeated queries")
st.bar_chart(q_num_exec_filtered, x = 'DATE', y = 'NUMBER_OF_QUERIES', color = 'QUERY_TEXT')



# ================================
# QUERY SEGMENTATION INTO TIME GROUPS
# ================================
df = query_execution_time_groups()
df = filter_date_time_frame(df, "query_num_of_executions", date_from, date_to)
df1 = df.groupby(by=['DATE', 'TIME_GROUP']).sum().reset_index()

st.subheader("Query segmentation into time groups")
st.bar_chart(df1, x = "DATE", y = 'COUNT_OF_QUERIES', color = 'TIME_GROUP', stack=False, use_container_width=True)

df2 = df.groupby(by=['DATE', 'WAREHOUSE_NAME']).sum().reset_index()
st.subheader("Execution minutes per WH")
st.bar_chart(df2, x = "DATE", y = 'EXECUTION_MINUTES', color = 'WAREHOUSE_NAME')



# ================================
# QUERY DURATION FILTERING
# ================================
q_duration = query_duration()

wh_names = set(q_duration["WAREHOUSE_NAME"])

# duration_in_minutes = st.slider('duration in minutes', 0.1, 600.00)
# warehouse_name = st.selectbox('warehouse_name', wh_names)
# execution_start = st.date_input("Execution start")

# q_duration = q_duration[q_duration["EXECUTION_MINUTES"] >= duration_in_minutes]
# q_duration = q_duration[q_duration["WAREHOUSE_NAME"] == warehouse_name]
# q_duration = q_duration[q_duration["START_TIME"] == execution_start]



# ================================
# DISPLAY LONGEST RUNNING QUERIES
# ================================
st.subheader("Longest running queries by user")

longest_q = longest_queries()
st.dataframe(longest_q, use_container_width=True)
longest_q = longest_q.groupby(by=['USER_NAME', 'YEAR_MONTH_WEEK']).sum().reset_index()


st.bar_chart(longest_q, x = "YEAR_MONTH_WEEK", y = "EXECUTION_TIME_MINUTES", color = "USER_NAME")