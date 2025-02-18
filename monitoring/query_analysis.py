# Import python packages
import altair as alt
import streamlit as st
import datetime
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from snowflake.snowpark.functions import col
from snowflake.snowpark.functions import to_date
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide")


# Get the current credentials
session = get_active_session()

# Define database and schema where the views are created
database = "dbt_demo"
schema = "public"



# Function to Fetch Data from Snowflake
def get_data(query):
    """Executes a SQL query and returns the result as a Pandas DataFrame."""
    return session.sql(query).to_pandas()

# Queries by number of Times Executed and Execution Time
def query_num_of_executions():
    """Queries by number of Times Executed and Execution Time"""
    query = f'select * from {database}.{schema}.query_num_of_executions'
    return get_data(query)


# Qeries by Execution Buckets over the Past 5 months
def query_execution_time_groups():
    """Qeries by Execution Buckets over the Past 5 months"""
    query = f'select * from {database}.{schema}.query_execution_time_groups'
    return get_data(query)


# Qeries by Execution Buckets over the Past 5 months
def query_duration():
    """Qeries by Execution Buckets over the Past 5 months"""
    query = f'select * from {database}.{schema}.query_duration'
    return get_data(query)


# 50 longest queries
def longest_queries():
    """50 longest queries"""
    query = f'select * from {database}.{schema}.top_50_longest_queries'
    return get_data(query)


################################ STREAMLIT UI ################################

st.title("Query analysis")

df = query_num_of_executions()
#df = df[df["QUERY_TEXT"] != ' ']
st.bar_chart(df, x = 'DATE', y = 'NUMBER_OF_QUERIES', color = 'WAREHOUSE_NAME')

df = query_num_of_executions()
#df = df[df["QUERY_TEXT"] != '/']
st.subheader("Repeated queries")
st.line_chart(df, x = 'DATE', y = 'NUMBER_OF_QUERIES', color = 'QUERY_TEXT')





df = query_execution_time_groups()

st.subheader("Query segmentation into time groups")
st.bar_chart(df, x = "DATE", y = 'COUNT_OF_QUERIES', color = 'TIME_GROUP')

st.subheader("Execution minutes per WH")
st.bar_chart(df, x = "DATE", y = 'EXECUTION_MINUTES', color = 'WAREHOUSE_NAME')




df = query_duration()

wh_names = set(df["WAREHOUSE_NAME"])

duration_in_minutes = st.slider('duration in minutes', 0.1, 600.00)
warehouse_name = st.selectbox('warehouse_name', wh_names)
execution_start = st.date_input("Execution start")

df = df[df["EXECUTION_MINUTES"] >= duration_in_minutes]
df = df[df["WAREHOUSE_NAME"] == warehouse_name]
df = df[df["START_TIME"] == execution_start]



df = longest_queries()

st.subheader("Longest running queries by user")
st.bar_chart(df, x = "YEAR_MONTH_WEEK", y = "EXECUTION_TIME_MINUTES", color = "USER_NAME")