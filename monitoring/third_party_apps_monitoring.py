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


# 3rd party apps consumption
def third_party_apps_consumption():
    """Fetch data for 3rd party apps analysis"""
    query = f'select * from {database}.{schema}.third_party_apps_consumption'
    return get_data(query)


# SF automation task
def sf_automation_task():
    query = f'select * from {database}.{schema}.sf_automation_task'
    return get_data(query)



################################ STREAMLIT UI ################################


# 3rd party apps consumption
st.title("3rd party apps consumption")

df = third_party_apps_consumption()

st.bar_chart(df, x = 'CLIENT_APPLICATION_NAME', y = 'APPROXIMATE_CREDITS_USED', color = 'WAREHOUSE_NAME')



# SF automation task

st.subheader("SF automation task")

df = sf_automation_task()

fig = go.Figure()

# bar chart by NAME
for warehouse in df['NAME'].unique():
    warehouse_data = df[df['NAME'] == warehouse]
    fig.add_trace(go.Bar(
        x=warehouse_data['DATE'],
        y=warehouse_data['DURATION_HOURS'],
        name=f'DURATION_HOURS ({warehouse})',
        opacity=0.6,
        text=warehouse_data['DURATION_HOURS'],
        textposition='inside',  
        textfont=dict(color='white')  
    ))

# CREDITS_USED
fig.add_trace(go.Scatter(
    x=df['DATE'],
    y=df['CREDITS_USED'],
    mode='lines',
    name='CREDITS_USED',
    line=dict(color='blue')
))

# Update layout
fig.update_layout(
    title='Daily task durations and credits used',
    xaxis_title='Date',
    yaxis_title='Values',
    barmode='group'
)

st.plotly_chart(fig, use_container_width=True)


st.subheader("Number of same queries executed (all groups summed)")

st.area_chart(df, x = 'DATE', y = 'EXE_COUNT', color = 'DATABASE_NAME')

