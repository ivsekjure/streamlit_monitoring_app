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

# DEFINE DATABASE AND SCHEMA WHERE THE VIEWS ARE CREATED
database = "monitoring"
schema = "monitoring_schema"


# ================================
# DATA QUERY FUNCTIONS
# ================================

# Function to Fetch Data from Snowflake
def get_data(query):
    """Executes a SQL query and returns the result as a Pandas DataFrame."""
    return pd.DataFrame(session.sql(query).collect())


# 3rd party apps consumption
@st.cache_data(ttl=3600)
def third_party_apps_consumption():
    """Fetch data for 3rd party apps analysis"""
    query = f'select * from table({database}.{schema}.third_party_apps_consumption())'
    return get_data(query)


# SF automation task
@st.cache_data(ttl=3600)
def sf_automation_task():
    query = f'select * from table({database}.{schema}.sf_automation_task())'
    return get_data(query)



################################################################ STREAMLIT UI ################################################################

# ================================
# 3RD PARTY APPS CONSUMPTION
# ================================
st.title("3rd party apps consumption")

third_party_consumption = third_party_apps_consumption()
third_party_consumption.sort_values(by='APPROXIMATE_CREDITS_USED', ascending=False)

unique_whs = third_party_consumption['WAREHOUSE_NAME'].unique()
color_map = {cat: px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)] for i, cat in enumerate(unique_whs)}

bar_charts = []
for warehouse in unique_whs:
    warehouse_data = third_party_consumption[third_party_consumption['WAREHOUSE_NAME'] == warehouse]
    bar_chart = go.Bar(
        x=warehouse_data['APPROXIMATE_CREDITS_USED'],
        y=warehouse_data['CLIENT_APPLICATION_NAME'],
        name=f'APPROXIMATE_CREDITS_USED - Warehouse {warehouse}',
        marker_color=color_map[warehouse],
        orientation='h'
    )
    bar_charts.append(bar_chart)

fig = go.Figure(data=bar_charts)

# fig = go.Figure(go.Bar(
#     x=third_party_consumption['APPROXIMATE_CREDITS_USED'],
#     y=third_party_consumption['CLIENT_APPLICATION_NAME'],
#     orientation='h',
#     marker=dict(color=[color_map[cat] for cat in third_party_consumption['WAREHOUSE_NAME']])
# ))

# Update layout
fig.update_layout(
    xaxis_title='APPROXIMATE_CREDITS_USED',
    yaxis_title='CLIENT_APPLICATION_NAME',
    yaxis={'categoryorder': 'total ascending'},
    barmode='stack',
    legend=dict(
        x=0.01,  
        y=1.5,  
    )
)

# Display the chart in Streamlit
st.plotly_chart(fig)
# st.bar_chart(third_party_consumption, x = 'CLIENT_APPLICATION_NAME', y = 'APPROXIMATE_CREDITS_USED', color = 'WAREHOUSE_NAME', horizontal=True)


# ================================
# SF AUTOMATION TASK
# ================================
st.subheader("SF automation task")

automation_task = sf_automation_task()
automation_task = filter_date_time_frame(automation_task, "sf_automation_task", date_from, date_to)


fig = go.Figure()

# bar chart by NAME
for warehouse in automation_task['NAME'].unique():
    warehouse_data = automation_task[automation_task['NAME'] == warehouse]
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
    x=automation_task['DATE'],
    y=automation_task['CREDITS_USED'],
    mode='lines',
    name='CREDITS_USED',
    line=dict(color='blue')
))

# Update layout
fig.update_layout(
    title='Daily task durations and credits used',
    xaxis_title='Date',
    yaxis_title='Hours',
    barmode='group'
)

st.plotly_chart(fig, use_container_width=True)



# ================================
# NUMBER OF SAME QUERIES EXECUTED
# ================================
st.subheader("Number of same queries executed (all groups summed)")

st.area_chart(automation_task, x = 'DATE', y = 'EXE_COUNT', color = 'DATABASE_NAME')

