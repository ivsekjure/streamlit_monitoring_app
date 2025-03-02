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
from snowflake.snowpark.functions import col, current_role
from snowflake.snowpark.functions import to_date
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from monitoring.utils import is_running_local, get_active_session, filter_date_time_frame



st.set_page_config(layout="wide")
date_from_default = datetime.now() - relativedelta(years=1)
date_from = st.sidebar.date_input(label="From", value=date_from_default)
date_to = st.sidebar.date_input(label="To", value="today")

if __name__ == '__page__':
    if is_running_local():
        import os
        # # RUN LOCALLY
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
        # # RUN ON SNOWFLAKE 
        session = get_active_session()

# # Get the current credentials
# session = get_active_session()


# Define database and schema where the views are created
database = "monitoring"
schema = "monitoring_schema"


# Function to Fetch Data from Snowflake
def get_data(query):
    """Executes a SQL query and returns the result as a Pandas DataFrame."""
    return pd.DataFrame(session.sql(query).collect())


# Function to fetch warehouse data
@st.cache_data(ttl=3600)
def show_warehouses():
    """Fetch warehouse information."""
    query = f'select * from  table({database}.{schema}.warehouses_vw())'
    return get_data(query)


# Function to Fetch Monthly WH Consumption
@st.cache_data(ttl=3600)
def wh_monthly_consumption_f():
    """Fetch monthly warehouse consumption."""
    query = f'select * from  table({database}.{schema}.wh_monthly_consumption_vw())'
    return get_data(query)


# Function to fetch average day-by-day consumption
@st.cache_data(ttl=3600)
def avg_day_by_day_consumption():
    """Fetch day-by-day warehouse consumption data."""
    query = f'select * from  table({database}.{schema}.avg_day_by_day_consumption())'
    return get_data(query)


# Daily peaks
@st.cache_data(ttl=3600)
def daily_peaks():
    """Daily peaks"""
    query = f'select * from  table({database}.{schema}.daily_peaks())'
    return get_data(query)


# Autosuspend costs
@st.cache_data(ttl=3600)
def autosuspend_costs():
    """Fetch information about autosuspend costs"""
    query = f'select * from  table({database}.{schema}.autosuspend_costs())'
    return get_data(query)


# 7 day average trend
@st.cache_data(ttl=3600)
def seven_day_average_trend():
    """Get 7 days average trend"""
    query = f'select * from  table({database}.{schema}.seven_day_average_trend())'
    return get_data(query)





################################ STREAMLIT UI ################################


st.title("Warehouse monitoring")


# Warehouse information
show_wh_data = show_warehouses()

abc = show_wh_data
st.subheader("List of Warehouses by size")
st.bar_chart(show_wh_data, x = 'WH_NAME', y = 'AUTO_SUSPEND', color = 'WH_SIZE')



# Monthly warehouse consumption (YEAR_MONTH)
monthly_consumption = wh_monthly_consumption_f()
monthly_consumption = filter_date_time_frame(monthly_consumption, "wh_monthly_consumption_f", date_from, date_to)

# monthly_consumption.CREDITS_USED = monthly_consumption.CREDITS_USED.astype(int)
st.subheader('Monthly warehouse consumption compute')
st.bar_chart(monthly_consumption, y = 'CREDITS_USED', x = 'YEAR_MONTH',  color = 'NAME')
st.subheader('Monthly warehouse serverless compute')
st.bar_chart(monthly_consumption, y = 'CREDITS_USED', x = 'YEAR_MONTH',  color = 'SERVICE_TYPE' )


st.table(monthly_consumption)


# Average day-by-day warehouse consumption
df = avg_day_by_day_consumption()
df = filter_date_time_frame(df, "avg_day_by_day_consumption", date_from, date_to)

st.subheader('Average Day-by-Day Consumption')

# Generate a color palette
color_palette = px.colors.qualitative.Plotly

unique_warehouses = df['WAREHOUSE_NAME'].unique()

# Assign colors dynamically to each warehouse
warehouse_colors = {warehouse: color_palette[i % len(color_palette)] for i, warehouse in enumerate(unique_warehouses)}

# Create a bar chart for "AVERAGE" with different colors for each warehouse
bar_charts = []
for warehouse in unique_warehouses:
    warehouse_data = df[df['WAREHOUSE_NAME'] == warehouse]
    bar_chart = go.Bar(
        x=warehouse_data['DATE'], 
        y=warehouse_data['CREDITS_USED_COMPUTE'], 
        name=f'CREDITS_USED_COMPUTE - Warehouse {warehouse}',
        marker_color=warehouse_colors[warehouse]
    )
    bar_charts.append(bar_chart)

# Create a line chart for "DAILY"
line_chart1 = go.Scatter(x=df['DATE'], y=df['AVERAGE'], mode='lines+markers', name='AVERAGE of credits used')

# Create a line chart for "STD"
line_chart2 = go.Scatter(x=df['DATE'], y=df['STDV'], mode='lines+markers', name='STDV of credits used')

fig = go.Figure(data=bar_charts + [line_chart1, line_chart2])

# Layout settings (optional)
fig.update_layout(
    barmode='stack',
    xaxis_title='DATE',
    yaxis_title='Values',
    legend_title='Legend',
    legend=dict(
        x=0.01,  
        y=1.5,  
    )
)

# Streamlit app
st.plotly_chart(fig)



# SNOWFLAKE COMPONENTS
df = wh_monthly_consumption_f()
df = filter_date_time_frame(df, "wh_monthly_consumption_f", date_from, date_to)

df1 = df[df["SERVICE_TYPE"] != 'WAREHOUSE_METERING']
# Create a horizontal bar chart using Plotly
fig = go.Figure(go.Bar(
    x=df1['CREDITS_USED'],
    y=df1['SERVICE_TYPE'],
    orientation='h'
))

fig.update_layout(
    title='Overall consumption by SF components excluding WH metering',
    xaxis_title='CREDITS USED',
    yaxis_title='SERVICE TYPE',
    barmode='group'
)

# Display the chart in Streamlit
st.plotly_chart(fig)



df2 = df[df["SERVICE_TYPE"] != 'WAREHOUSE_METERING']

# Create the figure
fig = go.Figure()

# Bar chart by SERVICE_TYPE
for service_group in df2['SERVICE_TYPE'].unique():
    service_data = df2[df2['SERVICE_TYPE'] == service_group]
    fig.add_trace(go.Bar(
        y=service_data['NAME'],
        x=service_data['CREDITS_USED'],
        name=service_group,
        orientation='h',
        text=service_data['CREDITS_USED'],
        textposition='inside',  
        textfont=dict(color='white')  
    ))

# Update layout
fig.update_layout(
    title='Credits used breakdown by warehouse',
    xaxis_title='Credits Used',
    yaxis_title='Warehouse Name',
    barmode='stack', 
    legend_title='Service Group'
)

# Display the chart in Streamlit
st.plotly_chart(fig, use_container_width=True)



# Daily peaks

st.subheader('Daily peeks')

df = daily_peaks()

df1 = df.fillna(0)
df1 = df1.iloc[:, :16]

wh_names = set(df1["WAREHOUSE_NAME"])
print(wh_names)


warehouse_name = st.selectbox('warehouse_name', wh_names, key=10)
year = st.selectbox('Year', range(1990, 2026), key=11)
month = st.selectbox('Month', range(1, 13), key=12)

df1 = df1[df1["WAREHOUSE_NAME"] == warehouse_name]
df1 = df1[df1["YEAR"] >= year]
df1 = df1[df1["MONTH"] == month]

st.subheader("Daily credit consumption 1st till 10th")
st.line_chart(df1, x = 'HOUR', y = ['1','2','3','4','5','6','7','8','9','10'])



df2 = df.fillna(0)
df2 = df2.drop(df.columns[6:15], axis=1)
df2 = df2.iloc[:, :17]
wh_names = set(df2["WAREHOUSE_NAME"])

warehouse_name = st.selectbox('warehouse_name', wh_names, key=20)
year = st.selectbox('Year', range(1990, 2026), key=21)
month = st.selectbox('Month', range(1, 13), key=22)

df2 = df2[df2["WAREHOUSE_NAME"] == warehouse_name]
df2 = df2[df2["YEAR"] >= year]
df2 = df2[df2["MONTH"] == month]

st.subheader("Daily credit consumption 11th till 20th")
st.line_chart(df2, x = 'HOUR', y = ['11','12','13','14','15','16','17','18','19','20'])



df3 = df.fillna(0)
df3 = df3.drop(df3.columns[6:26], axis=1)
df3.iloc[:, :17]
wh_names = set(df3["WAREHOUSE_NAME"])
print(wh_names)

warehouse_name = st.selectbox('warehouse_name', wh_names, key=30)
year = st.selectbox('Year', range(1990, 2026), key=31)
month = st.selectbox('Month', range(1, 13), key=32)

df3 = df3[df3["WAREHOUSE_NAME"] == warehouse_name]
df3 = df3[df3["YEAR"] >= year]
df3 = df3[df3["MONTH"] == month]

st.subheader("Daily credit consumption 21th till 31th")
st.line_chart(df3, x = 'HOUR', y = ['21','22','23','24','25','26','27','28','29','30','31'])



# Autosuspend costs

st.subheader('Autosuspend costs')

df = autosuspend_costs()
df = filter_date_time_frame(df, "autosuspend_costs", date_from, date_to)

fig = go.Figure()
fig.add_annotation(text='Excluding today\'s date',xref="paper", yref="paper", x=0, y=1.1, showarrow=False)

# Add the area chart for CREDITS_USED_TOTAL
fig.add_trace(go.Bar(
    x=df['DATE'],
    y=df['CREDITS_USED_TOTAL'],
    # mode='lines',
    name='CREDITS_USED_TOTAL'
    # fill='tozeroy'
))

# Add the area chart for CREDIT_USED_FOR_RESUME
fig.add_trace(go.Bar(
    x=df['DATE'],
    y=df['CREDIT_USED_FOR_RESUME'],
    # mode='lines',
    name='CREDIT_USED_FOR_RESUME'
    # fill='tonexty'
))

# Add the bar chart for PCN_AUTORESUME_COST
fig.add_trace(go.Bar(
    x=df['DATE'],
    y=df['PCN_AUTORESUME_COST'],
    name='PCN_AUTORESUME_COST',
    marker_color='rgba(255, 0, 0, 0.6)',
    opacity=0.5
))

# Update layout
fig.update_layout(
    title='Autosuspend daily cost',
    xaxis_title='Date',
    yaxis_title='Values',
    barmode='overlay'
)

# Render the chart in Streamlit
st.plotly_chart(fig, use_container_width=True)




# 7 day average trend
st.subheader('7 day average trend')

df = seven_day_average_trend()
df = filter_date_time_frame(df, "seven_day_average_trend", date_from, date_to)

warehouse_name = st.selectbox("Pick a desired warehouse: ",set(df["WAREHOUSE_NAME"]))


#warehouse_name = 'COMPUTE_WH' #CHANGE THE NAME OF DESIRED WAREHOUSE 
df = df[df["WAREHOUSE_NAME"] == warehouse_name]

# Create the figure
fig = go.Figure()

# Add the area chart for CREDITS_USED_DATE_WH
fig.add_trace(go.Scatter(
    x=df['START_DATE'],
    y=df['CREDITS_USED_DATE_WH'],
    mode='lines',
    name='CREDITS_USED_DATE_WH',
    fill='tozeroy'
))

# Add the area chart for CREDIT_USED_FOR_RESUME
fig.add_trace(go.Scatter(
    x=df['START_DATE'],
    y=df['CREDITS_USED_7_DAY_AVG'],
    mode='lines',
    name='CREDITS_USED_7_DAY_AVG',
#   fill='tonexty'
     line=dict(color='red')
)
             )

# Update layout
fig.update_layout(
    title='Credits Usage and Cost Over Time',
    xaxis_title='Date',
    yaxis_title='Values',
    barmode='overlay'
)

# Render the chart in Streamlit
st.plotly_chart(fig, use_container_width=True)