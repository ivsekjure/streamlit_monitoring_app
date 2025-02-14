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



# Function to Fetch Data from Snowflake
def get_data(query):
    """Executes a SQL query and returns the result as a Pandas DataFrame."""
    return session.sql(query).to_pandas()


# Function to fetch warehouse data
def show_warehouses():
    """Fetch warehouse information."""
    session.sql("SHOW WAREHOUSES").collect()
    query = """select * from dbt_demo.public.warehouses_vw"""
    return get_data(query)


# Function to Fetch Monthly WH Consumption
def wh_monthly_consumption_f():
    """Fetch monthly warehouse consumption."""
    query = """select * from dbt_demo.public.wh_monthly_consumption_vw"""
    return get_data(query)


# Function to fetch average day-by-day consumption
def avg_day_by_day_consumption():
    """Fetch day-by-day warehouse consumption data."""
    query = """select * from dbt_demo.public.avg_day_by_day_consumption"""
    return get_data(query)


# Daily peaks
def daily_peaks():
    """Daily peaks"""
    query = """select * from dbt_demo.public.daily_peaks"""
    return get_data(query)


# Autosuspend costs
def autosuspend_costs():
    """Fetch information about autosuspend costs"""
    query = """select * from dbt_demo.public.autosuspend_costs"""
    return get_data(query)


# 7 day average trend
def seven_day_average_trend():
    """Get 7 days average trend"""
    query = """select * from dbt_demo.public.seven_day_average_trend"""
    return get_data(query)





################################ STREAMLIT UI ################################


st.title("warehouse monitoring")


# Warehouse information
show_wh_data = show_warehouses()
st.subheader("List of Warehouses by size")
st.bar_chart(show_wh_data, x = 'AUTO_SUSPEND', y = 'WH_NAME', color = 'WH_SIZE')



# Monthly warehouse consumption
monthly_consumption = wh_monthly_consumption_f()
st.subheader('Monthly warehouse consumption compute')
st.bar_chart(monthly_consumption, x = 'YEAR_MONTH', y = 'CREDITS_USED', color = 'NAME')
st.subheader('Monthly warehouse serverless compute')
st.bar_chart(monthly_consumption, x = 'YEAR_MONTH', y = 'CREDITS_USED', color = 'SERVICE_TYPE')



# Average day-by-day warehouse consumption
df = avg_day_by_day_consumption()

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
df = df[df["SERVICE_TYPE"] != 'WAREHOUSE_METERING']
# Create a horizontal bar chart using Plotly
fig = go.Figure(go.Bar(
    x=df['CREDITS_USED'],
    y=df['SERVICE_TYPE'],
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



df = wh_monthly_consumption_f()
df = df[df["SERVICE_TYPE"] != 'WAREHOUSE_METERING']

# Create the figure
fig = go.Figure()

# Bar chart by SERVICE_TYPE
for service_group in df['SERVICE_TYPE'].unique():
    service_data = df[df['SERVICE_TYPE'] == service_group]
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