# Import python packages
import streamlit as st
import datetime
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
import plotly.express as px
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
        # run locally
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
        # run on snowflake 
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

# Daily peaks
@st.cache_data(ttl=3600)
def daily_peaks1():
    """Daily peaks"""
    query = f'select * from  table({database}.{schema}.daily_peaks1())'
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





################################################################ STREAMLIT UI ################################################################

st.title("Warehouse monitoring")


# ================================
# WAREHOUSE INFORMATION
# ================================
warehouses = show_warehouses()

warehouses = warehouses.sort_values(by='AUTO_SUSPEND',ascending=False)

st.subheader("List of Warehouses by size")
st.bar_chart(warehouses, x = 'WH_NAME', y = 'AUTO_SUSPEND', color = 'WH_SIZE', horizontal=True)



# ================================
# MONTHLY WAREHOUSE CONSUMPTION
# ================================
monthly_consumption = wh_monthly_consumption_f()
monthly_consumption = filter_date_time_frame(monthly_consumption, "wh_monthly_consumption_f", date_from, date_to)

# monthly_consumption.CREDITS_USED = monthly_consumption.CREDITS_USED.astype(int)
st.subheader('Monthly warehouse consumption compute')
st.bar_chart(monthly_consumption, y = 'CREDITS_USED', x = 'YEAR_MONTH',  color = 'NAME')
st.subheader('Monthly warehouse serverless compute')
st.bar_chart(monthly_consumption, y = 'CREDITS_USED', x = 'YEAR_MONTH',  color = 'SERVICE_TYPE' )



# ================================
# AVERAGE DAY-BY-DAY WAREHOUSE CONSUMPTION
# ================================
avg_day_by_day = avg_day_by_day_consumption()
avg_day_by_day = filter_date_time_frame(avg_day_by_day, "avg_day_by_day_consumption", date_from, date_to)

st.subheader('Average Day-by-Day Consumption')

# Generate a color palette
color_palette = px.colors.qualitative.Plotly

unique_warehouses = avg_day_by_day['WAREHOUSE_NAME'].unique()

# Assign colors dynamically to each warehouse
warehouse_colors = {warehouse: color_palette[i % len(color_palette)] for i, warehouse in enumerate(unique_warehouses)}

# Create a bar chart for "AVERAGE" with different colors for each warehouse
bar_charts = []
for warehouse in unique_warehouses:
    warehouse_data = avg_day_by_day[avg_day_by_day['WAREHOUSE_NAME'] == warehouse]
    bar_chart = go.Bar(
        x=warehouse_data['DATE'], 
        y=warehouse_data['CREDITS_USED_COMPUTE'], 
        name=f'CREDITS_USED_COMPUTE - Warehouse {warehouse}',
        marker_color=warehouse_colors[warehouse]
    )
    bar_charts.append(bar_chart)

# Create a line chart for "DAILY"
line_chart1 = go.Scatter(x=avg_day_by_day['DATE'], y=avg_day_by_day['AVERAGE'], mode='lines+markers', name='AVERAGE of credits used')

# Create a line chart for "STD"
line_chart2 = go.Scatter(x=avg_day_by_day['DATE'], y=avg_day_by_day['STDV'], mode='lines+markers', name='STDV of credits used')

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



# ================================
# OVERALL CONSUMPTION BY SF COMPONENTS
# ================================
wh_monthly = wh_monthly_consumption_f()
wh_monthly = filter_date_time_frame(wh_monthly, "wh_monthly_consumption_f", date_from, date_to)

df1 = wh_monthly[wh_monthly["SERVICE_TYPE"] != 'WAREHOUSE_METERING']

df1 = df1.groupby(['SERVICE_TYPE'])['CREDITS_USED'].sum().reset_index()
df1.sort_values(by=['CREDITS_USED', 'SERVICE_TYPE'], ascending=[False,True])

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
    # yaxis={'CREDITS_USED':'total descending'},
    barmode='group'
)

# Display the chart in Streamlit
st.plotly_chart(fig)



# ================================
# CREDITS USED BREAKDOWN BY WAREHOUSE
# ================================
df2 = wh_monthly.groupby(['NAME','SERVICE_TYPE'])['CREDITS_USED'].sum().reset_index()


unique_services = df2["SERVICE_TYPE"].unique()
color_map = {cat: px.colors.qualitative.Plotly[i % len(px.colors.qualitative.Plotly)] for i, cat in enumerate(unique_services)}



# Create a horizontal bar chart using Plotly
fig = go.Figure(go.Bar(
    x=df2['CREDITS_USED'],
    y=df2['NAME'],
    marker=dict(color=[color_map[cat] for cat in wh_monthly['SERVICE_TYPE']]),
    orientation='h'
))

# Update layout
fig.update_layout(
    title='Credits used breakdown by warehouse',
    xaxis_title='Credits Used',
    yaxis_title='Warehouse Name',
    barmode='stack', 
    yaxis={'categoryorder': 'total ascending'},
    legend_title='Service Type'
)

# Display the chart in Streamlit
st.plotly_chart(fig, use_container_width=True)



# ================================
# DAILY PEAKS
# ================================
st.subheader('Daily peeks')

daily_p = daily_peaks1()

wh_names = set(daily_p["WAREHOUSE_NAME"])

# warehouse_name = st.selectbox('warehouse_name', wh_names, key=10)
year = st.selectbox('Year', range(2025, 1990, -1), key=11)
month = st.selectbox('Month', range(1, 13), key=12)

# df1 = df1[df1["WAREHOUSE_NAME"] == warehouse_name]
daily_p = daily_p[daily_p["YEAR"] == year]
daily_p = daily_p[daily_p["MONTH"] == month]

st.subheader("Daily credit consumption in selected month")

fig = px.bar(daily_p, x='DAY', y='CREDITS_USED', color='WAREHOUSE_NAME')
fig.update_layout(
    xaxis={"dtick":1}
)
st.plotly_chart(fig)



# df2 = df.fillna(0)
# df2 = df2.drop(df.columns[6:15], axis=1)
# df2 = df2.iloc[:, :17]
# wh_names = set(df2["WAREHOUSE_NAME"])

# warehouse_name = st.selectbox('warehouse_name', wh_names, key=20)
# year = st.selectbox('Year', range(1990, 2026), key=21)
# month = st.selectbox('Month', range(1, 13), key=22)

# df2 = df2[df2["WAREHOUSE_NAME"] == warehouse_name]
# df2 = df2[df2["YEAR"] >= year]
# df2 = df2[df2["MONTH"] == month]

# st.subheader("Daily credit consumption 11th till 20th")
# st.line_chart(df2, x = 'Day', y = ['11','12','13','14','15','16','17','18','19','20'])



# df3 = df.fillna(0)
# df3 = df3.drop(df3.columns[6:26], axis=1)
# df3.iloc[:, :17]
# wh_names = set(df3["WAREHOUSE_NAME"])
# print(wh_names)

# warehouse_name = st.selectbox('warehouse_name', wh_names, key=30)
# year = st.selectbox('Year', range(1990, 2026), key=31)
# month = st.selectbox('Month', range(1, 13), key=32)

# df3 = df3[df3["WAREHOUSE_NAME"] == warehouse_name]
# df3 = df3[df3["YEAR"] >= year]
# df3 = df3[df3["MONTH"] == month]

# st.subheader("Daily credit consumption 21th till 31th")
# st.line_chart(df3, x = 'Day', y = ['21','22','23','24','25','26','27','28','29','30','31'])



# ================================
# AUTOSUSPEND COSTS
# ================================
st.subheader('Autosuspend costs')

autosuspend_c = autosuspend_costs()
autosuspend_c = filter_date_time_frame(autosuspend_c, "autosuspend_costs", date_from, date_to)

fig = go.Figure()
# fig.add_annotation(text='Excluding today\'s date',xref="paper", yref="paper", x=0, y=1.1, showarrow=False)

# Add the area chart for CREDITS_USED_TOTAL
fig.add_trace(go.Bar(
    x=autosuspend_c['DATE'],
    y=autosuspend_c['CREDITS_USED_TOTAL'],
    # mode='lines',
    name='CREDITS_USED_TOTAL'
    # fill='tozeroy'
))

# Add the area chart for CREDIT_USED_FOR_RESUME
fig.add_trace(go.Bar(
    x=autosuspend_c['DATE'],
    y=autosuspend_c['CREDIT_USED_FOR_RESUME'],
    # mode='lines',
    name='CREDIT_USED_FOR_RESUME'
    # fill='tonexty'
))

# Add the bar chart for PCN_AUTORESUME_COST
fig.add_trace(go.Bar(
    x=autosuspend_c['DATE'],
    y=autosuspend_c['PCN_AUTORESUME_COST'],
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



# ================================
# 7 DAY AVERAGE TREND
# ================================
st.subheader('7 day average trend')

seven_day_avg = seven_day_average_trend()
seven_day_avg = filter_date_time_frame(seven_day_avg, "seven_day_average_trend", date_from, date_to)

warehouse_name = st.selectbox("Pick a desired warehouse: ",set(seven_day_avg["WAREHOUSE_NAME"]))

#warehouse_name = 'COMPUTE_WH' #CHANGE THE NAME OF DESIRED WAREHOUSE 
seven_day_avg = seven_day_avg[seven_day_avg["WAREHOUSE_NAME"] == warehouse_name]

# Create the figure
fig = go.Figure()

# Add the area chart for CREDITS_USED_DATE_WH
fig.add_trace(go.Scatter(
    x=seven_day_avg['START_DATE'],
    y=seven_day_avg['CREDITS_USED_DATE_WH'],
    mode='lines',
    name='CREDITS_USED_DATE_WH',
    fill='tozeroy'
))

# Add the area chart for CREDIT_USED_FOR_RESUME
fig.add_trace(go.Scatter(
    x=seven_day_avg['START_DATE'],
    y=seven_day_avg['CREDITS_USED_7_DAY_AVG'],
    mode='lines',
    name='CREDITS_USED_7_DAY_AVG',
#   fill='tonexty'
     line=dict(color='red')
)
             )

# Update layout
fig.update_layout(
    # title='Credits Usage and Cost Over Time',
    xaxis_title='Date',
    yaxis_title='Values',
    barmode='overlay',
    legend=dict(
        x=0.01,  
        y=1.3,  
    )
)

# Render the chart in Streamlit
st.plotly_chart(fig, use_container_width=True)