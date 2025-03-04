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

# User groups analysis
@st.cache_data(ttl=3600)
def user_group_analysis():
    """Fetch data for user groups"""
    query = f'select * from table({database}.{schema}.user_groups_analysis())'
    return get_data(query)


################################################################ STREAMLIT UI ################################################################

st.title("User groups monitoring")


# ================================
# USER GROUPS ANALYSIS
# ================================
user_group = user_group_analysis()
user_group = filter_date_time_frame(user_group, "user_group_analysis", date_from, date_to)

user_group_1 = user_group.groupby(by=['ROLE_NAME', 'YEAR_MONTH_WEEK']).sum().reset_index()
st.subheader("Weekly user groups consumption")
st.bar_chart(user_group_1, x = 'YEAR_MONTH_WEEK', y = 'CREDITS_USED_COMPUTE', color = 'ROLE_NAME')

user_group_2 = user_group.groupby(by=['USER_NAME', 'YEAR_MONTH_WEEK']).sum().reset_index()
st.subheader("Weekly user groups by user")
st.bar_chart(user_group_2, x = 'YEAR_MONTH_WEEK', y = 'CREDITS_USED_COMPUTE', color = 'USER_NAME')

