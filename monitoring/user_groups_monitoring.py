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

# User groups analysis
@st.cache_data(ttl=3600)
def user_group_analysis():
    """Fetch data for user groups"""
    query = f'select * from table({database}.{schema}.user_groups_analysis())'
    return get_data(query)


################################ STREAMLIT UI ################################


st.title("User groups monitoring")


# User groups analysis
df = user_group_analysis()
df = filter_date_time_frame(df, "user_group_analysis", date_from, date_to)

st.subheader("Weekly user groups consumption")
st.bar_chart(df, x = 'YEAR_MONTH_WEEK', y = 'CREDITS_USED_COMPUTE', color = 'ROLE_NAME')


st.subheader("Weekly user groups by user")
st.bar_chart(df, x = 'YEAR_MONTH_WEEK', y = 'CREDITS_USED_COMPUTE', color = 'USER_NAME')

