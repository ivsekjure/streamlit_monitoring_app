

import streamlit as st
from snowflake.snowpark.context import get_active_session
from utils import is_running_local, get_active_session
from snowflake.snowpark import Session
from snowflake.core import Root # requires snowflake>=0.8.0


if __name__ == "__main__":

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

    # Import python packages



    warehouse_monitoring = st.Page("monitoring/warehouse_monitoring.py", title="Warehouse monitoring", icon=":material/warehouse:")
    user_groups_monitoring = st.Page("monitoring/user_groups_monitoring.py", title="User groups monitoring", icon=":material/group:")
    third_party_apps_monitoring = st.Page("monitoring/third_party_apps_monitoring.py", title="Third party apps monitoring", icon=":material/apps:")
    query_analysis = st.Page("monitoring/query_analysis.py", title="Query analysis", icon=":material/query_stats:")

    pg = st.navigation(
            {
                "Monitoring": [warehouse_monitoring, user_groups_monitoring, third_party_apps_monitoring, query_analysis]
            }
        )
    pg.run()