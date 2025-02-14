# Import python packages
import streamlit as st

warehouse_monitoring = st.Page("warehouse_monitoring.py", title="Warehouse monitoring", icon=":material/warehouse:")
user_groups_monitoring = st.Page("user_groups_monitoring.py", title="User groups monitoring", icon=":material/group:")
third_party_apps_monitoring = st.Page("third_party_apps_monitoring.py", title="Third party apps monitoring", icon=":material/apps:")
query_analysis = st.Page("query_analysis.py", title="Query analysis", icon=":material/query_stats:")

pg = st.navigation([warehouse_monitoring, user_groups_monitoring, third_party_apps_monitoring, query_analysis])
pg.run()