import os
import streamlit as st

def is_running_local():
    return "LOCAL" in os.environ


from typing import Callable, Optional

import snowflake.snowpark


def get_active_session() -> "snowflake.snowpark.Session":
    """Returns the current active Snowpark session.

    Raises: SnowparkSessionException: If there is more than one active session or no active sessions.

    Returns:
        A :class:`Session` object for the current session.
    """
    return snowflake.snowpark.session._get_active_session()


import datetime
import pandas as pd

def filter_date_time_frame(df: pd.DataFrame, function_name: str, t_from: datetime.date, t_to: datetime.date) -> pd.DataFrame:
    date_mappings = {
        "wh_monthly_consumption_f": ("YEAR_MONTH", "%Y-%m"),
        "avg_day_by_day_consumption": ("DATE", "%Y-%m-%d"),
        "autosuspend_costs": ("DATE", "%Y-%m-%d"),
        "seven_day_average_trend": ("START_DATE", "%Y-%m-%d"),
        "user_group_analysis": ("YEAR_MONTH_WEEK", "%Y-%m-%W"),
        "sf_automation_task": ("DATE", "%Y-%m-%d"),
        "query_num_of_executions": ("DATE", "%Y-%m-%d")
    }


    if function_name in date_mappings:
        column, format = date_mappings[function_name]
         
        # if df[column].dtype == object:
        df = df[df[column].astype('str') >= t_from.strftime(format)]
        df = df[df[column].astype('str') <= t_to.strftime(format)]

    return df