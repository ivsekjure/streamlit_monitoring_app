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