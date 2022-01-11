"""
Purpose:
    Start DC Fire and EMS Data Visualizer
"""

# Python imports
import datetime
from typing import Type, Union, Dict, Any, List

# 3rd party imports
import pandas as pd
import altair as alt
import streamlit as st

# My imports
import connect_to_mongo


def get_mongo_collection():
    """
    Purpose:
        Connects to mongodb
    Args:
        N/A
    Returns:
        mongodb collection
    """
    # Fetch data from URL here, and then clean it up.
    db = connect_to_mongo.mongo_connect()
    db_col = connect_to_mongo.create_timeseries_collection(db)

    return db_col


@st.cache
def get_mongo_day_data(curr_date):
    """
    Purpose:
        Gets daily data
    Args:
        curr_date - date to get
    Returns:
        Mongo daily data
    """
    db_col = get_mongo_collection()
    mongo_data = connect_to_mongo.date_query(db_col, curr_date)
    return mongo_data


@st.cache
def get_monthly_data(month, year):
    """
    Purpose:
        Gets Monthly data
    Args:
        month - month
        year - year
    Returns:
        Mongo Monthly data
    """
    db_col = get_mongo_collection()
    mongo_data = connect_to_mongo.adv_query(db_col, "month")

    for item in mongo_data:
        if (
            item["_id"]["firstDayOfMonth"].month == month
            and item["_id"]["firstDayOfMonth"].year == year
        ):
            return item
    # Invalid Month...
    return None


# TODO do we need this data?
@st.cache
def get_yearly_data(year):
    """
    Purpose:
        Gets Yearly data
    Args:
        year - year
    Returns:
        Mongo Yearly data
    """
    db_col = get_mongo_collection()
    mongo_data = connect_to_mongo.adv_query(db_col, "year")
    for item in mongo_data:
        if item["_id"]["firstDayOfMonth"].year == year:
            return item
    # Invalid year
    return None


def make_pie_chart(crit: str, non_crit: str, fire: str, total_num):
    """
    Purpose:
        Ceates Pie chart from daily data
    Args:
        total - total num
        crit - num critical
        non_crit - num non_critical
        fire - num fire
    Returns:
        N/A
    """

    source = pd.DataFrame(
        {
            "category": [
                f"critical  ({round(crit/total_num,2)}%) ",
                f"non_critical ({round(non_crit/total_num,2)}%)",
                f"fire ({round(fire/total_num,2)}%)",
            ],
            "value": [crit, non_crit, fire],
        }
    )

    base = alt.Chart(source).encode(
        theta=alt.Theta("value:Q", stack=True),
        color=alt.Color("category:N", legend=None),
    )

    pie = base.mark_arc(outerRadius=60)
    text = base.mark_text(radius=130, size=14).encode(text="category:N")

    st.altair_chart(pie + text, use_container_width=True)


def sidebar() -> None:
    """
    Purpose:
        Shows the side bar
    Args:
        N/A
    Returns:
        N/A
    """

    st.sidebar.image(
        "https://pbs.twimg.com/media/FIr-e2_WYAAtv9u?format=jpg&name=small",
        use_column_width=True,
    )

    # Create the Navigation Section
    st.sidebar.markdown(
        "This application uses MongoDB time series collections to highlight the DC Fire and EMS call volume between August 2014 through August 2015"
    )

    # mode = st.selectbox("View mode", ["Daily", "Monthly"])

    # return mode


def app() -> None:
    """
    Purpose:
        Controls the app flow
    Args:
        N/A
    Returns:
        N/A
    """

    # Spin up the sidebar
    sidebar()

    # User selects date
    curr_date = st.date_input(
        "Select Day",
        datetime.date(2014, 8, 1),
        min_value=datetime.date(2014, 8, 1),
        max_value=datetime.date(2015, 8, 31),
    )

    mongo_data = get_mongo_day_data(curr_date)
    # Mongo data
    num_calls = mongo_data["total_calls"]
    num_crit = mongo_data["critical"]
    num_non_crit = mongo_data["non_critical"]
    num_fire = mongo_data["fire"]

    date_string = curr_date.strftime("%A, %B %d, %Y")

    # Get mongo_results
    user_text = f"** #DCsBravest responded to {num_calls} calls on {date_string}. There were {num_crit} critical and {num_non_crit} non-critical EMS dispatches, and {num_fire} fire related incidents and other types of emergencies. **"

    st.markdown(user_text)

    make_pie_chart(num_crit, num_non_crit, num_fire, num_calls)
    monthly_data = get_monthly_data(curr_date.month, curr_date.year)

    col1, col2, col3, col4 = st.columns(4)

    # Montly data
    avgTotalCalls = round(monthly_data["avgTotalCalls"], 2)
    avgCriticalCalls = round(monthly_data["avgCriticalCalls"], 2)
    avgNonCriticalCalls = round(monthly_data["avgNonCriticalCalls"], 2)
    avgFireCalls = round(monthly_data["avgFireCalls"], 2)

    with col1:
        st.metric(
            "Monthly Average Total Calls",
            avgTotalCalls,
            round((num_calls - avgTotalCalls), 2),
        )

    with col2:
        st.metric(
            "Monthly Average Critical Calls",
            avgCriticalCalls,
            round((num_crit - avgCriticalCalls), 2),
        )
    with col3:
        st.metric(
            "Monthly Average Non Critical Calls",
            avgNonCriticalCalls,
            round((num_non_crit - avgNonCriticalCalls), 2),
        )
    with col4:
        st.metric(
            "Monthly Average Fire Calls",
            avgFireCalls,
            round((num_fire - avgFireCalls), 2),
        )


def main() -> None:
    """
    Purpose:
        Controls the flow of the streamlit app
    Args:
        N/A
    Returns:
        N/A
    """

    # Start the streamlit app
    st.title("DC Fire and EMS Data Visualizer")
    app()


if __name__ == "__main__":
    main()
