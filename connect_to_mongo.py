# Python imports
import datetime
import logging
import os

# 3rd party imports
import pandas as pd
import pymongo

# Mongo envs
MONGO_USER = os.environ["MONGODB_USER"]
MONGO_CLUSTER = os.environ["MONGODB_CLUSTER"]
MONGO_PASSWORD = os.environ["MONGODB_PASSWORD"]
DB_NAME = "dcfireems_db"


def mongo_connect():
    """
    Purpose:
        Connect to mongodb
    Args:
        N/A
    Returns:
        Mongodb
    """
    client = pymongo.MongoClient(
        f"mongodb+srv://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_CLUSTER}/{DB_NAME}?retryWrites=true&w=majority"
    )
    db = client.dcfireems_db
    logging.info(db)

    return db


def create_timeseries_collection(db):
    """
    Purpose:
        Creates a timeseries collection
    Args:
        db - Mongodb object
    Returns:
        Mongodb collection
    """

    try:
        dcfireems_col = db.create_collection(
            "dcfireems", timeseries={"timeField": "timestamp"}
        )
    except Exception as error:
        # collection exists already
        dcfireems_col = db.dcfireems_col
    logging.info(dcfireems_col)

    return dcfireems_col


def insert_data_into_col(db_col, df):
    """
    Purpose:
        Inserts data into db collection
    Args:
        db_col - Mongodb collection
        df - dataframe
    Returns:
        Mongodb collection
    """
    # Convert df to json records

    records = df.to_dict("records")
    # df_records = None
    for record in records:

        timestamp = record["timestamp"]
        # Break out parts
        timestamp_parts = timestamp.split("/")
        month = int(timestamp_parts[0])
        day = int(timestamp_parts[1])
        year = int(timestamp_parts[2])
        # datetime.datetime(year, month, day,)
        # Convert to timestamp
        record["timestamp"] = datetime.datetime(year, month, day)

    db_col.insert_many(records)


def date_query(db_col, date):
    """
    Purpose:
        Does a date query
    Args:
        db_col - Mongodb collection
        date - date to use
    Returns:
        query results
    """

    # Do a find one search
    # search_val = db_col.find_one({"timestamp": date})

    search_val = db_col.find_one(
        {"timestamp": datetime.datetime(date.year, date.month, date.day)}
    )

    logging.info(search_val)

    return search_val


def adv_query(db_col, agg):
    """
    Purpose:
        Does an advanced query
    Args:
        db_col - Mongodb collection
        agg - how to aggerate search
    Returns:
        query results
    """

    # Query Avg
    agg_result = db_col.aggregate(
        [
            {
                "$group": {
                    "_id": {
                        "firstDayOfMonth": {
                            "$dateTrunc": {"date": "$timestamp", "unit": agg},
                        },
                    },
                    "avgTotalCalls": {"$avg": "$total_calls"},
                    "avgCriticalCalls": {"$avg": "$critical"},
                    "avgNonCriticalCalls": {"$avg": "$non_critical"},
                    "avgFireCalls": {"$avg": "$fire"},
                }
            },
            {"$sort": {"_id": 1}},
        ]
    )

    # logging.info(list(agg_result))

    return list(agg_result)


def main():
    """
    Purpose:
        Run main function
    Args:
        N/A
    Returns:
        N/A
    """
    logging.info("Testing mongodb connection")
    # Connect to Mongo DB
    db = mongo_connect()

    # Drop collection
    db.dcfireems_col.drop()

    # Create timeseries_collection
    db_col = create_timeseries_collection(db)
    # Load Data
    df = pd.read_csv("data/2014_2015_dcfireems_data.csv")
    # Insert data
    insert_data_into_col(db_col, df)

    # Do a find one search
    search_val = db_col.find_one({"timestamp": datetime.datetime(2014, 8, 1)})
    logging.info(search_val)

    agg_result = adv_query(db_col, "month")

    logging.info("done and done")


if __name__ == "__main__":
    loglevel = logging.INFO
    logging.basicConfig(format="%(levelname)s: %(message)s", level=loglevel)
    main()
