# -*- coding: utf-8 -*-
import logging
import os

import redshift_connector

LOGGER = logging.getLogger(__name__)


class Redshift:
    def __init__(self):
        self._conn = None

    def connect_to_db(self):
        """
        connect to redshift database with creditials
        """
        self._conn = redshift_connector.connect(
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PWD"),
            host=os.getenv("REDSHIFT_HOST"),
            port=int(os.getenv("REDSHIFT_PORT")),
            database=os.getenv("REDSHIFT_DB"),
        )
        LOGGER.info("Connected to redshift database")

    def close_connection(self):
        """
        commit and close redshift connection
        """
        self._conn.commit()
        self._conn.close()
        LOGGER.info("redshift database connection closed")

    def drop_tables(self):
        """
        drop two tables before create new ones
        :return:
        """
        self._conn.cursor().execute(
            f"DROP TABLE IF EXISTS {os.getenv('REDSHIFT_SCHEMA')}.event, {os.getenv('REDSHIFT_SCHEMA')}.event_sequence;"
        )
        LOGGER.info("tables dropped")

    def create_tables(self):
        """
        create two tables: event, event_sequence if they do not exist
        """
        self._conn.cursor().execute(
            (
                "CREATE TABLE IF NOT EXISTS "
                f"{os.getenv('REDSHIFT_SCHEMA')}.event (ID varchar, CREATED_AT timestamp, DEVICE_ID varchar, "
                "IP_ADDRESS varchar, USER_ID varchar, UUID varchar, EVENT_TYPE varchar, PROPERTY_LABEL varchar,"
                "PLATFORM varchar, DEVICE_TYPE varchar, USER_EMAIL varchar, USER_AGENT varchar, PAGE_TITLE varchar);"
            )
        )
        self._conn.cursor().execute(
            (
                "CREATE TABLE IF NOT EXISTS "
                f"{os.getenv('REDSHIFT_SCHEMA')}.event_sequence (EVENT_ID varchar, PREVIOUS_EVENT_ID varchar, "
                "NEXT_EVENT_ID varchar, USER_ID varchar);"
            )
        )
        LOGGER.info("checked or created tables")

    def copy_into_event_table(self, file_keys):
        """
        copy from csv files stored in s3 into event table
        :param table_names: table names used to name csv file on s3
        :param execution_date: execution_date in string format
        """
        cursor = self._conn.cursor()
        for file_key in file_keys:
            cursor.execute(
                (
                    f"copy {os.getenv('REDSHIFT_SCHEMA')}.event from 's3://{os.getenv('BUCKET_NAME')}/{file_key}' "
                    f"credentials 'aws_access_key_id={os.getenv('AWS_ACCESS_KEY_ID')};"
                    f"aws_secret_access_key={os.getenv('AWS_SECRET_ACCESS_KEY')}' "
                    f"region '{os.getenv('AWS_REGION')}' "
                    f"timeformat as 'auto'"
                    f"delimiter  ';' "
                    f"removequotes "
                    f"ignoreheader 1 ;"
                )
            )
            LOGGER.info(f"{file_key} copied into event table")

    def update_sequence_table(self):
        """
        first delete from, and then update event_sequence table by querying from event table
        """
        cursor = self._conn.cursor()
        cursor.execute(f"DELETE FROM {os.getenv('REDSHIFT_SCHEMA')}.event_sequence")
        cursor.execute(
            (
                f"INSERT INTO {os.getenv('REDSHIFT_SCHEMA')}.event_sequence "
                f"WITH EVENT_USER AS (SELECT "
                f"ID, USER_ID, CREATED_AT, RANK() OVER(PARTITION BY USER_ID ORDER BY CREATED_AT) "
                f"FROM {os.getenv('REDSHIFT_SCHEMA')}.event "
                f"WHERE USER_ID IS NOT NULL) "
                f"SELECT e1.ID as EVENT_ID, e2.ID as PREVIOUS_EVENT_ID, e3.ID as NEXT_EVENT_ID, e1.USER_ID "
                f"FROM EVENT_USER e1 LEFT JOIN EVENT_USER e2 ON e1.USER_ID = e2.USER_ID AND e2.rank+1 = e1.rank "
                f"LEFT JOIN EVENT_USER e3 ON e1.USER_ID = e3.USER_ID AND e3.rank-1 = e1.rank;"
            )
        )

        LOGGER.info("event sequence table updated")

    def execute_query(self, query):
        """
        execute customized query for data check
        :param query:
        :return:
        """
        cursor = self._conn.cursor()
        cursor.execute(query)
        return cursor.fetch_dataframe()

    def trouble_shooting(self):
        """
        query stl_load_errors table
        :return: dataframe
        """
        cursor = self._conn.cursor()
        cursor.execute(
            """
            select query, substring(filename,22,25) as filename,line_number as line, 
            substring(colname,0,15) as column, type, position as pos, substring(raw_line,0,30) as line_text,
            substring(raw_field_value,0,25) as field_text, 
            substring(err_reason,0,100) as error_reason
            from stl_load_errors 
            order by query desc, filename ;
            """
        )
        return cursor.fetch_dataframe()
