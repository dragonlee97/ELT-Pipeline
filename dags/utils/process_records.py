# -*- coding: utf-8 -*-
import csv
import json
import logging
from datetime import datetime
from io import StringIO

LOGGER = logging.getLogger(__name__)


class RecordProcessor:
    with open("/elt-example-dag/dags/table_schemas/events.json", "r") as file:
        events_schema = json.load(file)
    COLUMNS = [field["name"] for field in events_schema]

    def __init__(self):
        self._buffer = StringIO()
        self._writer = csv.DictWriter(
            self._buffer, fieldnames=self.COLUMNS, quotechar='"', delimiter=";", extrasaction="ignore"
        )
        self._writer.writeheader()

    @staticmethod
    def _parse_data(record: dict):
        """
        parse and extract readable data in the record
        :param record: An event record in dict format
        :return: cleaned event record in dict format
        """
        if "METADATA" in record:
            record["USER_AGENT"] = json.loads(record["METADATA"]).get("browser_user_agent")
            record["PAGE_TITLE"] = json.loads(record["METADATA"]).get("page_title")
        if "EVENT_PROPERTIES" in record:
            record["PROPERTY_LABEL"] = json.loads(record["EVENT_PROPERTIES"]).get("label")
        return record

    def _process(self, record: dict):
        """
        parse the record and write it into a csv row in buffer
        :param record: An event record in dict format
        """
        processed_record = self._parse_data(record)
        self._writer.writerow(processed_record)

    def transform_to_csv(self, table, execution_date: str):
        """
        iterate over Airtable object, process the event of execution date
        :param table: an Airtable object
        :param execution_date: execution date in string format
        :return: csv string file written in buffer
        """
        LOGGER.info(f"Started process the data of {execution_date}")
        execution_date = datetime.strptime(execution_date, "%Y-%m-%d").date()
        counter = 0
        for records in table.iterate():
            event = records[0]["fields"]
            if datetime.strptime(event["CREATED_AT"], "%Y-%m-%dT%H:%M:%S.%f%z").date() == execution_date:
                counter += 1
                self._process(event)
        LOGGER.info(f"write into csv file of {counter} rows")
        return self._buffer.getvalue()
