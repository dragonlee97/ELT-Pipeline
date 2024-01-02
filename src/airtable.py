# -*- coding: utf-8 -*-
import logging
import os

from pyairtable import Table

LOGGER = logging.getLogger(__name__)


class Airtable:
    def __init__(self):
        self._api_key = os.environ["AIRTABLE_API_KEY"]
        self._base_id = os.environ["AIRTABLE_BASE_ID"]

    def load_from_airtable(self, table_name):
        """
        load table via Airtable API
        :param table_name: the table name to be loaded
        :return: An Airtable object
        """
        table = Table(self._api_key, self._base_id, table_name)
        LOGGER.info(f"Successfully loaded airtable: {table_name}")
        return table
