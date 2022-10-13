#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from calendar import month
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import logging
import json
from datetime import datetime, timedelta
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams import IncrementalMixin

logger = logging.getLogger("airbyte")

# Basic full refresh stream
class IAuditorStream(HttpStream, ABC):

    url_base = "https://api.safetyculture.io/"
        
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return [response.json()]
# Source
class SourceIAuditor(AbstractSource):

    def get_new_token(self, config):

        api_auth_url = config["api_auth_url"]
        grant_type = config["grant_type"]
        username = config["username"]
        password = config["password"]

        payload = {
            'grant_type': grant_type,
            'username': username,
            'password': password
        }

        token_response = requests.post(api_auth_url,
                                       headers={
                                           "Content-Type": "application/x-www-form-urlencoded"},
                                       data=payload)

        if token_response.status_code != 200:
            logger.info("Failed to obtain token from the OAuth 2.0 server")

        logger.info("Successfuly obtained a new token")
        tokens = json.loads(token_response.text)
        return tokens['access_token']

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        token = self.get_new_token(config)
        api_call_headers = {'Authorization': 'Bearer ' + token}
        api_call_response = requests.get(
            config["api_url_base"], headers=api_call_headers, verify=False)
        logger.info(api_call_response.status_code)
        # logger.info(json.loads(api_call_response.text))
        logger.info("Completed")

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        access_token = self.get_new_token(config)
        start_date = datetime.strptime('2001-01-01', '%Y-%m-%d')
        auth = TokenAuthenticator(token=access_token)
        return [Audits(authenticator=auth, start_date=start_date)]

class Audits(IAuditorStream, IncrementalMixin):

    cursor_field = "modified_at"
    cursor_filter = "modified_after"
    date_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    primary_key = None

    def __init__(self, authenticator: Any, start_date: datetime, **kwargs):
        super().__init__(authenticator)
        self.start_date = start_date
        self._cursor_value = None
        self.modified_after = '2022-01-01T00:00:00.000Z'
        self.archived = 'both'

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "audits/search"

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if self._cursor_value:
            return { 'modified_after': self._cursor_value,
                 'archived': self.archived
                    }
        else:
            return { 'modified_after': self.modified_after,
                 'archived': self.archived
                    }
    
    def read_records(self, *args, **kwargs) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(*args, **kwargs):
            if record.get('audits',False):
                latest_record_date = datetime.strptime(record['audits'][-1][self.cursor_field], self.date_format)
                if self._cursor_value:
                    self._cursor_value = max(self._cursor_value, latest_record_date)
                else:
                    self._cursor_value =latest_record_date
                yield record

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        if decoded_response.get("count", False):
            if decoded_response['count'] == 1000:
                logger.info('counter = 1000')
                latest_record_date = datetime.strptime(decoded_response['audits'][-1][self.cursor_field], self.date_format)
                return {self.cursor_field: latest_record_date}
            else:
                logger.info('counter != 1000')

    @property
    def state(self) -> Mapping[str, Any]:
        if self._cursor_value:
            return {self.cursor_filter: self._cursor_value.strftime(self.date_format)}
        else:
            return {self.cursor_filter: self.start_date.strftime(self.date_format)}
    
    @state.setter
    def state(self, value: Mapping[str, Any]):
        logger.info('value')
        logger.info(value)
        self._cursor_value = datetime.strptime(value[self.cursor_field], self.date_format)

    def _chunk_date_range(self, start_date: datetime) -> List[Mapping[str, Any]]:
        dates = []
        while start_date < datetime.now():
            dates.append({self.cursor_filter: start_date.strftime(self.date_format)})
            start_date += timedelta(days=120)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], self.date_format) if stream_state and self.cursor_filter in stream_state else self.start_date
        return self._chunk_date_range(start_date)