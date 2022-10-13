#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from abc import ABC
from datetime import datetime
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

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, 
        stream_state: Mapping[str, Any], 
        stream_slice: Mapping[str, any] = None, 
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

        
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return [response.json()]


# class Customers(IAuditorStream):
#     """
#     TODO: Change class name to match the table/data source this stream corresponds to.
#     """

#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "customer_id"

#     def path(
#         self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
#     ) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/customers then this
#         should return "customers". Required.
#         """
#         return "customers"


# Basic incremental stream
# class IncrementalIAuditorStream(IAuditorStream, ABC):
#     """
#     TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
#          if you do not need to implement incremental sync for any streams, remove this class.
#     """

#     # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
#     state_checkpoint_interval = None

#     @property
#     def cursor_field(self) -> str:
#         """
#         TODO
#         Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
#         usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

#         :return str: The name of the cursor field.
#         """
#         return []

#     def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
#         """
#         Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
#         the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
#         """
#         return {}


# class Employees(IncrementalIAuditorStream):
#     """
#     TODO: Change class name to match the table/data source this stream corresponds to.
#     """

#     # TODO: Fill in the cursor_field. Required.
#     cursor_field = "start_date"

#     # TODO: Fill in the primary key. Required. This is usually a unique field in the stream, like an ID or a timestamp.
#     primary_key = "employee_id"

#     def path(self, **kwargs) -> str:
#         """
#         TODO: Override this method to define the path this stream corresponds to. E.g. if the url is https://example-api.com/v1/employees then this should
#         return "single". Required.
#         """
#         return "employees"

#     def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
#         """
#         TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

#         Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
#         This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
#         section of the docs for more information.

#         The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
#         necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
#         This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

#         An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
#         craft that specific request.

#         For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
#         this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
#         till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
#         the date query param.
#         """
#         raise NotImplementedError(
#             "Implement stream slices or delete this method!")


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
        self.modified_after = '2001-01-01T00:00:00.000Z'
        self.modified_before = '2023-04-04'
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
            if bool(record):
                logger.info(record['audits'][-1])
            else:
                break

            latest_record_date = datetime.strptime(record['audits'][-1][self.cursor_field], self.date_format)
            if self._cursor_value:
                self._cursor_value = max(self._cursor_value, latest_record_date)
            else:
                self._cursor_value =latest_record_date
            yield record
            

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
            start_date += timedelta(days=30)
        return dates

    def stream_slices(self, sync_mode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Optional[Mapping[str, Any]]]:
        start_date = datetime.strptime(stream_state[self.cursor_field], self.date_format) if stream_state and self.cursor_filter in stream_state else self.start_date
        return self._chunk_date_range(start_date)