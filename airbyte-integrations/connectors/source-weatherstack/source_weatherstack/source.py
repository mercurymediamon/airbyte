#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#


from abc import ABC, abstractmethod
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
import json
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.models import SyncMode


from .names import URL_BASE
from .helpers import Transform

"""
TODO: Most comments in this class are instructive and should be deleted after the source is implemented.

This file provides a stubbed example of how to use the Airbyte CDK to develop both a source connector which supports full refresh or and an
incremental syncs from an HTTP API.

The various TODOs are both implementation hints and steps - fulfilling all the TODOs should be sufficient to implement one basic and one incremental
stream from a source. This pattern is the same one used by Airbyte internally to implement connectors.

The approach here is not authoritative, and devs are free to use their own judgement.

There are additional required TODOs in the files within the integration_tests folder and the spec.json file.
"""


# Basic full refresh stream
class WeatherstackStream(HttpStream, ABC):
    url_base = URL_BASE

    #Weatherstack has no paging, so we don't need to return anything
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def stream_slices(
            self, *, sync_mode: SyncMode, cursor_field: List[str] = None, stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for city in self.config.get("query", []):
            yield {"city": city}

    #Parameters we need for GET-methods
    def request_params(
            self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    #Define what happens when data is returned
    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {response.json()}

class Forecast(WeatherstackStream, ABC):

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config

    #Path just tells the name of an endpoint
    def path(self, **kwargs) -> str:
        path = f"forecast"
        return path

    @abstractmethod
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None):
        return None

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        #we only want the response text as a json, nothing else
        #return [response.json()] # NEEDS to be an array
        response = response.json()
        restructured_response = Transform.restructure_response(response, "forecast")
        return [restructured_response]

class ForecastDaily(Forecast):

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "access_key": self.config.get("access_key"),
            "query": stream_slice["city"],
            "forecast_days": self.config.get("forecast", {}).get("forecast_days"),
            "hourly": 0
        }

class ForecastHourly(Forecast):

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "access_key": self.config.get("access_key"),
            "query": stream_slice["city"],
            "forecast_days": self.config.get("forecast", {}).get("forecast_days"),
            "hourly": 1,
            "interval": self.config.get("forecast", {}).get("hourly_interval")
        }

class Historical(WeatherstackStream, ABC):

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config

    #Path just tells the name of an endpoint
    def path(self, **kwargs) -> str:
        path = f"historical"
        return path

    @abstractmethod
    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None):
        return None

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        #we only want the response text as a json, nothing else
        #return [response.json()] # NEEDS to be an array
        response = response.json()
        restructured_response = Transform.restructure_response(response, "historical")
        return [restructured_response]

class HistoricalDaily(Historical):

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if self.config.get("historical", {}).get("date") is not None:
            return {
                "access_key": self.config.get("access_key"),
                "query": stream_slice["city"],
                "historical_date": self.config.get("historical", {}).get("date"),
                "hourly": 0
            }
        elif self.config.get("historical", {}).get("date_start") is not None and self.config.get("historical", {}).get("date_end") is not None:
            return {
                "access_key": self.config.get("access_key"),
                "query": stream_slice["city"],
                "historical_date_start": self.config.get("historical", {}).get("date_start"),
                "historical_date_end": self.config.get("historical", {}).get("date_end"),
                "hourly": 0
            }
        else:
        #Never reached because of prior config-error
            raise ValueError('Neither a "date", nor a "date_start" and "date_end" field have been entered')

class HistoricalHourly(Historical):

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        if self.config.get("historical", {}).get("date") is not None:
            return {
                "access_key": self.config.get("access_key"),
                "query": self.config.get("query")[0],
                "historical_date": self.config.get("historical", {}).get("date"),
                "hourly": 1
            }
        else:
            return {
                "access_key": self.config.get("access_key"),
                "query": stream_slice["city"],
                "historical_date_start": self.config.get("historical", {}).get("date_start"),
                "historical_date_end": self.config.get("historical", {}).get("date_end"),
                "hourly": 1
            }

class Current(WeatherstackStream):

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config

    #Path just tells the name of an endpoint
    def path(self, **kwargs) -> str:
        path = f"current"
        return path

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "access_key": self.config.get("access_key"),
            "query": stream_slice["city"]
        }

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        #we only want the response text as a json, nothing else
        #return [response.json()] # NEEDS to be an array
        response = response.json()
        restructured_response = Transform.restructure_response(response, "current")
        return [restructured_response]

class LocationLookup(WeatherstackStream):

    primary_key = None

    def __init__(self, config: Mapping[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.config = config

    #Path just tells the name of an endpoint
    def path(self, **kwargs) -> str:
        path = f"autocomplete"
        return path

    def request_params(
            self,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, any] = None,
            next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {
            "access_key": self.config.get("location_kooup", {}).get("access_key"),
            "query": self.config.get("current", {}).get("query")
        }

    def parse_response(
            self,
            response: requests.Response,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        #we only want the response text as a json, nothing else
        #return [response.json()] # NEEDS to be an array
        response = response.json()
        restructured_response = Transform.restructure_response(response, "location_lookup")
        return [restructured_response]

# Basic incremental stream
class IncrementalWeatherstackStream(WeatherstackStream, ABC):
    """
    TODO fill in details of this class to implement functionality related to incremental syncs for your connector.
         if you do not need to implement incremental sync for any streams, remove this class.
    """

    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        """
        TODO
        Override to return the cursor field used by this stream e.g: an API entity might always use created_at as the cursor field. This is
        usually id or date based. This field's presence tells the framework this in an incremental stream. Required for incremental.

        :return str: The name of the cursor field.
        """
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}


    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, any]]]:
        """
        TODO: Optionally override this method to define this stream's slices. If slicing is not needed, delete this method.

        Slices control when state is saved. Specifically, state is saved after a slice has been fully read.
        This is useful if the API offers reads by groups or filters, and can be paired with the state object to make reads efficient. See the "concepts"
        section of the docs for more information.

        The function is called before reading any records in a stream. It returns an Iterable of dicts, each containing the
        necessary data to craft a request for a slice. The stream state is usually referenced to determine what slices need to be created.
        This means that data in a slice is usually closely related to a stream's cursor_field and stream_state.

        An HTTP request is made for each returned slice. The same slice can be accessed in the path, request_params and request_header functions to help
        craft that specific request.

        For example, if https://example-api.com/v1/employees offers a date query params that returns data for that particular day, one way to implement
        this would be to consult the stream state object for the last synced date, then return a slice containing each date from the last synced date
        till now. The request_params function would then grab the date from the stream_slice and make it part of the request by injecting it into
        the date query param.
        """
        raise NotImplementedError("Implement stream slices or delete this method!")


# Source
class SourceWeatherstack(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        """
        TODO: Implement a connection check to validate that the user-provided config can be used to connect to the underlying API

        See https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-stripe/source_stripe/source.py#L232
         for an example.

        :param config:  the user-input config object conforming to the connector's spec.json
        :param logger:  logger object
        :return Tuple[bool, any]: (True, None) if the input config can be used to connect to the API successfully, (False, error) otherwise.
        """

        url_base = URL_BASE
        #test, ob valider access_key (teste mit aktuellem Wetter)
        # wenn es access_key validator geben würde, so nicht nötgi
        endpoint = "current"
        url = url_base + endpoint
        access_key = config.get("access_key")
        query = "Berlin"
        payload = {'access_key': access_key, 'query': query}

        r = requests.get(url, params=payload)
        j = r.json()
        #gibt es einen Key request im Objekt? wenn ja, hat es funktioniert (Domänenwissen)
        #TODO: Falsche URL_BASE? Internetausfall? Beides catchen!
        #raise_for_error-Methode, die man aus r-Objekt ausführen kann. Bspw. 404 ist drin, aber kein JSON.
        #r.status_code != 200 eigentlich immer fehlerhaft
        if j.get("request"):
            return True, None
        else:
            return False, j.get("error").get("info")

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [
            ForecastDaily(config = config),
            ForecastHourly(config = config),
            HistoricalDaily(config = config),
            HistoricalHourly(config = config),
            Current(config = config)
        ]
