import gzip
import json
import ssl
import urllib.request
from logging import Logger
from typing import Dict, List, Optional

import requests
from pydantic import PrivateAttr
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    wait_exponential_jitter,
)

from dagster import ConfigurableResource, get_dagster_logger


class CanvasData2ApiResource(ConfigurableResource):
    """Class for interacting with the Canvas Data 2 API"""

    client_id: str
    client_secret: str

    _log: Logger = PrivateAttr()
    _base_url: str = PrivateAttr()
    _api_token: str = PrivateAttr()

    def setup_for_execution(self, context) -> None:
        self._log = get_dagster_logger()
        self._base_url = "https://api-gateway.instructure.com"
        # get API access token for client
        self._log.info(f"Retrieving API auth token for client id {self.client_id}")
        response = self._call_api(
            "POST",
            f"{self._base_url}/ids/auth/login",
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials"},
        )
        try:
            self._api_token = response["access_token"]
        except KeyError as err:
            self._log.warn(f"Failed to retrieve API auth token: {err}")
            raise err
        self._log.info("API auth token successfully retrieved")

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _call_api(
        self,
        method: str,
        url: str,
        headers: Optional[Dict] = None,
        auth: Optional[Dict] = None,
        data: Optional[Dict] = None,
        json_data: Optional[Dict] = None,
    ) -> Dict:
        """Wrapper around requests.request for error logging and handling

        :param method: request method
        :type method: str
        :param url: request url
        :type url: str
        :param headers: request headers, defaults to None
        :type headers: Optional[Dict], optional
        :param auth: request auth, defaults to None
        :type auth: Optional[Dict], optional
        :param data: request data, defaults to None
        :type data: Optional[Dict], optional
        :param json_data: request json, defaults to None
        :type json_data: Optional[Dict], optional
        :raises err: request exception
        :return: response.json
        :rtype: Dict
        """
        try:
            response = requests.request(
                method,
                url,
                headers=headers,
                auth=auth,
                data=data,
                json=json_data,
                timeout=60,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self._log.warn(f"Failed to retrieve data: {err}")
            self._log.warn(response.reason)
            self._log.warn(
                f"{response.reason}: {err}\n"
                f"url: {response.request.url}\n"
                f"body: {response.request.body}"
            )
            raise err
        return response.json()

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_exponential_jitter(initial=4, max=600, jitter=10),
    )
    def check_job_status(self, job_id: str):
        try:
            response = requests.request(
                "GET",
                f"{self._base_url}/dap/job/{job_id}",
                headers={"Accept": "application/json", "x-instauth": self._api_token},
                timeout=60,
            )
            response.raise_for_status()
        except requests.exceptions.HTTPError as err:
            self._log.warn(f"Failed to retrieve data: {err}")
            self._log.warn(response.reason)
            raise err
        return response.json()

    @staticmethod
    def _get_cert_context() -> ssl.SSLContext:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context

    def get_data(self, namespace: str, table: str, since: Optional[str]) -> List[Dict]:
        self._log.info(f"Fetching data for '{namespace}/{table}'")
        # prepare query parameters
        json_data = {"format": "jsonl"}
        if since is not None:
            json_data["since"] = since
        # query table for data
        response = self._call_api(
            "POST",
            f"{self._base_url}/dap/query/{namespace}/table/{table}/data",
            headers={"Accept": "application/json", "x-instauth": self._api_token},
            json_data=json_data,
        )
        status = response["status"]
        job_id = response["id"]
        while status in ("waiting", "running"):
            # wait until query is completed using a longer exponential backoff
            response = self.check_job_status(job_id)
            status = response["status"]
        if status == "failed":
            if "since" in response:
                self._log.warn(f"earliest permitted timestamp: {response['since']}")
            raise ValueError(f"job failed: {response}")
        data_objects = response["objects"]
        schema_version = response["schema_version"]
        # obtain the last timestamp in records (i.e., starting point of next query)
        try:
            query_ts = response["at"]
        except KeyError:
            query_ts = response["until"]
        self._log.info(f"{len(data_objects)} objects to download")
        # get a downloadable link for each object returned from query
        dl_response = self._call_api(
            "POST",
            f"{self._base_url}/dap/object/url",
            headers={"Accept": "application/json", "x-instauth": self._api_token},
            json_data=data_objects,
        )
        # iterate over download links, download, unzip, and combine json lines
        result = []
        context = self._get_cert_context()
        for obj, item in dl_response["urls"].items():
            self._log.info(f"downloading {obj}")
            dl_url = item["url"]
            with urllib.request.urlopen(dl_url, context=context) as r:
                with gzip.GzipFile(fileobj=r) as uncompressed:
                    result.extend(
                        [
                            {"data": json.loads(json_str)}
                            for json_str in uncompressed.read().decode().splitlines()
                        ]
                    )
        return result, schema_version, query_ts
