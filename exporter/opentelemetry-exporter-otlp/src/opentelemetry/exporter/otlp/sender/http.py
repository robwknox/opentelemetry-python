# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import requests
from typing import Dict, Optional


REQUESTS_SUCCESS_STATUS_CODES = (200, 202)

logger = logging.getLogger(__name__)


class HttpSender:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        insecure: Optional[bool] = None,
        cert_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[str] = None,
    ):
        self._endpoint = endpoint
        self._insecure = insecure
        self._cert_file = cert_file
        self._headers = headers
        self._timeout = timeout
        self._compression = compression

    def send(self, serialized_spans: str, content_type: str) -> bool:
        post_result = requests.post(
            url=self._endpoint,
            data=serialized_spans,
            headers={**self._headers, **{"Content-Type": content_type}},
        )

        if post_result.status_code in REQUESTS_SUCCESS_STATUS_CODES:
            success = True
        else:
            logger.error(
                "Traces cannot be uploaded; status code: %s, message %s",
                post_result.status_code,
                post_result.text,
            )
            success = False
        return success
