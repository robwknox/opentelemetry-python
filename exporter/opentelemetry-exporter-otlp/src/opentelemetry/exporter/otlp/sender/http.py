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

import gzip
from io import BytesIO
import logging
import requests
from typing import Optional
import zlib

from opentelemetry.exporter.otlp import Compression, Headers

logger = logging.getLogger(__name__)


class HttpSender:
    def __init__(
        self,
        endpoint: Optional[str] = None,
        insecure: Optional[bool] = False,
        cert_file: Optional[str] = None,
        headers: Optional[Headers] = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
    ):
        self._endpoint = endpoint
        self._insecure = insecure
        self._cert_file = cert_file
        self._headers = headers
        self._timeout = timeout
        self._compression = compression

    def send(self, serialized_spans: str, content_type: str) -> bool:
        post_args = {
            "headers": {**self._headers, **{"Content-Type": content_type}},
        }

        if self._timeout:
            post_args["timeout"] = float(self._timeout)

        if not self._insecure and self._cert_file:
            post_args["verify"] = self._cert_file

        if self._compression == Compression.GZIP:
            gzip_data = BytesIO()
            with gzip.GzipFile(fileobj=gzip_data, mode="w") as f:
                f.write(serialized_spans)
            post_args["headers"]["Content-Encoding"] = "gzip"
            post_args["data"] = gzip_data.getvalue()
        elif self._compression == Compression.DEFLATE:
            post_args["headers"]["Content-Encoding"] = "deflate"
            post_args["data"] = zlib.compress(bytes(serialized_spans))
        else:
            post_args["data"] = serialized_spans

        post_result = requests.post(self._endpoint, **post_args)

        if post_result.status_code in (200, 202):
            success = True
        else:
            logger.error(
                "Traces cannot be uploaded; status code: %s, message %s",
                post_result.status_code,
                post_result.text,
            )
            success = False
        return success
