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
from time import sleep
from typing import Dict, Optional

from backoff import expo
from google.rpc.error_details_pb2 import RetryInfo
from grpc import (
    ChannelCredentials,
    Compression,
    RpcError,
    StatusCode,
    insecure_channel,
    secure_channel,
    ssl_channel_credentials,
)

from opentelemetry.exporter import otlp
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2_grpc import (
    TraceServiceStub
)

RETRYABLE_ERROR_CODES = [
    StatusCode.CANCELLED,
    StatusCode.DEADLINE_EXCEEDED,
    StatusCode.PERMISSION_DENIED,
    StatusCode.UNAUTHENTICATED,
    StatusCode.RESOURCE_EXHAUSTED,
    StatusCode.ABORTED,
    StatusCode.OUT_OF_RANGE,
    StatusCode.UNAVAILABLE,
    StatusCode.DATA_LOSS,
]

logger = logging.getLogger(__name__)


class GrpcSender:
    def __init__(
        self,
        endpoint: str,
        insecure: Optional[bool] = False,
        cert_file: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[int] = None,
        compression: Optional[otlp.Compression] = None,
    ):
        self._endpoint = endpoint
        self._insecure = insecure
        self._cert_file = cert_file
        self._headers = headers
        self._timeout = timeout

        if not compression or compression == otlp.Compression.NONE:
            grpc_compression = Compression.NoCompression
        elif compression == otlp.Compression.GZIP:
            grpc_compression = Compression.Gzip
        else:
            logger.warning("Unsupported compression type %r specified - "
                           "defaulting to none" % compression)
            grpc_compression = Compression.NoCompression

        self._compression = grpc_compression

        if insecure:
            self._channel = insecure_channel(
                self._endpoint, compression=self._compression
            )
        else:
            if not cert_file:
                raise ValueError("No cert_file provided in secure mode.")
            channel_credentials = _load_credential_from_file(cert_file)
            if not channel_credentials:
                raise ValueError(
                    "Unable to read credentials from file: %r" % cert_file
                )
            self._channel = secure_channel(
                self._endpoint,
                channel_credentials,
                compression=self._compression
            )

    def send(self, resource_spans: ExportTraceServiceRequest) -> bool:
        # expo returns a generator that yields delay values which grow
        # exponentially. Once delay is greater than max_value, the yielded
        # value will remain constant.
        # max_value is set to 900 (900 seconds is 15 minutes) to use the same
        # value as used in the Go implementation.
        expo_max_value = 900

        for delay in expo(max_value=expo_max_value):

            if delay == expo_max_value:
                return False

            try:
                TraceServiceStub(self._channel).Export(
                    request=resource_spans,
                    metadata=self._headers,
                    timeout=self._timeout,
                )
                return True
            except RpcError as error:
                if error.code() in RETRYABLE_ERROR_CODES:

                    retry_info_bin = dict(error.trailing_metadata()).get(
                        "google.rpc.retryinfo-bin"
                    )
                    if retry_info_bin is not None:
                        retry_info = RetryInfo()
                        retry_info.ParseFromString(retry_info_bin)
                        delay = (
                            retry_info.retry_delay.seconds
                            + retry_info.retry_delay.nanos / 1.0e9
                        )
                    logger.debug(
                        "Waiting %ss before retrying export of span", delay
                    )
                    sleep(delay)
                    continue

                if error.code() == StatusCode.OK:
                    return True
                return False

        return False


def _load_credential_from_file(filepath: str) -> Optional[ChannelCredentials]:
    try:
        with open(filepath, "rb") as f:
            credential = f.read()
            return ssl_channel_credentials(credential)
    except FileNotFoundError:
        return None
