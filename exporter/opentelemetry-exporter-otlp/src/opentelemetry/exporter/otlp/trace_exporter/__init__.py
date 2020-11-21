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

from typing import Optional, Sequence

from opentelemetry.configuration import Configuration
from opentelemetry.exporter import otlp
from opentelemetry.exporter.otlp.sender.grpc import GrpcSender
from opentelemetry.exporter.otlp.sender.http import HttpSender
from opentelemetry.exporter.otlp.trace_exporter.encoder.protobuf import (
    ProtobufEncoder,
)
from opentelemetry.sdk.trace import Span
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult


class OTLPSpanExporter(SpanExporter):
    def __init__(
        self,
        endpoint: Optional[str] = None,
        protocol: Optional[otlp.Protocol] = None,
        insecure: Optional[bool] = None,
        cert_file: Optional[str] = None,
        headers: Optional[str] = None,
        timeout: Optional[int] = None,
        compression: Optional[otlp.Compression] = None,
    ):
        endpoint = (
            endpoint
            or Configuration().EXPORTER_OTLP_SPAN_ENDPOINT
            or Configuration().EXPORTER_OTLP_ENDPOINT
            or otlp.DEFAULT_ENDPOINT
        )

        protocol = (
            protocol
            or Configuration().EXPORTER_OTLP_SPAN_PROTOCOL
            or Configuration().EXPORTER_OTLP_PROTOCOL
            or otlp.DEFAULT_PROTOCOL
        )

        insecure = (
            insecure
            or Configuration().EXPORTER_OTLP_SPAN_INSECURE
            or Configuration().EXPORTER_OTLP_INSECURE
            or otlp.DEFAULT_INSECURE
        )

        if insecure:
            cert_file = None
        else:
            cert_file = (
                cert_file
                or Configuration().EXPORTER_OTLP_SPAN_CERTIFICATE
                or Configuration().EXPORTER_OTLP_CERTIFICATE
            )

        headers = otlp.parse_headers(
            headers
            or Configuration().EXPORTER_OTLP_SPAN_HEADERS
            or Configuration().EXPORTER_OTLP_HEADERS
        )

        timeout = (
            timeout
            or Configuration().EXPORTER_OTLP_SPAN_TIMEOUT
            or Configuration().EXPORTER_OTLP_TIMEOUT
            or otlp.DEFAULT_TIMEOUT
        )

        compression = compression or self._get_env_or_default_compression()

        if protocol == otlp.Protocol.GRPC:
            self.sender = GrpcSender(
                endpoint, insecure, cert_file, headers, timeout, compression
            )
        else:
            self.sender = HttpSender(
                endpoint, insecure, cert_file, headers, timeout, compression
            )

        self.encoder = ProtobufEncoder()

    def export(self, spans: Sequence[Span]) -> SpanExportResult:
        if isinstance(self.sender, GrpcSender):
            send_result = self.sender.send(self.encoder.encode_spans(spans))
        else:
            send_result = self.sender.send(
                self.encoder.serialize(spans), self.encoder.content_type()
            )

        return (
            SpanExportResult.SUCCESS
            if send_result
            else SpanExportResult.FAILURE
        )

    def shutdown(self) -> None:
        pass

    @staticmethod
    def _get_env_or_default_compression() -> otlp.Compression:
        exporter_span_env = Configuration().EXPORTER_OTLP_SPAN_COMPRESSION
        if exporter_span_env:
            compression = otlp.Compression(exporter_span_env)
        else:
            exporter_env = Configuration().EXPORTER_OTLP_COMPRESSION
            if exporter_env:
                compression = otlp.Compression(exporter_env)
            else:
                compression = otlp.DEFAULT_COMPRESSION
        return compression
