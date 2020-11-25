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


"""
This library allows to export tracing data to an OTLP collector.

Usage
-----

The **OTLP Span Exporter** allows to export `OpenTelemetry`_ traces to the
`OTLP`_ collector.


.. _OTLP: https://github.com/open-telemetry/opentelemetry-collector/
.. _OpenTelemetry: https://github.com/open-telemetry/opentelemetry-python/

.. envvar:: OTEL_EXPORTER_OTLP_COMPRESSION

The :envvar:`OTEL_EXPORTER_OTLP_COMPRESSION` environment variable allows a
compression algorithm to be passed to the OTLP exporter. The compression
algorithms that are supported include gzip and no compression. The value should
be in the format of a string "gzip" for gzip compression, and no value specified
if no compression is the desired choice.
Additional details are available `in the specification
<https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/protocol/exporter.md#opentelemetry-protocol-exporter>`_.

.. code:: python

    from opentelemetry import trace
    from opentelemetry.exporter.otlp.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchExportSpanProcessor

    # Resource can be required for some backends, e.g. Jaeger
    # If resource wouldn't be set - traces wouldn't appears in Jaeger
    resource = Resource(attributes={
        "service.name": "service"
    })

    trace.set_tracer_provider(TracerProvider(resource=resource)))
    tracer = trace.get_tracer(__name__)

    otlp_exporter = OTLPSpanExporter(endpoint="localhost:55680", insecure=True)

    span_processor = BatchExportSpanProcessor(otlp_exporter)

    trace.get_tracer_provider().add_span_processor(span_processor)

    with tracer.start_as_current_span("foo"):
        print("Hello world!")

API
---
"""

import logging
from typing import Optional, Sequence, Union

from opentelemetry.configuration import Configuration
from opentelemetry.exporter.otlp.sender.grpc import GrpcSender
from opentelemetry.exporter.otlp.sender.http import HttpSender
from opentelemetry.exporter.otlp.encoder.metric.protobuf import (
    MetricProtobufEncoder,
)
from opentelemetry.exporter.otlp.encoder.span.protobuf import (
    SpanProtobufEncoder,
)
from opentelemetry.exporter.otlp.util import (
    Compression,
    ExporterType,
    HeadersInput,
    Headers,
    Protocol,
)
from opentelemetry.sdk.metrics.export import ExportRecord, MetricsExportResult
from opentelemetry.sdk.trace import Span
from opentelemetry.sdk.trace.export import SpanExportResult

DEFAULT_ENDPOINT = "localhost:4317"
DEFAULT_PROTOCOL = Protocol.GRPC
DEFAULT_INSECURE = False
DEFAULT_COMPRESSION = Compression.NONE
DEFAULT_TIMEOUT = 10  # seconds

SDKExportData = Union[Sequence[ExportRecord], Sequence[Span]]
ExportResult = Union[MetricsExportResult, SpanExportResult]

logger = logging.getLogger(__name__)


class OTLPExporter:
    def __init__(
        self,
        exporter_type: ExporterType,
        endpoint: Optional[str] = None,
        protocol: Optional[Protocol] = None,
        insecure: Optional[bool] = None,
        cert_file: Optional[str] = None,
        headers: HeadersInput = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
    ):
        self._type = exporter_type
        type_name = exporter_type.value
        config = Configuration()

        endpoint = (
            endpoint
            or getattr(config, "EXPORTER_OTLP_" + type_name + "_ENDPOINT")
            or getattr(config, "EXPORTER_OTLP_ENDPOINT")
            or DEFAULT_ENDPOINT
        )

        protocol = protocol or self._get_env_or_default_protocol()

        if insecure is None:
            insecure = self._get_env_or_default_insecure()

        if insecure:
            cert_file = None
        else:
            cert_file = (
                cert_file
                or getattr(
                    config, "EXPORTER_OTLP_" + type_name + "_CERTIFICATE"
                )
                or getattr(config, "EXPORTER_OTLP_CERTIFICATE")
            )

        headers = _parse_headers(
            headers
            or getattr(config, "EXPORTER_OTLP_" + type_name + "_HEADERS")
            or getattr(config, "EXPORTER_OTLP_HEADERS")
        )

        timeout = int(
            timeout
            or getattr(config, "EXPORTER_OTLP_" + type_name + "_TIMEOUT")
            or getattr(config, "EXPORTER_OTLP_TIMEOUT")
            or DEFAULT_TIMEOUT
        )

        compression = compression or self._get_env_or_default_compression()

        if protocol == Protocol.GRPC:
            self._sender = GrpcSender(
                endpoint, insecure, cert_file, headers, timeout, compression
            )
        else:
            self._sender = HttpSender(
                endpoint, insecure, cert_file, headers, timeout, compression
            )

        if exporter_type == ExporterType.SPAN:
            self._encoder = SpanProtobufEncoder()
        else:
            self._encoder = MetricProtobufEncoder()

    def export(self, sdk_data: SDKExportData) -> ExportResult:
        if isinstance(self._sender, GrpcSender):
            send_result = self._sender.send(self._encoder.encode(sdk_data))
        else:
            send_result = self._sender.send(
                self._encoder.serialize(sdk_data), self._encoder.content_type()
            )

        if self._type == ExporterType.SPAN:
            export_result = (
                SpanExportResult.SUCCESS
                if send_result
                else SpanExportResult.FAILURE
            )
        else:
            export_result = (
                MetricsExportResult.SUCCESS
                if send_result
                else MetricsExportResult.FAILURE
            )

        return export_result

    def shutdown(self) -> None:
        pass

    def _get_env_or_default_compression(self) -> Compression:
        config = Configuration()
        exporter_type_env_val = getattr(
            config,
            "EXPORTER_OTLP_" + self._type.value + "_COMPRESSION",
        )
        if exporter_type_env_val:
            compression = Compression(exporter_type_env_val)
        else:
            exporter_env_val = config.EXPORTER_OTLP_COMPRESSION
            if exporter_env_val:
                compression = Compression(exporter_env_val)
            else:
                compression = DEFAULT_COMPRESSION
        return compression

    def _get_env_or_default_protocol(self) -> Protocol:
        config = Configuration()
        exporter_type_env_val = getattr(
            config,
            "EXPORTER_OTLP_" + self._type.value + "_PROTOCOL",
        )
        if exporter_type_env_val:
            protocol = Protocol(exporter_type_env_val)
        else:
            exporter_env_val = config.EXPORTER_OTLP_PROTOCOL
            if exporter_env_val:
                protocol = Protocol(exporter_env_val)
            else:
                protocol = DEFAULT_PROTOCOL
        return protocol

    def _get_env_or_default_insecure(self) -> bool:
        config = Configuration()
        env_val = (
            getattr(config, "EXPORTER_OTLP_" + self._type.value + "_INSECURE")
            or getattr(config, "EXPORTER_OTLP_INSECURE")
        )

        if env_val:
            env_val_lower = str(env_val).lower()
            if env_val_lower == "false":
                insecure = False
            elif env_val_lower == "true":
                insecure = True
            else:
                logger.warning(
                    "Invalid value %s provided for 'insecure' "
                    "parameter - defaulting to True.",
                    env_val_lower)
                insecure = True
        else:
            insecure = DEFAULT_INSECURE

        return insecure


class OTLPSpanExporter(OTLPExporter):
    """Convenience class"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        protocol: Optional[Protocol] = None,
        insecure: Optional[bool] = None,
        cert_file: Optional[str] = None,
        headers: HeadersInput = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
    ):
        super().__init__(
            exporter_type=ExporterType.SPAN,
            endpoint=endpoint,
            protocol=protocol,
            insecure=insecure,
            cert_file=cert_file,
            headers=headers,
            timeout=timeout,
            compression=compression,
        )

    def export_spans(self, sdk_spans: Sequence[Span]) -> SpanExportResult:
        return super().export(sdk_spans)


class OTLPMetricExporter(OTLPExporter):
    """Convenience class"""

    def __init__(
        self,
        endpoint: Optional[str] = None,
        protocol: Optional[Protocol] = None,
        insecure: Optional[bool] = None,
        cert_file: Optional[str] = None,
        headers: HeadersInput = None,
        timeout: Optional[int] = None,
        compression: Optional[Compression] = None,
    ):
        super().__init__(
            exporter_type=ExporterType.METRIC,
            endpoint=endpoint,
            protocol=protocol,
            insecure=insecure,
            cert_file=cert_file,
            headers=headers,
            timeout=timeout,
            compression=compression,
        )

    def export_metrics(
        self, sdk_metrics: Sequence[ExportRecord]
    ) -> MetricsExportResult:
        return super().export(sdk_metrics)


def _parse_headers(headers_input: HeadersInput) -> Optional[Headers]:
    if isinstance(headers_input, dict):
        headers = headers_input
    elif isinstance(headers_input, str):
        headers = {}
        for header in headers_input.split(","):
            header_parts = header.split("=")
            if len(header_parts) == 2:
                headers[header_parts[0]] = header_parts[1]
            else:
                logger.warning(
                    "Invalid OTLP exporter header skipped: %r", header
                )
        if not headers:
            headers = None
    else:
        headers = None

    return headers
