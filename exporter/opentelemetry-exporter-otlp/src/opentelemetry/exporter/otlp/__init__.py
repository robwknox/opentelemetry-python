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
import enum
import logging
from typing import Dict, Optional, Union


class Protocol(enum.Enum):
    GRPC = "grpc"
    HTTP_PROTOBUF = "http/protobuf"


class Compression(enum.Enum):
    DEFLATE = "deflate"
    GZIP = "gzip"
    NONE = "none"


DEFAULT_ENDPOINT = "localhost:4317"
DEFAULT_PROTOCOL = Protocol.GRPC
DEFAULT_INSECURE = False
DEFAULT_COMPRESSION = Compression.NONE
DEFAULT_TIMEOUT = 10  # seconds

HeadersInput = Union[Dict[str, str], str, None]
Headers = Dict[str, str]

logger = logging.getLogger(__name__)


def parse_headers(headers_input: HeadersInput) -> Headers:
    if headers_input is None:
        headers = {}
    elif isinstance(headers_input, dict):
        headers = headers_input
    elif isinstance(headers_input, str):
        headers = {}
        for header in headers_input.split(","):
            header_parts = header.split("=")
            if len(header_parts) == 2:
                headers[header_parts[0]] = header_parts[1]
            else:
                logger.warning(
                    "Invalid OTLP exporter header skipped: %r" % header
                )
    else:
        headers = {}

    return headers
