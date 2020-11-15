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

"""Zipkin Export Encoder for Thrift

API spec: https://github.com/openzipkin/zipkin-api/tree/master/thrift

Thrift documentation is severely lacking and largely outdated, so the cppkin
implementation proved invaluable as a reference and is the basis for the
majority of the encoding logic. Link:

https://github.com/Dudi119/cppKin/blob/master/src/ThriftEncoder.h

WARNING: Thrift only supports signed integers (max 64 bits). This results in
the Zipkin Thrift API having definitions of:

  i64 id (span id)
  i64 trace_id
  i64 trace_id_high

Which in turn results in our encoding having truncation logic built-in to avoid
overflow when converting from unsigned -> signed ints, as well as to chunk the
trace_id into two components if necessary for transport. Refer to the
encode_trace_id() and encode_span_id() methods for details.
"""

import ipaddress
import logging
from typing import Sequence

from opentelemetry.exporter.zipkin.encoder.v1 import V1Encoder
from opentelemetry.exporter.zipkin.encoder.v1.thrift.gen.zipkinCore import (
    ttypes,
)
from opentelemetry.trace import Span, SpanContext
from thrift.Thrift import TType
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol import TBinaryProtocol

logger = logging.getLogger(__name__)


class ThriftEncoder(V1Encoder):
    """Zipkin Export Encoder for Thrift

    API spec: https://github.com/openzipkin/zipkin-api/tree/master/thrift
    """

    def _encode_spans(self, spans: Sequence[Span]):
        encoded_local_endpoint = self._encode_local_endpoint()
        buffer = TMemoryBuffer()
        protocol = TBinaryProtocol.TBinaryProtocolFactory().getProtocol(buffer)
        protocol.writeListBegin(TType.STRUCT, len(spans))
        for span in spans:
            self._encode_span(span, encoded_local_endpoint).write(protocol)
        protocol.writeListEnd()
        return buffer.getvalue()

    def _encode_local_endpoint(self):
        endpoint = ttypes.Endpoint(
            service_name=self.local_endpoint.service_name,
            port=self.local_endpoint.port,
        )
        if self.local_endpoint.ipv4 is not None:
            endpoint.ipv4 = ipaddress.ip_address(
                self.local_endpoint.ipv4
            ).packed
        if self.local_endpoint.ipv6 is not None:
            endpoint.ipv6 = ipaddress.ip_address(
                self.local_endpoint.ipv6
            ).packed
        return endpoint

    def _encode_span(self, span: Span, encoded_local_endpoint):
        context = span.get_span_context()
        thrift_trace_id, thrift_trace_id_high = self.encode_trace_id(
            context.trace_id
        )
        thrift_span = ttypes.Span(
            trace_id=thrift_trace_id,
            trace_id_high=thrift_trace_id_high,
            id=self.encode_span_id(context.span_id),
            name=span.name,
            timestamp=self.nsec_to_usec_round(span.start_time),
            duration=self.nsec_to_usec_round(span.end_time - span.start_time),
            annotations=self._encode_annotations(span, encoded_local_endpoint),
            binary_annotations=self._encode_binary_annotations(
                span, encoded_local_endpoint
            ),
        )

        if context.trace_flags.sampled:
            thrift_span.debug = True

        if isinstance(span.parent, Span):
            thrift_span.parent_id = self.encode_span_id(
                span.parent.get_span_context().span_id
            )
        elif isinstance(span.parent, SpanContext):
            thrift_span.parent_id = self.encode_span_id(span.parent.span_id)

        return thrift_span

    def _encode_annotations(self, span: Span, encoded_local_endpoint):
        annotations = self._extract_annotations_from_events(span.events)
        if annotations is None:
            encoded_annotations = None
        else:
            encoded_annotations = []
            for annotation in self._extract_annotations_from_events(
                span.events
            ):
                encoded_annotations.append(
                    ttypes.Annotation(
                        timestamp=annotation["timestamp"],
                        value=annotation["value"],
                        host=encoded_local_endpoint,
                    )
                )
        return encoded_annotations

    def _encode_binary_annotations(self, span: Span, encoded_local_endpoint):
        thrift_binary_annotations = []

        for binary_annotation in self._extract_binary_annotations(
            span, encoded_local_endpoint
        ):
            thrift_binary_annotations.append(
                ttypes.BinaryAnnotation(
                    key=binary_annotation["key"],
                    value=binary_annotation["value"].encode("utf-8"),
                    annotation_type=ttypes.AnnotationType.STRING,
                    host=binary_annotation["endpoint"],
                )
            )

        return thrift_binary_annotations

    @staticmethod
    def encode_span_id(span_id: int):
        """Since Thrift only supports signed integers (max size 64 bits) the
        Zipkin Thrift API defines the span id as an i64 field.

        If a provided span id is > 63 bits it will be truncated to 63 bits
        to fit into the API field and a warning log will be emitted. We use
        63 bits instead of 64 bits because we have to leave 1 bit in the API
        field for the positive sign representation.
        """
        bits = format(span_id, "b")
        if len(bits) < 64:
            encoded_span_id = span_id
        else:
            encoded_span_id = int(bits[-63:], 2)
            logger.warning(
                "Span id truncated to fit into Thrift "
                "protocol signed integer format: [%02x => %02x]",
                span_id,
                encoded_span_id,
            )

        return encoded_span_id

    @staticmethod
    def encode_trace_id(trace_id: int):
        """Since Thrift only supports signed integers (max size 64 bits) the
        Zipkin Thrift API defines two fields to hold a trace id:
          - i64 trace_id
          - i64 trace_id_high (only used if provided trace id is > 63 bits)

        If a provided trace id is > 126 bits it will be truncated to 126 bits
        to fit into the API fields and a warning log will be emitted. We use
        126 bits instead of 128 bits because we have to leave 1 bit in each
        of the two API fields for the positive sign representation.

        :param trace_id:
        :return: tuple of (encoded_trace_id, encoded_trace_id_high)
        """
        bits = format(trace_id, "b")
        bits_length = len(bits)
        encoded_trace_id = int(bits[-63:], 2)

        if bits_length > 63:
            encoded_trace_id_high = int(bits[-126:-63], 2)
            if bits_length > 126:
                logger.warning(
                    "Trace id truncated to fit into Thrift "
                    "protocol signed integer format: [%02x => %02x%02x]",
                    trace_id,
                    encoded_trace_id_high,
                    encoded_trace_id,
                )
        else:
            encoded_trace_id_high = None

        return encoded_trace_id, encoded_trace_id_high
