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
from typing import List, Optional, Sequence

from opentelemetry.exporter.otlp.encoder.protobuf import (
    _encode_instrumentation_library,
    _encode_resource,
    ProtobufEncoder,
)
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest as PB2ExportTraceServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import KeyValue as PB2KeyValue
from opentelemetry.proto.trace.v1.trace_pb2 import (
    InstrumentationLibrarySpans as PB2InstrumentationLibrarySpans,
    ResourceSpans as PB2ResourceSpans,
)
from opentelemetry.proto.trace.v1.trace_pb2 import (
    Span as PB2SPan,
    Status as PB2Status,
)
from opentelemetry.sdk.trace import (
    Event as SDKEvent,
    Span as SDKSpan,
    SpanContext as SDKSpanContext,
)
from opentelemetry.trace import (
    Link as SDKLink,
    SpanKind as SDKSpanKind,
    TraceState as SDKTraceState,
)
from opentelemetry.trace.status import (
    Status as SDKStatus,
    StatusCode as SDKStatusCode,
)
from opentelemetry.util.types import Attributes as SDKAttributes

# pylint: disable=E1101
SPAN_KIND_MAP = {
    SDKSpanKind.INTERNAL: PB2SPan.SpanKind.SPAN_KIND_INTERNAL,
    SDKSpanKind.SERVER: PB2SPan.SpanKind.SPAN_KIND_SERVER,
    SDKSpanKind.CLIENT: PB2SPan.SpanKind.SPAN_KIND_CLIENT,
    SDKSpanKind.PRODUCER: PB2SPan.SpanKind.SPAN_KIND_PRODUCER,
    SDKSpanKind.CONSUMER: PB2SPan.SpanKind.SPAN_KIND_CONSUMER,
}

logger = logging.getLogger(__name__)


class SpanProtobufEncoder(ProtobufEncoder):
    @classmethod
    def serialize(cls, sdk_spans: Sequence[SDKSpan]) -> str:
        return cls.encode(sdk_spans).SerializeToString()

    @staticmethod
    def encode(sdk_spans: Sequence[SDKSpan],) -> PB2ExportTraceServiceRequest:
        return PB2ExportTraceServiceRequest(
            resource_spans=_encode_resource_spans(sdk_spans)
        )


def _encode_resource_spans(
    sdk_spans: Sequence[SDKSpan],
) -> List[PB2ResourceSpans]:
    # We need to inspect the spans and group + structure them as:
    #
    #   Resource
    #     Instrumentation Library
    #       Spans
    #
    # First loop organizes the SDK spans in this structure. Protobuf messages
    # are not hashable so we stick with SDK data in this phase.
    #
    # Second loop encodes the data into Protobuf format.
    #
    sdk_resource_spans = {}

    for sdk_span in sdk_spans:
        sdk_resource = sdk_span.resource
        sdk_instrumentation = sdk_span.instrumentation_info or "None"
        pb2_span = _encode_span(sdk_span)

        if sdk_resource not in sdk_resource_spans.keys():
            sdk_resource_spans[sdk_resource] = {
                sdk_instrumentation: [pb2_span]
            }
        elif (
            sdk_instrumentation not in sdk_resource_spans[sdk_resource].keys()
        ):
            sdk_resource_spans[sdk_resource][sdk_instrumentation] = [pb2_span]
        else:
            sdk_resource_spans[sdk_resource][sdk_instrumentation].append(
                pb2_span
            )

    pb2_resource_spans = []

    for sdk_resource, sdk_instrumentations in sdk_resource_spans.items():
        instrumentation_library_spans = []
        for sdk_instrumentation, pb2_spans in sdk_instrumentations.items():
            instrumentation_library_spans.append(
                PB2InstrumentationLibrarySpans(
                    instrumentation_library=(
                        _encode_instrumentation_library(sdk_instrumentation)
                    ),
                    spans=pb2_spans,
                )
            )
        pb2_resource_spans.append(
            PB2ResourceSpans(
                resource=_encode_resource(sdk_resource),
                instrumentation_library_spans=instrumentation_library_spans,
            )
        )

    return pb2_resource_spans


def _encode_span(sdk_span: SDKSpan) -> PB2SPan:
    sdk_context = sdk_span.get_span_context()
    return PB2SPan(
        trace_id=_encode_trace_id(sdk_context.trace_id),
        span_id=_encode_span_id(sdk_context.span_id),
        trace_state=_encode_trace_state(sdk_context.trace_state),
        parent_span_id=_encode_parent_id(sdk_span.parent),
        name=sdk_span.name,
        kind=SPAN_KIND_MAP[sdk_span.kind],
        start_time_unix_nano=sdk_span.start_time,
        end_time_unix_nano=sdk_span.end_time,
        attributes=_encode_attributes(sdk_span.attributes),
        events=_encode_events(sdk_span.events),
        links=_encode_links(sdk_span.links),
        status=_encode_status(sdk_span.status),
    )


def _encode_events(
    sdk_events: Sequence[SDKEvent],
) -> Optional[List[PB2SPan.Event]]:
    pb2_events = None
    if sdk_events:
        pb2_events = []
        for sdk_event in sdk_events:
            encoded_event = PB2SPan.Event(
                name=sdk_event.name, time_unix_nano=sdk_event.timestamp,
            )
            for key, value in sdk_event.attributes.items():
                try:
                    encoded_event.attributes.append(
                        _encode_key_value(key, value)
                    )
                # pylint: disable=broad-except
                except Exception as error:
                    logger.exception(error)
            pb2_events.append(encoded_event)
    return pb2_events


def _encode_links(sdk_links: List[SDKLink]) -> List[PB2SPan.Link]:
    pb2_links = None
    if sdk_links:
        pb2_links = []
        for sdk_link in sdk_links:
            encoded_link = PB2SPan.Link(
                trace_id=_encode_trace_id(sdk_link.context.trace_id),
                span_id=_encode_span_id(sdk_link.context.span_id),
            )
            for key, value in sdk_link.attributes.items():
                try:
                    encoded_link.attributes.append(
                        _encode_key_value(key, value)
                    )
                # pylint: disable=broad-except
                except Exception as error:
                    logger.exception(error)
            pb2_links.append(encoded_link)
    return pb2_links


def _encode_status(sdk_status: SDKStatus) -> Optional[PB2Status]:
    pb2_status = None
    if sdk_status is not None:
        # TODO: Update this when the proto definitions are updated to include
        #  UNSET and ERROR
        encoded_status_code = PB2Status.STATUS_CODE_OK
        if sdk_status.status_code is SDKStatusCode.ERROR:
            encoded_status_code = PB2Status.STATUS_CODE_UNKNOWN_ERROR
        pb2_status = PB2Status(
            code=encoded_status_code, message=sdk_status.description,
        )
    return pb2_status


def _encode_trace_state(sdk_trace_state: SDKTraceState) -> Optional[str]:
    pb2_trace_state = None
    if sdk_trace_state is not None:
        pb2_trace_state = ",".join(
            [
                "{}={}".format(key, value)
                for key, value in (sdk_trace_state.items())
            ]
        )
    return pb2_trace_state


def _encode_parent_id(context: Optional[SDKSpanContext]) -> Optional[bytes]:
    if isinstance(context, SDKSpanContext):
        encoded_parent_id = _encode_span_id(context.span_id)
    else:
        encoded_parent_id = None
    return encoded_parent_id


def _encode_attributes(
    sdk_attributes: SDKAttributes,
) -> Optional[List[PB2KeyValue]]:
    if sdk_attributes:
        attributes = []
        for key, value in sdk_attributes.items():
            try:
                attributes.append(_encode_key_value(key, value))
            except Exception as error:  # pylint: disable=broad-except
                logger.exception(error)
    else:
        attributes = None
    return attributes


def _encode_span_id(span_id: int) -> bytes:
    return span_id.to_bytes(length=8, byteorder="big", signed=False)


def _encode_trace_id(trace_id: int) -> bytes:
    return trace_id.to_bytes(length=16, byteorder="big", signed=False)
