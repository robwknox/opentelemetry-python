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
import ipaddress
import json
import sys

from .common_tests import CommonEncoderTestCases
from opentelemetry import trace as trace_api
from opentelemetry.exporter.zipkin.encoder.v1.thrift import ThriftEncoder
from opentelemetry.exporter.zipkin.encoder.v1.thrift.gen.zipkinCore import (
    ttypes,
)
from opentelemetry.exporter.zipkin.endpoint import Endpoint
from opentelemetry.sdk import trace
from opentelemetry.trace import SpanKind, TraceFlags
from thrift.Thrift import TType
from thrift.transport.TTransport import TMemoryBuffer
from thrift.protocol import TBinaryProtocol


class TestThriftEncoder(CommonEncoderTestCases.CommonEncoderTest):

    @staticmethod
    def get_encoder(*args, **kwargs) -> ThriftEncoder:
        return ThriftEncoder(*args, **kwargs)

    def test_encode_trace_id(self):
        trace_id = 2**63 - 1
        encoded_trace_id, encoded_trace_id_high = (
            ThriftEncoder.encode_trace_id(trace_id)
        )
        self.assertEqual(int(format(trace_id, "b")[-63:], 2), encoded_trace_id)
        self.assertEqual(None, encoded_trace_id_high)

    def test_encode_trace_id_64_bits(self):
        trace_id = 2**63
        encoded_trace_id, encoded_trace_id_high = (
            ThriftEncoder.encode_trace_id(trace_id)
        )
        self.assertEqual(int(format(trace_id, "b")[-63:], 2), encoded_trace_id)
        self.assertEqual(
            int(format(trace_id, "b")[-126:-63], 2),
            encoded_trace_id_high
        )

    def test_encode_trace_id_126_bits(self):
        trace_id = 2**126 - 1
        encoded_trace_id, encoded_trace_id_high = (
            ThriftEncoder.encode_trace_id(trace_id)
        )
        self.assertEqual(int(format(trace_id, "b")[-63:], 2), encoded_trace_id)
        self.assertEqual(
            int(format(trace_id, "b")[-126:-63], 2),
            encoded_trace_id_high
        )

    def test_encode_trace_id_127_bits(self):
        trace_id = 2**126
        with self.assertLogs(level='WARNING') as cm:
            encoded_trace_id, encoded_trace_id_high = (
                ThriftEncoder.encode_trace_id(trace_id)
            )
        self.assertEqual(
            'Trace id truncated to fit into Thrift protocol signed integer '
            'format: [40000000000000000000000000000000 => 0000]',
            cm.records[0].message
        )
        self.assertEqual(int(format(trace_id, "b")[-63:], 2), encoded_trace_id)
        self.assertEqual(
            int(format(trace_id, "b")[-126:-63], 2),
            encoded_trace_id_high
        )

    def test_encode_span_id(self):
        span_id = 2**63 - 1
        self.assertEqual(
            int(format(span_id, "b")[-63:], 2),
            ThriftEncoder.encode_span_id(span_id),
        )

    def test_encode_span_id_truncate(self):
        span_id = 2**63
        with self.assertLogs(level='WARNING') as cm:
            encoded_span_id = ThriftEncoder.encode_span_id(span_id)
        self.assertEqual(
            'Span id truncated to fit into Thrift protocol signed integer '
            'format: [8000000000000000 => 00]',
            cm.records[0].message
        )
        self.assertEqual(int(format(span_id, "b")[-63:], 2), encoded_span_id)

    def test_encode_local_endpoint_default(self):
        service_name = "test-service-name"
        self.assertEqual(
            ttypes.Endpoint(service_name=service_name),
            ThriftEncoder(Endpoint(service_name))._encode_local_endpoint(),
        )

    def test_encode_local_endpoint_explicits(self):
        service_name = "test-service-name"
        ipv4 = "192.168.0.1"
        ipv6 = "2001:db8::c001"
        port = 414120
        self.assertEqual(
            ttypes.Endpoint(
                service_name=service_name,
                ipv4=ipaddress.ip_address(ipv4).packed,
                ipv6=ipaddress.ip_address(ipv6).packed,
                port=port,
            ),
            ThriftEncoder(
                Endpoint(service_name, ipv4, ipv6, port)
            )._encode_local_endpoint(),
        )

    def test_encode(self):
        service_name = "test-service"
        local_endpoint = ttypes.Endpoint(service_name=service_name)

        otel_spans = self.get_exhaustive_otel_span_list()
        thrift_trace_id, thrift_trace_id_high = ThriftEncoder.encode_trace_id(
            otel_spans[0].context.trace_id
        )

        expected_thrift_spans = [
            ttypes.Span(
                trace_id=thrift_trace_id,
                trace_id_high=thrift_trace_id_high,
                id=ThriftEncoder.encode_span_id(
                    otel_spans[0].context.span_id
                ),
                name=otel_spans[0].name,
                timestamp=ThriftEncoder.nsec_to_usec_round(
                    otel_spans[0].start_time
                ),
                duration=(
                    ThriftEncoder.nsec_to_usec_round(
                        otel_spans[0].end_time - otel_spans[0].start_time
                    )
                ),
                annotations=[
                    ttypes.Annotation(
                        timestamp=otel_spans[0].events[0].timestamp
                        // 10 ** 3,
                        value=json.dumps({
                            "event0": {
                                "annotation_bool": True,
                                "annotation_string": "annotation_test",
                                "key_float": 0.3,
                            }
                        }),
                        host=local_endpoint,
                    )
                ],
                binary_annotations=[
                    ttypes.BinaryAnnotation(
                        key="key_bool",
                        value=str(False).encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="key_string",
                        value="hello_world".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="key_float",
                        value="111.22".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="otel.status_code",
                        value="2".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="otel.status_description",
                        value="Example description".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                ],
                debug=True,
                parent_id=ThriftEncoder.encode_span_id(
                    otel_spans[0].parent.span_id
                ),
            ),
            ttypes.Span(
                trace_id=thrift_trace_id,
                trace_id_high=thrift_trace_id_high,
                id=ThriftEncoder.encode_span_id(
                    otel_spans[1].context.span_id
                ),
                name=otel_spans[1].name,
                timestamp=ThriftEncoder.nsec_to_usec_round(
                    otel_spans[1].start_time
                ),
                duration=(
                    ThriftEncoder.nsec_to_usec_round(
                        otel_spans[1].end_time - otel_spans[1].start_time
                    )
                ),
                annotations=None,
                binary_annotations=[
                    ttypes.BinaryAnnotation(
                        key="key_resource",
                        value="some_resource".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="otel.status_code",
                        value="1".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                ],
                debug=False,
            ),
            ttypes.Span(
                trace_id=thrift_trace_id,
                trace_id_high=thrift_trace_id_high,
                id=ThriftEncoder.encode_span_id(
                    otel_spans[2].context.span_id
                ),
                name=otel_spans[2].name,
                timestamp=ThriftEncoder.nsec_to_usec_round(
                    otel_spans[2].start_time
                ),
                duration=(
                    ThriftEncoder.nsec_to_usec_round(
                        otel_spans[2].end_time - otel_spans[2].start_time
                    )
                ),
                annotations=None,
                binary_annotations=[
                    ttypes.BinaryAnnotation(
                        key="key_string",
                        value="hello_world".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="key_resource",
                        value="some_resource".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="otel.status_code",
                        value="1".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                ],
                debug=False,
            ),
            ttypes.Span(
                trace_id=thrift_trace_id,
                trace_id_high=thrift_trace_id_high,
                id=ThriftEncoder.encode_span_id(
                    otel_spans[3].context.span_id
                ),
                name=otel_spans[3].name,
                timestamp=ThriftEncoder.nsec_to_usec_round(
                    otel_spans[3].start_time
                ),
                duration=(
                    ThriftEncoder.nsec_to_usec_round(
                        otel_spans[3].end_time - otel_spans[3].start_time
                    )
                ),
                annotations=None,
                binary_annotations=[
                    ttypes.BinaryAnnotation(
                        key="otel.instrumentation_library.name",
                        value="name".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="otel.instrumentation_library.version",
                        value="version".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                    ttypes.BinaryAnnotation(
                        key="otel.status_code",
                        value="1".encode("utf-8"),
                        annotation_type=ttypes.AnnotationType.STRING,
                        host=local_endpoint,
                    ),
                ],
                debug=False,
            ),
        ]

        self.assertEqual_encoded_spans(
            expected_thrift_spans,
            ThriftEncoder(Endpoint(service_name)).encode(otel_spans)
        )

    def _test_encode_max_tag_length(self, max_tag_value_length: int):
        service_name = "test-service"
        trace_id = 0x000C63257DE34C926F9EFCD03927272E
        span_id = 0x04BF92DEEFC58C92
        start_time = 683647322 * 10 ** 9  # in ns
        duration = 50 * 10 ** 6
        end_time = start_time + duration
        tag1_value = "v" * 500
        tag2_value = "v" * 50

        otel_span = trace._Span(
            name=service_name,
            context=trace_api.SpanContext(
                trace_id,
                span_id,
                is_remote=False,
                trace_flags=TraceFlags(TraceFlags.SAMPLED),
            ),
        )
        otel_span.start(start_time=start_time)
        otel_span.resource = trace.Resource({})
        otel_span.set_attribute("k1", tag1_value)
        otel_span.set_attribute("k2", tag2_value)
        otel_span.end(end_time=end_time)

        thrift_trace_id, thrift_trace_id_high = ThriftEncoder.encode_trace_id(
            trace_id
        )
        thrift_local_endpoint = ttypes.Endpoint(service_name=service_name)
        expected_thrift_span = ttypes.Span(
            trace_id=thrift_trace_id,
            trace_id_high=thrift_trace_id_high,
            id=ThriftEncoder.encode_span_id(span_id),
            name=service_name,
            timestamp=ThriftEncoder.nsec_to_usec_round(start_time),
            duration=ThriftEncoder.nsec_to_usec_round(duration),
            annotations=None,
            binary_annotations=[
                ttypes.BinaryAnnotation(
                    key="k1",
                    value=tag1_value[:max_tag_value_length].encode("utf-8"),
                    annotation_type=ttypes.AnnotationType.STRING,
                    host=thrift_local_endpoint,
                ),
                ttypes.BinaryAnnotation(
                    key="k2",
                    value=tag2_value[:max_tag_value_length].encode("utf-8"),
                    annotation_type=ttypes.AnnotationType.STRING,
                    host=thrift_local_endpoint,
                ),
                ttypes.BinaryAnnotation(
                    key="otel.status_code",
                    value="1".encode("utf-8"),
                    annotation_type=ttypes.AnnotationType.STRING,
                    host=thrift_local_endpoint,
                ),
            ],
            debug=True
        )

        test = ThriftEncoder(Endpoint(service_name))
        encode = test.encode([otel_span])

        self.assertEqual_encoded_spans(
            [expected_thrift_span],
            ThriftEncoder(
                Endpoint(service_name),
                max_tag_value_length
            ).encode([otel_span])
        )

    def assertEqual_encoded_spans(self, expected_thrift_spans,
                                  actual_serialized_output):
        """Since list ordering is not guaranteed in py3.5 or lower we can't
        compare the serialized output. Instead we deserialize the actual
        output and compare the thrift objects while explicitly handling the
        annotations and binary annotations lists."""
        if sys.version_info.major == 3 and sys.version_info.minor <= 5:
            actual_thrift_spans = []
            protocol = TBinaryProtocol.TBinaryProtocolFactory().getProtocol(
                TMemoryBuffer(actual_serialized_output)
            )
            etype, size = protocol.readListBegin()
            for _ in range(size):
                span = ttypes.Span()
                span.read(protocol)
                actual_thrift_spans.append(span)
            protocol.readListEnd()

            for expected_span, actual_span in zip(
                    expected_thrift_spans, actual_thrift_spans
            ):
                actual_annotations = actual_span.annotations
                if actual_annotations is not None:
                    actual_annotations = sorted(
                        actual_annotations, key=lambda x: x.timestamp
                    )
                expected_annotations = expected_span.annotations
                if expected_annotations is not None:
                    expected_annotations = sorted(
                        expected_annotations, key=lambda x: x.timestamp
                    )
                actual_binary_annotations = actual_span.binary_annotations
                if actual_binary_annotations is not None:
                    actual_binary_annotations = sorted(
                        actual_binary_annotations, key=lambda x: x.key
                    )
                expected_binary_annotations = expected_span.binary_annotations
                if expected_binary_annotations is not None:
                    expected_binary_annotations = sorted(
                        expected_binary_annotations, key=lambda x: x.key
                    )

                actual_span.annotations = []
                actual_span.binary_annotations = []
                expected_span.annotations = []
                expected_span.binary_annotations = []

                self.assertEqual(expected_span, actual_span)
                self.assertEqual(expected_annotations, actual_annotations)
                self.assertEqual(
                    expected_binary_annotations,
                    actual_binary_annotations
                )
        else:
            buffer = TMemoryBuffer()
            protocol = TBinaryProtocol.TBinaryProtocolFactory().getProtocol(
                buffer)
            protocol.writeListBegin(TType.STRUCT, len(expected_thrift_spans))
            for expected_thrift_span in expected_thrift_spans:
                expected_thrift_span.write(protocol)
            protocol.writeListEnd()
            expected_serialized_output = buffer.getvalue()
            self.assertEqual(expected_serialized_output,
                             actual_serialized_output)
