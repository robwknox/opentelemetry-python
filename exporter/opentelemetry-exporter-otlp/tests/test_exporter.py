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

import os
import unittest
from unittest.mock import patch

from opentelemetry.configuration import Configuration
from opentelemetry.exporter.otlp import (
    DEFAULT_ENDPOINT,
    DEFAULT_INSECURE,
    DEFAULT_COMPRESSION,
    DEFAULT_TIMEOUT,
    OTLPExporter,
    _parse_headers,
)
from opentelemetry.exporter.otlp.util import (
    ExporterType,
    Protocol,
    Compression,
)
from opentelemetry.exporter.otlp.encoder.metric.protobuf import (
    MetricProtobufEncoder,
)
from opentelemetry.exporter.otlp.encoder.span.protobuf import (
    SpanProtobufEncoder,
)
from opentelemetry.exporter.otlp.sender.grpc import GrpcSender
from opentelemetry.exporter.otlp.sender.http import HttpSender

OS_ENV_BASE_ENDPOINT = "os.env.base"
OS_ENV_BASE_CERTIFICATE = "os/env/base.crt"
OS_ENV_BASE_HEADERS = "osenv=base"
OS_ENV_BASE_TIMEOUT = "300"

OS_ENV_SPAN_ENDPOINT = "os.env.span"
OS_ENV_SPAN_CERTIFICATE = "os/env/span.crt"
OS_ENV_SPAN_HEADERS = "osenv=span"
OS_ENV_SPAN_TIMEOUT = "400"

OS_ENV_METRIC_ENDPOINT = "os.env.metric"
OS_ENV_METRIC_CERTIFICATE = "os/env/metric.crt"
OS_ENV_METRIC_HEADERS = "osenv=metric"
OS_ENV_METRIC_TIMEOUT = "500"


class TestOTLPExporter(unittest.TestCase):
    def tearDown(self):
        Configuration()._reset()  # pylint: disable=protected-access
        otlp_env_vars = [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_SPAN_ENDPOINT",
            "OTEL_EXPORTER_OTLP_METRIC_ENDPOINT",
            "OTEL_EXPORTER_OTLP_PROTOCOL",
            "OTEL_EXPORTER_OTLP_SPAN_PROTOCOL",
            "OTEL_EXPORTER_OTLP_METRIC_PROTOCOL",
            "OTEL_EXPORTER_OTLP_INSECURE",
            "OTEL_EXPORTER_OTLP_SPAN_INSECURE",
            "OTEL_EXPORTER_OTLP_METRIC_INSECURE",
            "OTEL_EXPORTER_OTLP_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_SPAN_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_METRIC_CERTIFICATE",
            "OTEL_EXPORTER_OTLP_HEADERS",
            "OTEL_EXPORTER_OTLP_SPAN_HEADERS",
            "OTEL_EXPORTER_OTLP_METRIC_HEADERS",
            "OTEL_EXPORTER_OTLP_COMPRESSION",
            "OTEL_EXPORTER_OTLP_SPAN_COMPRESSION",
            "OTEL_EXPORTER_OTLP_METRIC_COMPRESSION",
            "OTEL_EXPORTER_OTLP_TIMEOUT",
            "OTEL_EXPORTER_OTLP_SPAN_TIMEOUT",
            "OTEL_EXPORTER_OTLP_METRIC_TIMEOUT",
        ]
        for otlp_env_var in otlp_env_vars:
            if otlp_env_var in os.environ:
                del os.environ[otlp_env_var]

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_default(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        # must be provided in default mode since we default to secure grpc
        cert_file = "fixtures/service.crt"

        exporter = OTLPExporter(
            exporter_type=ExporterType.SPAN, cert_file=cert_file
        )

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            DEFAULT_ENDPOINT,
            DEFAULT_INSECURE,
            cert_file,
            None,
            DEFAULT_TIMEOUT,
            DEFAULT_COMPRESSION,
        )

    def test_constructor_default_requires_cert_file(self):
        # must be provided since we default to secure grpc.
        with self.assertRaises(ValueError) as cm:
            OTLPExporter(ExporterType.SPAN)
        self.assertEqual(
            cm.exception.args[0], "No cert_file provided in secure mode."
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_span_grpc_args(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        exporter = OTLPExporter(
            exporter_type=ExporterType.SPAN,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.GRPC,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )


    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_span_grpc_args_override_env(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        os_env_protocol = Protocol.GRPC
        os_env_compression = Compression.DEFLATE

        self._set_os_env_base_vars(os_env_protocol, True, os_env_compression)
        self._set_os_env_metric_vars(os_env_protocol, True, os_env_compression)

        exporter = OTLPExporter(
            exporter_type=ExporterType.SPAN,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.GRPC,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_span_grpc_env_base(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        self._set_os_env_base_vars(Protocol.GRPC, True, Compression.GZIP)

        exporter = OTLPExporter(exporter_type=ExporterType.SPAN)

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            OS_ENV_BASE_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_BASE_HEADERS),
            int(OS_ENV_BASE_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_span_grpc_env_span(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        self._set_os_env_span_vars(Protocol.GRPC, True, Compression.GZIP)

        exporter = OTLPExporter(exporter_type=ExporterType.SPAN)

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            OS_ENV_SPAN_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_SPAN_HEADERS),
            int(OS_ENV_SPAN_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_span_grpc_env_span_overrides_base(
            self, mock_grpc_sender
    ):
        mock_grpc_sender.return_value = None

        self._set_os_env_base_vars(
            Protocol.HTTP_PROTOBUF, True, Compression.GZIP
        )
        self._set_os_env_span_vars(Protocol.GRPC, False, Compression.DEFLATE)

        exporter = OTLPExporter(exporter_type=ExporterType.SPAN)

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            OS_ENV_SPAN_ENDPOINT,
            False,
            OS_ENV_SPAN_CERTIFICATE,
            _parse_headers(OS_ENV_SPAN_HEADERS),
            int(OS_ENV_SPAN_TIMEOUT),
            Compression.DEFLATE,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_span_http_args(self, mock_http_sender):
        mock_http_sender.return_value = None

        exporter = OTLPExporter(
            exporter_type=ExporterType.SPAN,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.HTTP_PROTOBUF,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_span_http_args_override_env(self, mock_http_sender):
        mock_http_sender.return_value = None

        os_env_protocol = Protocol.GRPC
        os_env_compression = Compression.DEFLATE

        self._set_os_env_base_vars(os_env_protocol, True, os_env_compression)
        self._set_os_env_span_vars(os_env_protocol, True, os_env_compression)

        exporter = OTLPExporter(
            exporter_type=ExporterType.SPAN,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.HTTP_PROTOBUF,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_span_http_env_base(self, mock_http_sender):
        mock_http_sender.return_value = None

        self._set_os_env_base_vars(
            Protocol.HTTP_PROTOBUF, True, Compression.GZIP
        )

        exporter = OTLPExporter(exporter_type=ExporterType.SPAN)

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            OS_ENV_BASE_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_BASE_HEADERS),
            int(OS_ENV_BASE_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_span_http_env_span(self, mock_http_sender):
        mock_http_sender.return_value = None

        self._set_os_env_span_vars(
            Protocol.HTTP_PROTOBUF, True, Compression.GZIP
        )

        exporter = OTLPExporter(exporter_type=ExporterType.SPAN)

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            OS_ENV_SPAN_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_SPAN_HEADERS),
            int(OS_ENV_SPAN_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_span_http_env_span_overrides_base(self, mock_http_sender):
        mock_http_sender.return_value = None

        self._set_os_env_base_vars(Protocol.GRPC, True, Compression.GZIP)
        self._set_os_env_span_vars(
            Protocol.HTTP_PROTOBUF, False, Compression.DEFLATE
        )

        exporter = OTLPExporter(exporter_type=ExporterType.SPAN)

        self.assertEqual(exporter._type, ExporterType.SPAN)
        self.assertIsInstance(exporter._encoder, SpanProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            OS_ENV_SPAN_ENDPOINT,
            False,
            OS_ENV_SPAN_CERTIFICATE,
            _parse_headers(OS_ENV_SPAN_HEADERS),
            int(OS_ENV_SPAN_TIMEOUT),
            Compression.DEFLATE,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_metric_grpc_args(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        exporter = OTLPExporter(
            exporter_type=ExporterType.METRIC,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.GRPC,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_metric_grpc_args_override_env(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        os_env_protocol = Protocol.GRPC
        os_env_compression = Compression.DEFLATE

        self._set_os_env_base_vars(os_env_protocol, True, os_env_compression)
        self._set_os_env_metric_vars(os_env_protocol, True, os_env_compression)

        exporter = OTLPExporter(
            exporter_type=ExporterType.METRIC,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.GRPC,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_metric_grpc_env_base(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        self._set_os_env_base_vars(Protocol.GRPC, True, Compression.GZIP)

        exporter = OTLPExporter(exporter_type=ExporterType.METRIC)

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            OS_ENV_BASE_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_BASE_HEADERS),
            int(OS_ENV_BASE_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_metric_grpc_env_metric(self, mock_grpc_sender):
        mock_grpc_sender.return_value = None

        self._set_os_env_metric_vars(Protocol.GRPC, True, Compression.GZIP)

        exporter = OTLPExporter(exporter_type=ExporterType.METRIC)

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            OS_ENV_METRIC_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_METRIC_HEADERS),
            int(OS_ENV_METRIC_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.grpc.GrpcSender.__init__")
    def test_constructor_metric_grpc_env_metric_overrides_base(
            self, mock_grpc_sender
    ):
        mock_grpc_sender.return_value = None

        self._set_os_env_base_vars(
            Protocol.HTTP_PROTOBUF, True, Compression.GZIP
        )
        self._set_os_env_metric_vars(Protocol.GRPC, False, Compression.DEFLATE)

        exporter = OTLPExporter(exporter_type=ExporterType.METRIC)

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, GrpcSender)

        mock_grpc_sender.assert_called_once_with(
            OS_ENV_METRIC_ENDPOINT,
            False,
            OS_ENV_METRIC_CERTIFICATE,
            _parse_headers(OS_ENV_METRIC_HEADERS),
            int(OS_ENV_METRIC_TIMEOUT),
            Compression.DEFLATE,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_metric_http_args(self, mock_http_sender):
        mock_http_sender.return_value = None

        exporter = OTLPExporter(
            exporter_type=ExporterType.METRIC,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.HTTP_PROTOBUF,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_metric_http_args_override_env(self, mock_http_sender):
        mock_http_sender.return_value = None

        os_env_protocol = Protocol.GRPC
        os_env_compression = Compression.DEFLATE

        self._set_os_env_base_vars(os_env_protocol, True, os_env_compression)
        self._set_os_env_metric_vars(os_env_protocol, True, os_env_compression)

        exporter = OTLPExporter(
            exporter_type=ExporterType.METRIC,
            endpoint="test.endpoint.com:46484",
            protocol=Protocol.HTTP_PROTOBUF,
            insecure=False,
            cert_file="fixtures/service.crt",
            headers="testHeader1=value1,testHeader2=value2",
            timeout=2,
            compression=Compression.GZIP,
        )

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            "test.endpoint.com:46484",
            False,
            "fixtures/service.crt",
            {"testHeader1": "value1", "testHeader2": "value2"},
            2,
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_metric_http_env_base(self, mock_http_sender):
        mock_http_sender.return_value = None

        self._set_os_env_base_vars(
            Protocol.HTTP_PROTOBUF, True, Compression.GZIP
        )

        exporter = OTLPExporter(exporter_type=ExporterType.METRIC)

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            OS_ENV_BASE_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_BASE_HEADERS),
            int(OS_ENV_BASE_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_metric_http_env_metric(self, mock_http_sender):
        mock_http_sender.return_value = None

        self._set_os_env_metric_vars(
            Protocol.HTTP_PROTOBUF, True, Compression.GZIP
        )

        exporter = OTLPExporter(exporter_type=ExporterType.METRIC)

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            OS_ENV_METRIC_ENDPOINT,
            True,
            None,
            _parse_headers(OS_ENV_METRIC_HEADERS),
            int(OS_ENV_METRIC_TIMEOUT),
            Compression.GZIP,
        )

    @patch("opentelemetry.exporter.otlp.sender.http.HttpSender.__init__")
    def test_constructor_metric_http_env_metric_overrides_base(
            self, mock_http_sender
    ):
        mock_http_sender.return_value = None

        self._set_os_env_base_vars(Protocol.GRPC, True, Compression.GZIP)
        self._set_os_env_metric_vars(
            Protocol.HTTP_PROTOBUF, False, Compression.DEFLATE
        )

        exporter = OTLPExporter(exporter_type=ExporterType.METRIC)

        self.assertEqual(exporter._type, ExporterType.METRIC)
        self.assertIsInstance(exporter._encoder, MetricProtobufEncoder)
        self.assertIsInstance(exporter._sender, HttpSender)

        mock_http_sender.assert_called_once_with(
            OS_ENV_METRIC_ENDPOINT,
            False,
            OS_ENV_METRIC_CERTIFICATE,
            _parse_headers(OS_ENV_METRIC_HEADERS),
            int(OS_ENV_METRIC_TIMEOUT),
            Compression.DEFLATE,
        )

    @staticmethod
    def _set_os_env_base_vars(
            protocol: Protocol, insecure: bool, compression: Compression
    ):
        os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = OS_ENV_BASE_ENDPOINT
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = protocol.value
        os.environ["OTEL_EXPORTER_OTLP_INSECURE"] = str(insecure).lower()
        os.environ["OTEL_EXPORTER_OTLP_CERTIFICATE"] = OS_ENV_BASE_CERTIFICATE
        os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = OS_ENV_BASE_HEADERS
        os.environ["OTEL_EXPORTER_OTLP_COMPRESSION"] = compression.value
        os.environ["OTEL_EXPORTER_OTLP_TIMEOUT"] = OS_ENV_BASE_TIMEOUT

    @staticmethod
    def _set_os_env_span_vars(
            protocol: Protocol, insecure: bool, compression: Compression
    ):
        os.environ["OTEL_EXPORTER_OTLP_SPAN_ENDPOINT"] = OS_ENV_SPAN_ENDPOINT
        os.environ["OTEL_EXPORTER_OTLP_SPAN_PROTOCOL"] = protocol.value
        os.environ["OTEL_EXPORTER_OTLP_SPAN_INSECURE"] = str(insecure).lower()
        os.environ["OTEL_EXPORTER_OTLP_SPAN_CERTIFICATE"] = (
            OS_ENV_SPAN_CERTIFICATE
        )
        os.environ["OTEL_EXPORTER_OTLP_SPAN_HEADERS"] = OS_ENV_SPAN_HEADERS
        os.environ["OTEL_EXPORTER_OTLP_SPAN_COMPRESSION"] = compression.value
        os.environ["OTEL_EXPORTER_OTLP_SPAN_TIMEOUT"] = OS_ENV_SPAN_TIMEOUT

    @staticmethod
    def _set_os_env_metric_vars(
            protocol: Protocol, insecure: bool, compression: Compression
    ):
        os.environ["OTEL_EXPORTER_OTLP_METRIC_ENDPOINT"] = (
            OS_ENV_METRIC_ENDPOINT
        )
        os.environ["OTEL_EXPORTER_OTLP_METRIC_PROTOCOL"] = protocol.value
        os.environ["OTEL_EXPORTER_OTLP_METRIC_INSECURE"] = (
            str(insecure).lower()
        )
        os.environ["OTEL_EXPORTER_OTLP_METRIC_CERTIFICATE"] = (
            OS_ENV_METRIC_CERTIFICATE
        )
        os.environ["OTEL_EXPORTER_OTLP_METRIC_HEADERS"] = OS_ENV_METRIC_HEADERS
        os.environ["OTEL_EXPORTER_OTLP_METRIC_COMPRESSION"] = compression.value
        os.environ["OTEL_EXPORTER_OTLP_METRIC_TIMEOUT"] = OS_ENV_METRIC_TIMEOUT

