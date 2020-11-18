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

import unittest
from unittest.mock import patch

from opentelemetry.exporter.zipkin import ZipkinSpanExporter
from opentelemetry.exporter.zipkin.encoder import Encoding
from opentelemetry.exporter.zipkin.sender.http import HttpSender
from opentelemetry.sdk.trace.export import SpanExportResult


class MockResponse:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = status_code


class TestHttpSender(unittest.TestCase):
    def test_constructor_invalid_encoding(self):
        with self.assertRaises(ValueError):
            HttpSender("https://localhost/api", "Fake_Encoding")

    @patch("requests.post")
    def test_send_endpoint(self, mock_post):
        endpoint = "http://localhost:9411/api/v2/spans"
        mock_post.return_value = MockResponse(200)
        result = ZipkinSpanExporter("test-service", endpoint).export([])
        self.assertEqual(SpanExportResult.SUCCESS, result)
        # pylint: disable=unsubscriptable-object
        kwargs = mock_post.call_args[1]
        self.assertEqual(kwargs["url"], endpoint)

    @patch("requests.post")
    def _test_send_content_type(self, encoding, content_type, mock_post):
        mock_post.return_value = MockResponse(200)
        ZipkinSpanExporter("test-service", encoding=encoding).export([])
        # pylint: disable=unsubscriptable-object
        kwargs = mock_post.call_args[1]
        self.assertEqual(content_type, kwargs["headers"]["Content-Type"])

    def test_send_content_type_v1_thrift(self):
        self._test_send_content_type(
            Encoding.V1_THRIFT, "application/x-thrift"
        )

    def test_send_content_type_v1_json(self):
        self._test_send_content_type(Encoding.V1_JSON, "application/json")

    def test_send_content_type_v2_json(self):
        self._test_send_content_type(Encoding.V2_JSON, "application/json")

    def test_send_content_type_v2_protobuf(self):
        self._test_send_content_type(
            Encoding.V2_PROTOBUF, "application/x-protobuf"
        )

    @patch("requests.post")
    def test_response_success(self, mock_post):
        mock_post.return_value = MockResponse(200)
        self.assertEqual(
            SpanExportResult.SUCCESS,
            ZipkinSpanExporter("test-service").export([]),
        )

    @patch("requests.post")
    def test_response_failure(self, mock_post):
        with self.assertLogs(level="ERROR") as cm:
            mock_post.return_value = MockResponse(404)
            self.assertEqual(
                SpanExportResult.FAILURE,
                ZipkinSpanExporter("test-service").export([]),
            )
        self.assertEqual(
            "Traces cannot be uploaded; status code: 404, message 404",
            cm.records[0].message,
        )
