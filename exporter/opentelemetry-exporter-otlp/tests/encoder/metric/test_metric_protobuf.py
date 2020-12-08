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

from typing import List, Tuple
import unittest

from opentelemetry.exporter.otlp.encoder.metric.protobuf import (
    INSTRUMENT_VALUE_TYPE_MAPPING,
    MetricProtobufEncoder,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest as PB2ExportMetricsServiceRequest,
)
from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue as PB2AnyValue,
    InstrumentationLibrary as PB2InstrumentationLibrary,
    KeyValue as PB2KeyValue,
    StringKeyValue as PB2StringKeyValue,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    AggregationTemporality as PB2AggregationTemporality,
    DoubleDataPoint as PB2DoubleDataPoint,
    DoubleGauge as PB2DoubleGauge,
    DoubleSum as PB2DoubleSum,
    InstrumentationLibraryMetrics as PB2InstrumentationLibraryMetrics,
    IntDataPoint as PB2IntDataPoint,
    IntGauge as PB2IntGauge,
    IntSum as PB2IntSum,
)
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric as PB2Metric
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    ResourceMetrics as PB2ResourceMetrics,
)
from opentelemetry.proto.resource.v1.resource_pb2 import (
    Resource as PB2Resource,
)
from opentelemetry.sdk.metrics import (
    Counter as SDKCounter,
    MeterProvider as SDKMeterProvider,
    SumObserver as SDKSumObserver,
    UpDownCounter as SDKUpDownCounter,
    UpDownSumObserver as SDKUpDownSumObserver,
    ValueObserver as SDKValueObserver,
    ValueRecorder as SDKValueRecorder,
)
from opentelemetry.sdk.metrics.export import ExportRecord as SDKExportRecord
from opentelemetry.sdk.metrics.export.aggregate import (
    HistogramAggregator as SDKHistogramAggregator,
    LastValueAggregator as SDKLastValueAggregator,
    MinMaxSumCountAggregator as SDKMinMaxSumCountAggregator,
    SumAggregator as SDKSumAggregator,
    ValueObserverAggregator as SDKValueObserverAggregator,
)
from opentelemetry.sdk.trace import Resource as SDKResource


class TestMetricProtobufEncoder(unittest.TestCase):
    def test_encode(self):
        sdk_records, expected_encoding = self.get_test_metrics()
        self.assertEqual(
            MetricProtobufEncoder.encode(sdk_records), expected_encoding
        )

    def test_serialize(self):
        sdk_records, expected_encoding = self.get_test_metrics()
        self.assertEqual(
            MetricProtobufEncoder.serialize(sdk_records),
            expected_encoding.SerializeToString(),
        )

    def test_content_type(self):
        self.assertEqual(
            MetricProtobufEncoder.content_type(), "application/x-protobuf"
        )

    def get_test_metrics(
        self,
    ) -> Tuple[List[SDKExportRecord], PB2ExportMetricsServiceRequest]:

        meter_provider = SDKMeterProvider()
        accumulator = meter_provider.get_meter("module_name", "module_ver")

        base_time = 683647322 * 10 ** 9  # in ns
        first_times = (
            base_time,
            base_time + 150 * 10 ** 6,
            base_time + 300 * 10 ** 6,
            base_time + 400 * 10 ** 6,
        )
        last_times = (
            first_times[0] + (50 * 10 ** 6),
            first_times[1] + (100 * 10 ** 6),
            first_times[2] + (200 * 10 ** 6),
            first_times[3] + (300 * 10 ** 6),
        )

        aggregator_1 = SDKSumAggregator()
        aggregator_1.checkpoint = 111
        aggregator_1.first_timestamp = first_times[0]
        aggregator_1.last_update_timestamp = last_times[0]

        test_config = {
            "1": {
                "name": "test_counter_int",
                "desc": "Test counter of type int",
                "unit": "1",
                "labels": (("label1_key", "label1_val"),),
                "aggregator": aggregator_1,
            }
        }

        sdk_records = [
            SDKExportRecord(
                resource=SDKResource({}),
                instrument=SDKCounter(
                    name=test_config["1"]["name"],
                    description=test_config["1"]["desc"],
                    unit=test_config["1"]["unit"],
                    value_type=int,
                    meter=accumulator,
                ),
                aggregator=test_config["1"]["aggregator"],
                labels=test_config["1"]["labels"],
            )
        ]

        pb2_service_request = PB2ExportMetricsServiceRequest(
            resource_metrics=[
                PB2ResourceMetrics(
                    resource=PB2Resource(),
                    instrumentation_library_metrics=[
                        PB2InstrumentationLibraryMetrics(
                            instrumentation_library=PB2InstrumentationLibrary(
                                name="module_name", version="module_ver",
                            ),
                            metrics=[
                                PB2Metric(
                                    name=test_config["1"]["name"],
                                    description=test_config["1"]["desc"],
                                    unit=test_config["1"]["unit"],
                                    int_sum=PB2IntSum(
                                        data_points=[
                                            PB2IntDataPoint(
                                                labels=[
                                                    PB2StringKeyValue(
                                                        key=test_config["1"][
                                                            "labels"
                                                        ][0][0],
                                                        value=test_config["1"][
                                                            "labels"
                                                        ][0][1],
                                                    )
                                                ],
                                                value=test_config["1"][
                                                    "aggregator"
                                                ].checkpoint,
                                                time_unix_nano=test_config[
                                                    "1"
                                                ][
                                                    "aggregator"
                                                ].last_update_timestamp,
                                                start_time_unix_nano=test_config[
                                                    "1"
                                                ][
                                                    "aggregator"
                                                ].first_timestamp,
                                            )
                                        ],
                                        aggregation_temporality=(
                                            PB2AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
                                        ),
                                        is_monotonic=True,
                                    ),
                                )
                            ],
                        )
                    ],
                )
            ]
        )

        return sdk_records, pb2_service_request
