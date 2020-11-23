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
from typing import List, Optional, Sequence, Type, TypeVar

from opentelemetry.exporter.otlp.encoder import Encoder
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest as PB2ExportMetricsServiceRequest,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2_grpc import (
    MetricsServiceStub as PB2MetricsServiceStub,
)
from opentelemetry.proto.common.v1.common_pb2 import StringKeyValue
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
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric as OTLPMetric
from opentelemetry.proto.metrics.v1.metrics_pb2 import (
    ResourceMetrics as PB2ResourceMetrics,
)
from opentelemetry.sdk.metrics import (
    Counter as SDKCounter,
    SumObserver as SDKSumObserver,
    UpDownCounter as SDKUpDownCounter,
    UpDownSumObserver as SDKUpDownSumObserver,
    ValueObserver as SDKValueObserver,
    ValueRecorder as SDKValueRecorder,
)
from opentelemetry.sdk.metrics.export import (
    ExportRecord as SDKExportRecord,
    MetricsExporter as SDKMetricsExporter,
    MetricsExportResult as SDKMetricsExportResult,
)
from opentelemetry.sdk.metrics.export.aggregate import (
    HistogramAggregator as SDKHistogramAggregator,
    LastValueAggregator as SDKLastValueAggregator,
    MinMaxSumCountAggregator as SDKMinMaxSumCountAggregator,
    SumAggregator as SDKSumAggregator,
    ValueObserverAggregator as SDKValueObserverAggregator,
)

logger = logging.getLogger(__name__)


class MetricProtobufEncoder(Encoder):
    @staticmethod
    def content_type() -> str:
        return "application/x-protobuf"

    @classmethod
    def serialize(cls, sdk_metrics: Sequence[SDKExportRecord]) -> str:
        return cls.encode(sdk_metrics).SerializeToString()

    @staticmethod
    def encode(
        sdk_metrics: Sequence[SDKExportRecord],
    ) -> PB2ExportMetricsServiceRequest:
        return PB2ExportMetricsServiceRequest(
            resource_metrics=_encode_resource_metrics(sdk_metrics)
        )


def _encode_resource_metrics(
    sdk_metrics: Sequence[SDKExportRecord],
) -> List[PB2ResourceMetrics]:
    return []  # TODO
