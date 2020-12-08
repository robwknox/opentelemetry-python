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

from opentelemetry.exporter.otlp.encoder.protobuf import (
    _encode_instrumentation_library,
    _encode_resource,
    ProtobufEncoder,
)
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
    ExportMetricsServiceRequest as PB2ExportMetricsServiceRequest,
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
from opentelemetry.proto.metrics.v1.metrics_pb2 import Metric as PB2Metric
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
from opentelemetry.sdk.metrics.export import ExportRecord as SDKExportRecord
from opentelemetry.sdk.metrics.export.aggregate import (
    HistogramAggregator as SDKHistogramAggregator,
    LastValueAggregator as SDKLastValueAggregator,
    MinMaxSumCountAggregator as SDKMinMaxSumCountAggregator,
    SumAggregator as SDKSumAggregator,
    ValueObserverAggregator as SDKValueObserverAggregator,
)

DataPointT = TypeVar("DataPointT", PB2IntDataPoint, PB2DoubleDataPoint)

INSTRUMENT_VALUE_TYPE_MAPPING = {
    int: {
        "sum": {"class": PB2IntSum, "argument": "int_sum"},
        "gauge": {"class": PB2IntGauge, "argument": "int_gauge"},
        "data_point_class": PB2IntDataPoint,
    },
    float: {
        "sum": {"class": PB2DoubleSum, "argument": "double_sum"},
        "gauge": {"class": PB2DoubleGauge, "argument": "double_gauge",},
        "data_point_class": PB2DoubleDataPoint,
    },
}

logger = logging.getLogger(__name__)


class MetricProtobufEncoder(ProtobufEncoder):
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
    sdk_export_records: Sequence[SDKExportRecord],
) -> List[PB2ResourceMetrics]:
    # We need to inspect the records and group + structure the metrics as:
    #
    #   Resource
    #     Instrumentation Library
    #       Metrics
    #
    # First loop organizes the SDK metrics in this structure. Protobuf messages
    # are not hashable so we stick with SDK data in this phase.
    #
    # Second loop encodes the data into Protobuf format.
    #
    sdk_resource_metrics = {}

    for sdk_export_record in sdk_export_records:

        pb2_metric = _encode_metric(sdk_export_record)

        if pb2_metric is None:
            continue

        sdk_resource = sdk_export_record.resource
        sdk_instrumentation = (
            sdk_export_record.instrument.meter.instrumentation_info or None
        )

        if sdk_resource not in sdk_resource_metrics.keys():
            sdk_resource_metrics[sdk_resource] = {
                sdk_instrumentation: [pb2_metric]
            }
        elif (
            sdk_instrumentation
            not in sdk_resource_metrics[sdk_resource].keys()
        ):
            sdk_resource_metrics[sdk_resource][sdk_instrumentation] = [
                pb2_metric
            ]
        else:
            sdk_resource_metrics[sdk_resource][sdk_instrumentation].append(
                pb2_metric
            )

    pb2_resource_metrics = []

    for sdk_resource, sdk_instrumentations in sdk_resource_metrics.items():
        instrumentation_library_metrics = []
        for sdk_instrumentation, pb2_metrics in sdk_instrumentations.items():
            instrumentation_library_metrics.append(
                PB2InstrumentationLibraryMetrics(
                    instrumentation_library=(
                        _encode_instrumentation_library(sdk_instrumentation)
                    ),
                    metrics=pb2_metrics,
                )
            )
        pb2_resource_metrics.append(
            PB2ResourceMetrics(
                resource=_encode_resource(sdk_resource),
                instrumentation_library_metrics=(
                    instrumentation_library_metrics
                ),
            )
        )

    return pb2_resource_metrics


def _encode_metric(sdk_export_record: SDKExportRecord) -> Optional[PB2Metric]:
    """
    The criteria to decide how to translate sdk_export_record is based on
    this table taken directly from OpenTelemetry Proto v0.5.0:

    Instrument         Type
    ----------------------------------------------
    Counter            Sum(aggregation_temporality=delta;is_monotonic=true)
    UpDownCounter      Sum(aggregation_temporality=delta;is_monotonic=false)
    SumObserver        Sum(aggregation_temporality=cumulative;is_monotonic=true)
    UpDownSumObserver  Sum(aggregation_temporality=cumulative;is_monotonic=false)
    ValueObserver      Gauge()
    ValueRecorder      TBD

    TODO: Update table after the decision on:
    https://github.com/open-telemetry/opentelemetry-specification/issues/731.
    By default, metrics recording using the OpenTelemetry API are exported as
    (the table does not include MeasurementValueType to avoid extra rows):
    """
    value_type_mapping = INSTRUMENT_VALUE_TYPE_MAPPING[
        sdk_export_record.instrument.value_type
    ]

    sum_class = value_type_mapping["sum"]["class"]
    gauge_class = value_type_mapping["gauge"]["class"]
    data_point_class = value_type_mapping["data_point_class"]

    if isinstance(sdk_export_record.instrument, SDKCounter):
        aggregation_temporality = (
            PB2AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
        )
        pb2_metric_data_arg = value_type_mapping["sum"]["argument"]
        pb2_metric_data = sum_class(
            data_points=_get_data_points(
                sdk_export_record, data_point_class, aggregation_temporality,
            ),
            aggregation_temporality=aggregation_temporality,
            is_monotonic=True,
        )

    elif isinstance(sdk_export_record.instrument, SDKUpDownCounter):
        aggregation_temporality = (
            PB2AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
        )
        pb2_metric_data_arg = value_type_mapping["sum"]["argument"]
        pb2_metric_data = sum_class(
            data_points=_get_data_points(
                sdk_export_record, data_point_class, aggregation_temporality,
            ),
            aggregation_temporality=aggregation_temporality,
            is_monotonic=False,
        )

    elif isinstance(sdk_export_record.instrument, SDKSumObserver):
        aggregation_temporality = (
            PB2AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
        )
        pb2_metric_data_arg = value_type_mapping["sum"]["argument"]
        pb2_metric_data = sum_class(
            data_points=_get_data_points(
                sdk_export_record, data_point_class, aggregation_temporality,
            ),
            aggregation_temporality=aggregation_temporality,
            is_monotonic=True,
        )

    elif isinstance(sdk_export_record.instrument, SDKUpDownSumObserver):
        aggregation_temporality = (
            PB2AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
        )
        pb2_metric_data_arg = value_type_mapping["sum"]["argument"]
        pb2_metric_data = sum_class(
            data_points=_get_data_points(
                sdk_export_record, data_point_class, aggregation_temporality,
            ),
            aggregation_temporality=aggregation_temporality,
            is_monotonic=False,
        )

    elif isinstance(sdk_export_record.instrument, SDKValueObserver):
        pb2_metric_data_arg = value_type_mapping["gauge"]["argument"]
        pb2_metric_data = gauge_class(
            data_points=_get_data_points(
                sdk_export_record,
                data_point_class,
                PB2AggregationTemporality.AGGREGATION_TEMPORALITY_DELTA,
            )
        )

    elif isinstance(sdk_export_record.instrument, SDKValueRecorder):
        logger.warning("Skipping exporting of ValueRecorder metric")
        return None

    else:
        return None

    return PB2Metric(
        **{
            "name": sdk_export_record.instrument.name,
            "description": sdk_export_record.instrument.description,
            "unit": sdk_export_record.instrument.unit,
            pb2_metric_data_arg: pb2_metric_data,
        }
    )


def _get_data_points(
    export_record: SDKExportRecord,
    data_point_class: Type[DataPointT],
    aggregation_temporality: int,
) -> List[DataPointT]:

    if isinstance(export_record.aggregator, SDKSumAggregator):
        value = export_record.aggregator.checkpoint

    elif isinstance(export_record.aggregator, SDKMinMaxSumCountAggregator):
        # FIXME: How are values to be interpreted from this aggregator?
        raise Exception("MinMaxSumCount aggregator data not supported")

    elif isinstance(export_record.aggregator, SDKHistogramAggregator):
        # FIXME: How are values to be interpreted from this aggregator?
        raise Exception("Histogram aggregator data not supported")

    elif isinstance(export_record.aggregator, SDKLastValueAggregator):
        value = export_record.aggregator.checkpoint

    elif isinstance(export_record.aggregator, SDKValueObserverAggregator):
        value = export_record.aggregator.checkpoint.last

    if aggregation_temporality == (
        PB2AggregationTemporality.AGGREGATION_TEMPORALITY_CUMULATIVE
    ):
        start_time_unix_nano = export_record.aggregator.first_timestamp
    else:
        start_time_unix_nano = (
            export_record.aggregator.initial_checkpoint_timestamp
        )

    return [
        data_point_class(
            labels=[
                StringKeyValue(key=str(label_key), value=str(label_value))
                for label_key, label_value in export_record.labels
            ],
            value=value,
            start_time_unix_nano=start_time_unix_nano,
            time_unix_nano=export_record.aggregator.last_update_timestamp,
        )
    ]
