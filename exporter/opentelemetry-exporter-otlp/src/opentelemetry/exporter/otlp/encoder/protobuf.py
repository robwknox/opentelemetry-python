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

from abc import ABC
from collections import abc
import logging
from typing import Any, Text

from opentelemetry.proto.common.v1.common_pb2 import (
    AnyValue as PB2AnyValue,
    InstrumentationLibrary as PB2InstrumentationLibrary,
    KeyValue as PB2KeyValue,
)
from opentelemetry.proto.resource.v1.resource_pb2 import (
    Resource as PB2Resource,
)
from opentelemetry.sdk.trace import (
    InstrumentationInfo as SDKInstrumentationInfo,
    Resource as SDKResource,
)

logger = logging.getLogger(__name__)


class ProtobufEncoder(ABC):
    @staticmethod
    def content_type() -> str:
        return "application/x-protobuf"


def _encode_resource(sdk_resource: SDKResource) -> PB2Resource:
    pb2_resource = PB2Resource()
    for key, value in sdk_resource.attributes.items():
        try:
            # pylint: disable=no-member
            pb2_resource.attributes.append(_encode_key_value(key, value))
        except Exception as error:  # pylint: disable=broad-except
            logger.exception(error)
    return pb2_resource


def _encode_instrumentation_library(
    sdk_instrumentation_info: SDKInstrumentationInfo,
) -> PB2InstrumentationLibrary:
    if sdk_instrumentation_info is None:
        pb2_instrumentation_library = PB2InstrumentationLibrary()
    else:
        pb2_instrumentation_library = PB2InstrumentationLibrary(
            name=sdk_instrumentation_info.name,
            version=sdk_instrumentation_info.version,
        )
    return pb2_instrumentation_library


def _encode_key_value(key: Text, value: Any) -> PB2KeyValue:
    if isinstance(value, bool):
        any_value = PB2AnyValue(bool_value=value)
    elif isinstance(value, str):
        any_value = PB2AnyValue(string_value=value)
    elif isinstance(value, int):
        any_value = PB2AnyValue(int_value=value)
    elif isinstance(value, float):
        any_value = PB2AnyValue(double_value=value)
    elif isinstance(value, abc.Sequence):
        any_value = PB2AnyValue(array_value=value)
    elif isinstance(value, abc.Mapping):
        any_value = PB2AnyValue(kvlist_value=value)
    else:
        raise Exception(
            "Invalid type {} of value {}".format(type(value), value)
        )
    return PB2KeyValue(key=key, value=any_value)
