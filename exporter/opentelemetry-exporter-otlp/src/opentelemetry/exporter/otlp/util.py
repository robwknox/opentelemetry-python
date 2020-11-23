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

import enum
from typing import Dict, Union

HeadersInput = Union[Dict[str, str], str, None]
Headers = Dict[str, str]


class ExporterType(enum.Enum):
    METRIC = "METRIC"
    SPAN = "SPAN"


class Protocol(enum.Enum):
    GRPC = "grpc"
    HTTP_PROTOBUF = "http/protobuf"


class Compression(enum.Enum):
    DEFLATE = "deflate"
    GZIP = "gzip"
    NONE = "none"
