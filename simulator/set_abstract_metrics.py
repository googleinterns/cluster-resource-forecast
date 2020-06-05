# Copyright 2020 Google LLC.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import apache_beam as beam


def SetMemoryMetricAndLimit(row, metric):
    row["sample"]["abstract_metrics"]["usage"] = row["sample"]["metrics"][metric]
    row["sample"]["abstract_metrics"]["limit"] = row["sample"]["metrics"][
        "memory_limit"
    ]
    return row


def SetCPUMetricAndLimit(row, metric, percentile=None):
    if percentile == None:
        row["sample"]["abstract_metrics"]["usage"] = row["sample"]["metrics"][metric]

    else:
        row["sample"]["abstract_metrics"]["usage"] = row["sample"]["metrics"][
            "p{}_cpu_usage".format(percentile)
        ]
    row["sample"]["abstract_metrics"]["limit"] = row["sample"]["metrics"]["cpu_limit"]
    return row


def SetAbstractMetrics(data, configs):
    metric = configs.metric.WhichOneof("metric")

    if metric == None:
        samples_with_abstract_metric = data
    else:
        if metric in [
            "avg_memory_usage",
            "max_memory_usage",
            "random_sample_memory_usage",
            "assigned_memory",
        ]:
            samples_with_abstract_metric = (
                data
                | "Setting Memory Metrics and Limit"
                >> beam.Map(SetMemoryMetricAndLimit, metric)
            )

        else:
            percentile = None
            if metric == "cpu_usage_percentile":
                percentile = configs.metric.cpu_usage_percentile
            samples_with_abstract_metric = (
                data
                | "Setting Memory Metrics and Limit"
                >> beam.Map(SetCPUMetricAndLimit, metric, percentile)
            )

    return samples_with_abstract_metric
