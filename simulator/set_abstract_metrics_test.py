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

import unittest
import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.runners.direct import direct_runner
from apache_beam.testing.test_pipeline import TestPipeline
from set_abstract_metrics import SetAbstractMetrics
import config_pb2


class JoinUsageAndEventTest(unittest.TestCase):
    def setUp(self):
        self.vmsample = [
            {
                "simulated_time": 1,
                "simulated_machine": 1,
                "sample": {
                    "time": 300000000,
                    "info": {
                        "unique_id": "1-2",
                        "collection_id": 1,
                        "instance_index": 2,
                        "priority": 6,
                        "scheduling_class": 3,
                        "machine_id": 3,
                        "alloc_collection_id": 0,
                        "alloc_instance_index": 5,
                        "collection_type": 0,
                    },
                    "metrics": {
                        "avg_cpu_usage": 0.8,
                        "avg_memory_usage": 8,
                        "max_cpu_usage": 0.1,
                        "max_memory_usage": 0.1,
                        "random_sample_cpu_usage": 0.11,
                        "random_sample_memory_usage": 12,
                        "assigned_memory": 13,
                        "sample_rate": 17,
                        "p0_cpu_usage": 0,
                        "p10_cpu_usage": 0.1,
                        "p20_cpu_usage": 0.2,
                        "p30_cpu_usage": 0.3,
                        "p40_cpu_usage": 0.4,
                        "p50_cpu_usage": 0.5,
                        "p60_cpu_usage": 0.6,
                        "p70_cpu_usage": 0.7,
                        "p80_cpu_usage": 0.8,
                        "p90_cpu_usage": 0.9,
                        "p91_cpu_usage": 0.91,
                        "p92_cpu_usage": 0.92,
                        "p93_cpu_usage": 0.93,
                        "p94_cpu_usage": 0.94,
                        "p95_cpu_usage": 0.95,
                        "p96_cpu_usage": 0.96,
                        "p97_cpu_usage": 0.97,
                        "p98_cpu_usage": 0.98,
                        "p99_cpu_usage": 0.99,
                        "memory_limit": 0.8,
                        "cpu_limit": 0.6,
                    },
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            }
        ]

    def test_setting_memory_metric(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.metric.max_memory_usage = True
        correct_output = [
            {
                "simulated_time": 1,
                "simulated_machine": 1,
                "sample": {
                    "time": 300000000,
                    "info": {
                        "unique_id": "1-2",
                        "collection_id": 1,
                        "instance_index": 2,
                        "priority": 6,
                        "scheduling_class": 3,
                        "machine_id": 3,
                        "alloc_collection_id": 0,
                        "alloc_instance_index": 5,
                        "collection_type": 0,
                    },
                    "metrics": {
                        "avg_cpu_usage": 0.8,
                        "avg_memory_usage": 8,
                        "max_cpu_usage": 0.1,
                        "max_memory_usage": 0.1,
                        "random_sample_cpu_usage": 0.11,
                        "random_sample_memory_usage": 12,
                        "assigned_memory": 13,
                        "sample_rate": 17,
                        "p0_cpu_usage": 0,
                        "p10_cpu_usage": 0.1,
                        "p20_cpu_usage": 0.2,
                        "p30_cpu_usage": 0.3,
                        "p40_cpu_usage": 0.4,
                        "p50_cpu_usage": 0.5,
                        "p60_cpu_usage": 0.6,
                        "p70_cpu_usage": 0.7,
                        "p80_cpu_usage": 0.8,
                        "p90_cpu_usage": 0.9,
                        "p91_cpu_usage": 0.91,
                        "p92_cpu_usage": 0.92,
                        "p93_cpu_usage": 0.93,
                        "p94_cpu_usage": 0.94,
                        "p95_cpu_usage": 0.95,
                        "p96_cpu_usage": 0.96,
                        "p97_cpu_usage": 0.97,
                        "p98_cpu_usage": 0.98,
                        "p99_cpu_usage": 0.99,
                        "memory_limit": 0.8,
                        "cpu_limit": 0.6,
                    },
                    "abstract_metrics": {"usage": 0.1, "limit": 0.8},
                },
            }
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsample = p | "Create test input" >> beam.Create(self.vmsample)
            output = SetAbstractMetrics(input_vmsample, simulation_config)
            assert_that(output, equal_to(correct_output))

    def test_setting_cpu_metric(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.metric.avg_cpu_usage = True
        correct_output = [
            {
                "simulated_time": 1,
                "simulated_machine": 1,
                "sample": {
                    "time": 300000000,
                    "info": {
                        "unique_id": "1-2",
                        "collection_id": 1,
                        "instance_index": 2,
                        "priority": 6,
                        "scheduling_class": 3,
                        "machine_id": 3,
                        "alloc_collection_id": 0,
                        "alloc_instance_index": 5,
                        "collection_type": 0,
                    },
                    "metrics": {
                        "avg_cpu_usage": 0.8,
                        "avg_memory_usage": 8,
                        "max_cpu_usage": 0.1,
                        "max_memory_usage": 0.1,
                        "random_sample_cpu_usage": 0.11,
                        "random_sample_memory_usage": 12,
                        "assigned_memory": 13,
                        "sample_rate": 17,
                        "p0_cpu_usage": 0,
                        "p10_cpu_usage": 0.1,
                        "p20_cpu_usage": 0.2,
                        "p30_cpu_usage": 0.3,
                        "p40_cpu_usage": 0.4,
                        "p50_cpu_usage": 0.5,
                        "p60_cpu_usage": 0.6,
                        "p70_cpu_usage": 0.7,
                        "p80_cpu_usage": 0.8,
                        "p90_cpu_usage": 0.9,
                        "p91_cpu_usage": 0.91,
                        "p92_cpu_usage": 0.92,
                        "p93_cpu_usage": 0.93,
                        "p94_cpu_usage": 0.94,
                        "p95_cpu_usage": 0.95,
                        "p96_cpu_usage": 0.96,
                        "p97_cpu_usage": 0.97,
                        "p98_cpu_usage": 0.98,
                        "p99_cpu_usage": 0.99,
                        "memory_limit": 0.8,
                        "cpu_limit": 0.6,
                    },
                    "abstract_metrics": {"usage": 0.8, "limit": 0.6},
                },
            }
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsample = p | "Create test input" >> beam.Create(self.vmsample)
            output = SetAbstractMetrics(input_vmsample, simulation_config)
            assert_that(output, equal_to(correct_output))

    def test_setting_percentile_metric(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.metric.cpu_usage_percentile = 90
        correct_output = [
            {
                "simulated_time": 1,
                "simulated_machine": 1,
                "sample": {
                    "time": 300000000,
                    "info": {
                        "unique_id": "1-2",
                        "collection_id": 1,
                        "instance_index": 2,
                        "priority": 6,
                        "scheduling_class": 3,
                        "machine_id": 3,
                        "alloc_collection_id": 0,
                        "alloc_instance_index": 5,
                        "collection_type": 0,
                    },
                    "metrics": {
                        "avg_cpu_usage": 0.8,
                        "avg_memory_usage": 8,
                        "max_cpu_usage": 0.1,
                        "max_memory_usage": 0.1,
                        "random_sample_cpu_usage": 0.11,
                        "random_sample_memory_usage": 12,
                        "assigned_memory": 13,
                        "sample_rate": 17,
                        "p0_cpu_usage": 0,
                        "p10_cpu_usage": 0.1,
                        "p20_cpu_usage": 0.2,
                        "p30_cpu_usage": 0.3,
                        "p40_cpu_usage": 0.4,
                        "p50_cpu_usage": 0.5,
                        "p60_cpu_usage": 0.6,
                        "p70_cpu_usage": 0.7,
                        "p80_cpu_usage": 0.8,
                        "p90_cpu_usage": 0.9,
                        "p91_cpu_usage": 0.91,
                        "p92_cpu_usage": 0.92,
                        "p93_cpu_usage": 0.93,
                        "p94_cpu_usage": 0.94,
                        "p95_cpu_usage": 0.95,
                        "p96_cpu_usage": 0.96,
                        "p97_cpu_usage": 0.97,
                        "p98_cpu_usage": 0.98,
                        "p99_cpu_usage": 0.99,
                        "memory_limit": 0.8,
                        "cpu_limit": 0.6,
                    },
                    "abstract_metrics": {"usage": 0.9, "limit": 0.6},
                },
            }
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsample = p | "Create test input" >> beam.Create(self.vmsample)
            output = SetAbstractMetrics(input_vmsample, simulation_config)
            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
