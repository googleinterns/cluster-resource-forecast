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
from map_to_schema import MapVMSampleToSchema


class MapVMSampleToSchemaTest(unittest.TestCase):
    def test_any_field_present(self):

        any_field_present = [{"start_time": 10}]
        correct_output = [
            {
                "time": 10,
                "info": {
                    "unique_id": "None-None",
                    "collection_id": None,
                    "instance_index": None,
                    "priority": -1,
                    "scheduling_class": -1,
                    "machine_id": None,
                    "alloc_collection_id": -1,
                    "alloc_instance_index": -100,
                    "collection_type": -1,
                },
                "metrics": {
                    "avg_cpu_usage": -1,
                    "avg_memory_usage": -1,
                    "max_cpu_usage": -1,
                    "max_memory_usage": -1,
                    "random_sample_cpu_usage": -1,
                    "assigned_memory": -1,
                    "sample_rate": -1,
                    "p0_cpu_usage": -1,
                    "p10_cpu_usage": -1,
                    "p20_cpu_usage": -1,
                    "p30_cpu_usage": -1,
                    "p40_cpu_usage": -1,
                    "p50_cpu_usage": -1,
                    "p60_cpu_usage": -1,
                    "p70_cpu_usage": -1,
                    "p80_cpu_usage": -1,
                    "p90_cpu_usage": -1,
                    "p91_cpu_usage": -1,
                    "p92_cpu_usage": -1,
                    "p93_cpu_usage": -1,
                    "p94_cpu_usage": -1,
                    "p95_cpu_usage": -1,
                    "p96_cpu_usage": -1,
                    "p97_cpu_usage": -1,
                    "p98_cpu_usage": -1,
                    "p99_cpu_usage": -1,
                    "memory_limit": -1,
                    "cpu_limit": -1,
                },
                "abstract_metrics": {"usage": 0, "limit": 0},
            }
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vm_sample = p | "Create any field present test input" >> beam.Create(
                any_field_present
            )
            output = input_vm_sample | "Apply MapVMSampleToSchema Transform" >> beam.Map(
                MapVMSampleToSchema
            )

            assert_that(output, equal_to(correct_output))

    def test_empty_input(self):

        empty_input = []
        correct_output = []

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vm_sample = p | "Create empty test input" >> beam.Create(empty_input)
            output = input_vm_sample | "Apply MapVMSampleToSchema Transform" >> beam.Map(
                MapVMSampleToSchema
            )

            assert_that(output, equal_to(correct_output))

    def test_all_fields_present(self):
        all_fields_present = [
            {
                "start_time": 10,
                "end_time": 15,
                "collection_id": 1,
                "instance_index": 2,
                "machine_id": 3,
                "alloc_collection_id": 4,
                "alloc_instance_index": 5,
                "collection_type": 5,
                "average_usage": {"cpus": 0.7, "memory": 8},
                "maximum_usage": {"cpus": 0.9, "memory": 10},
                "random_sample_usage": {"cpus": 0.11, "memory": 12},
                "assigned_memory": 13,
                "sample_rate": 17,
                "cpu_usage_distribution": [
                    0,
                    0.1,
                    0.2,
                    0.3,
                    0.4,
                    0.5,
                    0.6,
                    0.7,
                    0.8,
                    0.9,
                    1,
                ],
                "tail_cpu_usage_distribution": [
                    0.91,
                    0.92,
                    0.93,
                    0.94,
                    0.95,
                    0.96,
                    0.97,
                    0.98,
                    0.99,
                ],
                "time": 5,
                "scheduling_class": 3,
                "priority": 6,
                "resource_request": {"cpus": 0.6, "memory": 2},
            }
        ]
        correct_output = [
            {
                "time": 10,
                "info": {
                    "unique_id": "1-2",
                    "collection_id": 1,
                    "instance_index": 2,
                    "priority": 6,
                    "scheduling_class": 3,
                    "machine_id": 3,
                    "alloc_collection_id": 4,
                    "alloc_instance_index": 5,
                    "collection_type": 5,
                },
                "metrics": {
                    "avg_cpu_usage": 0.7,
                    "avg_memory_usage": 8,
                    "max_cpu_usage": 0.9,
                    "max_memory_usage": 10,
                    "random_sample_cpu_usage": 0.11,
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
                    "memory_limit": 2,
                    "cpu_limit": 0.6,
                },
                "abstract_metrics": {"usage": 0, "limit": 0},
            }
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vm_sample = p | "Create all fields present test input" >> beam.Create(
                all_fields_present
            )
            output = input_vm_sample | "Apply MapVMSampleToSchema Transform" >> beam.Map(
                MapVMSampleToSchema
            )

            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
