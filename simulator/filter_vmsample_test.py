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

from filter_vmsample import FilterVMSample
import config_pb2


class FilterVMSampleTest(unittest.TestCase):
    def setUp(self):
        self.multiple_vmsamples = [
            {
                "time": 10,
                "info": {
                    "priority": 2,
                    "scheduling_class": 0,
                    "machine_id": 100,
                    "alloc_collection_id": 0,
                },
            },
            {
                "time": 20,
                "info": {
                    "priority": 4,
                    "scheduling_class": 1,
                    "machine_id": 200,
                    "alloc_collection_id": 1,
                },
            },
            {
                "time": 30,
                "info": {
                    "priority": 6,
                    "scheduling_class": 2,
                    "machine_id": 300,
                    "alloc_collection_id": 0,
                },
            },
            {
                "time": 40,
                "info": {
                    "priority": 8,
                    "scheduling_class": 3,
                    "machine_id": 400,
                    "alloc_collection_id": 1,
                },
            },
        ]

    def test_time_filter(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.filter.start_time = 15
        simulation_config.filter.end_time = 25

        correct_output = [
            {
                "time": 20,
                "info": {
                    "priority": 4,
                    "scheduling_class": 1,
                    "machine_id": 200,
                    "alloc_collection_id": 1,
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create time test input" >> beam.Create(
                self.multiple_vmsamples
            )
            output = FilterVMSample(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_priority_filter(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.filter.priority_range.lower_bound = 3
        simulation_config.filter.priority_range.upper_bound = 7

        correct_output = [
            {
                "time": 20,
                "info": {
                    "priority": 4,
                    "scheduling_class": 1,
                    "machine_id": 200,
                    "alloc_collection_id": 1,
                },
            },
            {
                "time": 30,
                "info": {
                    "priority": 6,
                    "scheduling_class": 2,
                    "machine_id": 300,
                    "alloc_collection_id": 0,
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create priority test input" >> beam.Create(
                self.multiple_vmsamples
            )
            output = FilterVMSample(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_scheduling_class_filter(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.filter.scheduling_class_range.lower_bound = 0
        simulation_config.filter.scheduling_class_range.upper_bound = 1

        correct_output = [
            {
                "time": 10,
                "info": {
                    "priority": 2,
                    "scheduling_class": 0,
                    "machine_id": 100,
                    "alloc_collection_id": 0,
                },
            },
            {
                "time": 20,
                "info": {
                    "priority": 4,
                    "scheduling_class": 1,
                    "machine_id": 200,
                    "alloc_collection_id": 1,
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create scheduling class input" >> beam.Create(
                self.multiple_vmsamples
            )
            output = FilterVMSample(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_top_level_filter(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.filter.remove_non_top_level_vms = True

        correct_output = [
            {
                "time": 10,
                "info": {
                    "priority": 2,
                    "scheduling_class": 0,
                    "machine_id": 100,
                    "alloc_collection_id": 0,
                },
            },
            {
                "time": 30,
                "info": {
                    "priority": 6,
                    "scheduling_class": 2,
                    "machine_id": 300,
                    "alloc_collection_id": 0,
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create top level filter input" >> beam.Create(
                self.multiple_vmsamples
            )
            output = FilterVMSample(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_all_filters(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.filter.remove_non_top_level_vms = True
        simulation_config.filter.priority_range.lower_bound = 1
        simulation_config.filter.priority_range.upper_bound = 7
        simulation_config.filter.scheduling_class_range.lower_bound = 0
        simulation_config.filter.scheduling_class_range.upper_bound = 1
        simulation_config.filter.start_time = 5
        simulation_config.filter.end_time = 15

        correct_output = [
            {
                "time": 10,
                "info": {
                    "priority": 2,
                    "scheduling_class": 0,
                    "machine_id": 100,
                    "alloc_collection_id": 0,
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create all filter input" >> beam.Create(
                self.multiple_vmsamples
            )
            output = FilterVMSample(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main()
