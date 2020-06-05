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
from pair_instance_event_to_usage import JoinUsageAndEvent


class JoinUsageAndEventTest(unittest.TestCase):
    def test_multiple_usages_multiple_events(self):
        multiple_usages = [
            {
                "start_time": 10,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.5,
            },
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
            },
        ]

        multiple_events = [
            {
                "time": 5,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "time": 17,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.5, "memory": 1},
            },
            {
                "time": 23,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.7, "memory": 0.5},
            },
        ]

        correct_output = [
            {
                "start_time": 10,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.5,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
                "time": 17,
                "resource_request": {"cpus": 0.5, "memory": 1},
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
                "time": 23,
                "resource_request": {"cpus": 0.7, "memory": 0.5},
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create multiple usage test input" >> beam.Create(
                multiple_usages
            )
            input_events = p | "Create multiple event test input" >> beam.Create(
                multiple_events
            )
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))

    def test_multiple_usages_single_events(self):
        multiple_usages = [
            {
                "start_time": 10,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.5,
            },
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
            },
        ]

        single_event = [
            {
                "time": 13,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
        ]

        correct_output = [
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
                "time": 13,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
                "time": 13,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
                "time": 13,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create multiple usage test input" >> beam.Create(
                multiple_usages
            )
            input_events = p | "Create multiple event test input" >> beam.Create(
                single_event
            )
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))

    def test_multiple_usages_no_event(self):
        multiple_usages = [
            {
                "start_time": 10,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.5,
            },
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
            },
        ]

        no_event = []

        correct_output = []

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create multiple usage test input" >> beam.Create(
                multiple_usages
            )
            input_events = p | "Create no event test input" >> beam.Create(no_event)
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))

    def test_no_usage_multiple_events(self):
        no_usage = []

        multiple_events = [
            {
                "time": 5,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "time": 17,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.5, "memory": 1},
            },
            {
                "time": 23,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.7, "memory": 0.5},
            },
        ]

        correct_output = []

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create multiple usage test input" >> beam.Create(
                no_usage
            )
            input_events = p | "Create multiple event test input" >> beam.Create(
                multiple_events
            )
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))

    def test_single_usage_multiple_events(self):
        single_usage = [
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
            },
        ]

        multiple_events = [
            {
                "time": 5,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "time": 17,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0, "memory": 1},
            },
            {
                "time": 23,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.7, "memory": 0.5},
            },
        ]

        correct_output = [
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create standard usage test input" >> beam.Create(
                single_usage
            )
            input_events = p | "Create zero request event test input" >> beam.Create(
                multiple_events
            )
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))

    def test_single_usage_single_events(self):
        single_usage = [
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
            },
        ]

        single_event = [
            {
                "time": 5,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
        ]

        correct_output = [
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            }
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create single usage test input" >> beam.Create(
                single_usage
            )
            input_events = p | "Create single event test input" >> beam.Create(
                single_event
            )
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))

    def test_multiple_usage_zero_cpus_event(self):
        multiple_usages = [
            {
                "start_time": 10,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.5,
            },
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
            },
        ]

        zero_cpus_event = [
            {
                "time": 5,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "time": 17,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0, "memory": 1},
            },
            {
                "time": 23,
                "collection_id": 2,
                "instance_index": 1,
                "resource_request": {"cpus": 0.7, "memory": 0.5},
            },
        ]

        correct_output = [
            {
                "start_time": 10,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.5,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 15,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.4,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 20,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.3,
                "time": 5,
                "resource_request": {"cpus": 0.6, "memory": 2},
            },
            {
                "start_time": 25,
                "collection_id": 2,
                "instance_index": 1,
                "avg_usage.cpus": 0.2,
                "time": 23,
                "resource_request": {"cpus": 0.7, "memory": 0.5},
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_usages = p | "Create standard usage test input" >> beam.Create(
                multiple_usages
            )
            input_events = p | "Create zero request event test input" >> beam.Create(
                zero_cpus_event
            )
            output = JoinUsageAndEvent(input_usages, input_events)

            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main(argv=["first-arg-is-ignored"], exit=False)
