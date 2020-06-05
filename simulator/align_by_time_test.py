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
from align_by_time import AlignByTime


class AlignByTimeTest(unittest.TestCase):
    def test_align_time(self):
        multiple_vmsamples = [
            {
                "time": 300_000_000,
                "info": {"unique_id": "1-2", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
            {
                "time": 305_000_000,
                "info": {"unique_id": "1-2", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
        ]

        correct_output = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 1},
                    "metrics": {
                        "avg_cpu_usage": 0.5,
                        "avg_memory_usage": 0.2,
                        "memory_limit": 0.4,
                        "cpu_limit": 0.8,
                    },
                    "abstract_metrics": {"usage": 1.0, "limit": 1.0},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create multiple samples input" >> beam.Create(
                multiple_vmsamples
            )
            output = AlignByTime(input_vmsamples)
            assert_that(output, equal_to(correct_output))

    def test_pick_max_record_and_align(self):

        multiple_vmsamples = [
            {
                "time": 300_000_000,
                "info": {"unique_id": "1-2", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
            {
                "time": 305_000_000,
                "info": {"unique_id": "1-2", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.6,
                    "avg_memory_usage": 0.1,
                    "memory_limit": 0.5,
                    "cpu_limit": 0.7,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
        ]

        correct_output = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 1},
                    "metrics": {
                        "avg_cpu_usage": 0.6,
                        "avg_memory_usage": 0.2,
                        "memory_limit": 0.5,
                        "cpu_limit": 0.8,
                    },
                    "abstract_metrics": {"usage": 1.0, "limit": 1.0},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create multiple samples input" >> beam.Create(
                multiple_vmsamples
            )
            output = AlignByTime(input_vmsamples)
            assert_that(output, equal_to(correct_output))

    def test_align_vms_seperately(self):

        multiple_vmsamples = [
            {
                "time": 300_000_000,
                "info": {"unique_id": "1-2", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
            {
                "time": 305_000_000,
                "info": {"unique_id": "1-2", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.6,
                    "avg_memory_usage": 0.1,
                    "memory_limit": 0.5,
                    "cpu_limit": 0.7,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
            {
                "time": 300_000_000,
                "info": {"unique_id": "1-3", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
            {
                "time": 305_000_000,
                "info": {"unique_id": "1-3", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
        ]

        correct_output = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 1},
                    "metrics": {
                        "avg_cpu_usage": 0.6,
                        "avg_memory_usage": 0.2,
                        "memory_limit": 0.5,
                        "cpu_limit": 0.8,
                    },
                    "abstract_metrics": {"usage": 1.0, "limit": 1.0},
                },
            },
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-3", "machine_id": 1},
                    "metrics": {
                        "avg_cpu_usage": 0.5,
                        "avg_memory_usage": 0.2,
                        "memory_limit": 0.4,
                        "cpu_limit": 0.8,
                    },
                    "abstract_metrics": {"usage": 1.0, "limit": 1.0},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create multiple samples input" >> beam.Create(
                multiple_vmsamples
            )
            output = AlignByTime(input_vmsamples)
            assert_that(output, equal_to(correct_output))

    def test_already_aligned(self):

        aligned_vmsample = [
            {
                "time": 600_000_000,
                "info": {"unique_id": "1-3", "machine_id": 1,},
                "metrics": {
                    "avg_cpu_usage": 0.5,
                    "avg_memory_usage": 0.2,
                    "memory_limit": 0.4,
                    "cpu_limit": 0.8,
                },
                "abstract_metrics": {"usage": 1.0, "limit": 1.0},
            },
        ]

        correct_output = [
            {
                "simulated_time": 600_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 600_000_000,
                    "info": {"unique_id": "1-3", "machine_id": 1},
                    "metrics": {
                        "avg_cpu_usage": 0.5,
                        "avg_memory_usage": 0.2,
                        "memory_limit": 0.4,
                        "cpu_limit": 0.8,
                    },
                    "abstract_metrics": {"usage": 1.0, "limit": 1.0},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create multiple samples input" >> beam.Create(
                aligned_vmsample
            )
            output = AlignByTime(input_vmsamples)
            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main()
