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
import random
import math
import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.runners.direct import direct_runner
from apache_beam.testing.test_pipeline import TestPipeline

from reset_and_shift_simulated_time import ResetAndShiftSimulatedTime
import config_pb2


def _MinutesToMicroseconds(minutes):
    return minutes * 60 * 1000000


class ResetAndShiftSimulatedTimeTest(unittest.TestCase):
    def setUp(self):
        self.simulation_config = config_pb2.SimulationConfig()
        self.simulation_config.reset_and_shift.reset_time_to_zero = True
        self.simulation_config.reset_and_shift.random_shift.lower_bound = 0
        self.simulation_config.reset_and_shift.random_shift.upper_bound = 600
        self.simulation_config.reset_and_shift.seed = 11

        random.seed(self.simulation_config.reset_and_shift.seed)
        self.random_shift = _MinutesToMicroseconds(
            5
            * (
                random.randrange(
                    self.simulation_config.reset_and_shift.random_shift.lower_bound,
                    self.simulation_config.reset_and_shift.random_shift.upper_bound,
                )
                // 5
            )
        )

    def test_ordered_samples(self):

        ordered_vmsamples = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
            {
                "simulated_time": 600_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 600_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.1,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        offset = 300_000_000

        correct_output = [
            {
                "simulated_time": 300_000_000 - offset + self.random_shift,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
            {
                "simulated_time": 600_000_000 - offset + self.random_shift,
                "simulated_machine": 1,
                "sample": {
                    "time": 600_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.1,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create time test input" >> beam.Create(
                ordered_vmsamples
            )
            output = ResetAndShiftSimulatedTime(input_vmsamples, self.simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_unordered_samples(self):

        unordered_vmsamples = [
            {
                "simulated_time": 600_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 600_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.1,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        offset = 300_000_000

        correct_output = [
            {
                "simulated_time": 300_000_000 - offset + self.random_shift,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
            {
                "simulated_time": 600_000_000 - offset + self.random_shift,
                "simulated_machine": 1,
                "sample": {
                    "time": 600_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.1,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create time test input" >> beam.Create(
                unordered_vmsamples
            )
            output = ResetAndShiftSimulatedTime(input_vmsamples, self.simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_samples_with_gap(self):

        vmsamples_with_gap = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
            {
                "simulated_time": 900_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 900_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.1,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        offset = 300_000_000

        correct_output = [
            {
                "simulated_time": 300_000_000 - offset + self.random_shift,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
            {
                "simulated_time": 900_000_000 - offset + self.random_shift,
                "simulated_machine": 1,
                "sample": {
                    "time": 900_000_000,
                    "info": {"unique_id": "1-2",},
                    "metrics": {"avg_cpu_usage": 0.1,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create time test input" >> beam.Create(
                vmsamples_with_gap
            )
            output = ResetAndShiftSimulatedTime(input_vmsamples, self.simulation_config)

            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main()
