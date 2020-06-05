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

from set_scheduler import SetScheduler
import config_pb2


def _MinutesToMicroseconds(minutes):
    return minutes * 60 * 1000000


class SetSchedulerTest(unittest.TestCase):
    def test_schedule_by_unique_id(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.scheduler.by_vm_unique_id = True

        vmsample = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 3},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        correct_output = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": "1-2",
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 3},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create test input" >> beam.Create(vmsample)
            output = SetScheduler(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_schedule_by_machine_id(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.scheduler.by_machine_id = True

        vmsample = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 3},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        correct_output = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 3,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 3},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create test input" >> beam.Create(vmsample)
            output = SetScheduler(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))

    def test_random_scheduler(self):
        simulation_config = config_pb2.SimulationConfig()
        simulation_config.scheduler.at_random.num_machines = 100
        simulation_config.scheduler.at_random.seed = 11

        random.seed(simulation_config.scheduler.at_random.seed)
        simulated_machine = random.randint(
            0, simulation_config.scheduler.at_random.num_machines
        )

        vmsample = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": 1,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 3},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        correct_output = [
            {
                "simulated_time": 300_000_000,
                "simulated_machine": simulated_machine,
                "sample": {
                    "time": 300_000_000,
                    "info": {"unique_id": "1-2", "machine_id": 3},
                    "metrics": {"avg_cpu_usage": 0.8,},
                    "abstract_metrics": {"usage": 1, "limit": 1},
                },
            },
        ]

        with TestPipeline(runner=direct_runner.BundleBasedDirectRunner()) as p:
            input_vmsamples = p | "Create test input" >> beam.Create(vmsample)
            output = SetScheduler(input_vmsamples, simulation_config)

            assert_that(output, equal_to(correct_output))


if __name__ == "__main__":
    unittest.main()
