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

import random
import apache_beam as beam


def _ScheduleByMachineID(row):
    row["simulated_machine"] = str(row["sample"]["info"]["machine_id"])
    return row


def _ScheduleByVMID(row):
    row["simulated_machine"] = row["sample"]["info"]["unique_id"]
    return row


def _ScheduleAtRandom(row, num_machines):
    row["simulated_machine"] = random.randint(0, num_machines)
    return row


def SetScheduler(data, configs):
    scheduler = configs.scheduler.WhichOneof("scheduler")

    if scheduler == "by_machine_id":
        scheduled_samples = data | "Scheduling by Machine ID" >> beam.Map(
            _ScheduleByMachineID
        )

    if scheduler == "by_vm_unique_id":
        scheduled_samples = data | "Scheduling by VM ID" >> beam.Map(_ScheduleByVMID)

    if scheduler == "at_random":
        if configs.scheduler.at_random.seed != None:
            random.seed(configs.scheduler.at_random.seed)
        scheduled_samples = data | "Scheduling at Random" >> beam.Map(
            _ScheduleAtRandom, configs.scheduler.at_random.num_machines,
        )

    return data
