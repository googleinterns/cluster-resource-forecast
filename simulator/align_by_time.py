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
import numpy as np


def _MinutesToMicroseconds(minutes):
    return minutes * 60 * 1000000


def _AssignUniqueIDAndFlooredTimeAsKey(row):
    return (
        str(row["info"]["unique_id"])
        + "-"
        + str(
            _MinutesToMicroseconds(
                int(np.floor(row["time"] / _MinutesToMicroseconds(5)))
            )
        ),
        row,
    )


class _PickMaxRecord(beam.DoFn):
    def process(self, data):
        _, streams = data

        time_dicts = []
        info_dicts = []
        metrics_dicts = []
        abstract_metrics_dicts = []
        for d in streams:
            time_dicts.append(d["time"])
            info_dicts.append(d["info"])
            metrics_dicts.append(d["metrics"])
            abstract_metrics_dicts.append(d["abstract_metrics"])
        vm_sample = {
            "time": time_dicts[0],
            "info": info_dicts[0],
            "metrics": {
                k: np.nanmax(
                    np.nan_to_num(
                        np.array([d[k] for d in metrics_dicts], dtype=np.float64)
                    )
                )
                for k in metrics_dicts[0]
            },
            "abstract_metrics": {
                k: max([d[k] for d in abstract_metrics_dicts])
                for k in abstract_metrics_dicts[0]
            },
        }
        return [vm_sample]


def _VMSampleToSimulatedSample(vm_sample):
    simulated_sample = {
        "simulated_time": _MinutesToMicroseconds(5)
        * int(np.floor(vm_sample["time"] / _MinutesToMicroseconds(5))),
        "simulated_machine": str(vm_sample["info"]["machine_id"]),
        "sample": vm_sample,
    }
    return simulated_sample


def AlignByTime(data):
    keyed_data = data | "Flooring time" >> beam.Map(_AssignUniqueIDAndFlooredTimeAsKey)
    five_minute_groups = keyed_data | "Group Data by Keys" >> beam.GroupByKey()
    max_record = five_minute_groups | "Pick Max Record in 5 Minutes" >> beam.ParDo(
        _PickMaxRecord()
    )
    simulated_sample = max_record | "Change VMSample to SimulatedSammple" >> beam.Map(
        _VMSampleToSimulatedSample
    )
    return simulated_sample
