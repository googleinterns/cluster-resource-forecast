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

import datetime
import apache_beam as beam
import numpy as np
from collections import deque
from typing import List
from simulator.fortune_teller_factory import FortuneTellerFactory
import sys
import json


def _AssignSimulatedMachineIDAsKey(row):
    return (str(row["simulated_machine"]), row)


def _AssignSimulatedTimeAsKey(row):
    return (str(row["simulated_time"]), row)


def _MinutesToMicroseconds(minutes):
    return minutes * 60 * 1_000_000


def _SecondsToMicroseconds(seconds):
    return seconds * 1_000_000


class _Snapshot:
    def __init__(self, time, measures):
        self.time = time
        self.measures = measures


def _FilterFutureSnapshot(current_snapshot, future_snapshot):
    current_VMs_list = [
        item["sample"]["info"]["unique_id"]
        for item in vars(current_snapshot)["measures"]
    ]
    filtered_future_snapshot = []
    for item in future_snapshot:
        filtered_measures = []
        for element in vars(item)["measures"]:
            if element["sample"]["info"]["unique_id"] in current_VMs_list:
                filtered_measures.append(element)
        filtered_future_snapshot.append(
            _Snapshot(vars(item)["time"], filtered_measures)
        )
    return current_snapshot, filtered_future_snapshot


class _FortuneTeller:
    def __init__(self, config):
        self.config = config
        self.name = self.config.name
        self.save_samples = self.config.save_samples
        self.teller = FortuneTellerFactory(self.config)
        self.horizon = (
            self.config.oracle.horizon_in_seconds
            if self.config.WhichOneof("teller") == "oracle"
            else 0
        )

    def UpdateMeasures(self, current_snapshot, future_snapshot):

        prediction_and_limit = (
            self.teller.UpdateMeasures(current_snapshot, future_snapshot)
            if self.config.WhichOneof("teller") == "oracle"
            else self.teller.UpdateMeasures(current_snapshot)
        )

        simulated_time = vars(current_snapshot)["measures"][0]["simulated_time"]
        simulated_machine = vars(current_snapshot)["measures"][0]["simulated_machine"]
        current_total_usage = sum(
            [
                usage["sample"]["abstract_metrics"]["usage"]
                for usage in vars(current_snapshot)["measures"]
            ]
        )

        simulation_result = {
            "fortune_teller_name": self.name,
            "simulated_time": simulated_time,
            "simulated_machine": simulated_machine,
            "predicted_peak": prediction_and_limit[0],
            "total_usage": current_total_usage,
            "limit": prediction_and_limit[1],
        }

        if self.save_samples == True:
            simulation_result["samples"] = vars(current_snapshot)["measures"]

        return simulation_result


def _FortuneTellerRunner(data, fortune_teller):
    _, streams = data

    running_measures = []
    results = []
    cache = deque()
    streams = list(streams)
    streams.append(
        {"simulated_time": np.inf, "simulated_machine": None, "sample": None}
    )

    TIME_STEP_IN_SEC = 300

    for index, sample in enumerate(streams):
        if index == 0:
            current_time = sample["simulated_time"]
            horizon = current_time + _SecondsToMicroseconds(fortune_teller.horizon)

        if sample["simulated_time"] <= current_time:
            running_measures.append(sample)
        else:
            snapshot = _Snapshot(current_time, running_measures)
            cache.append(snapshot)
            running_measures = []
            running_measures.append(sample)
            current_time = sample["simulated_time"]

        if sample["simulated_time"] > horizon:
            current_snapshot = cache.popleft()
            future_snapshot = list(cache)
            current_snapshot, future_snapshot = _FilterFutureSnapshot(
                current_snapshot, future_snapshot
            )

            simulation_result = fortune_teller.UpdateMeasures(
                current_snapshot=current_snapshot, future_snapshot=future_snapshot
            )
            horizon = vars(current_snapshot)["measures"][0][
                "simulated_time"
            ] + _SecondsToMicroseconds(fortune_teller.horizon + TIME_STEP_IN_SEC)
            results.append(simulation_result)

    return results


class _SortBySimulatedTime(beam.DoFn):
    def process(self, data):
        key, streams = data
        yield key, sorted(list(streams), key=lambda k: k["simulated_time"])


def CallFortuneTellerRunner(data, config):

    keyed_data = data | "Assign Simulated Machine ID to Data" >> beam.Map(
        _AssignSimulatedMachineIDAsKey
    )

    grouped_data = (
        keyed_data | "Group by Machine ID before Sorting" >> beam.GroupByKey()
    )

    sorted_data = grouped_data | "Sort by Simulated Time" >> beam.ParDo(
        _SortBySimulatedTime()
    )

    results_schema_without_samples_file = open(
        "simulator/results_schema_without_samples.json"
    )
    results_schema_without_samples = json.load(results_schema_without_samples_file)
    for fortune_teller_config in config.fortune_teller:

        simulation_results = sorted_data | "Calling Fortune Teller Runner For {}".format(
            fortune_teller_config.name
        ) >> beam.Map(
            _FortuneTellerRunner, _FortuneTeller(fortune_teller_config)
        )

        unpacked_simulation_results = simulation_results | "Ungroup the simulation resultsFor {}".format(
            fortune_teller_config.name
        ) >> beam.FlatMap(
            lambda elements: elements
        )

        simulation_result_dataset = config.simulation_result.dataset
        simulation_result_table = (
            config.simulation_result.table
            if config.simulation_result.HasField("table")
            else fortune_teller_config.name
        )

        if fortune_teller_config.save_samples == True:
            # TODO: add support for saving samples
            pass
        else:
            unpacked_simulation_results | "Save {} results to BQ table".format(
                fortune_teller_config.name
            ) >> beam.io.WriteToBigQuery(
                "{}.{}".format(simulation_result_dataset, simulation_result_table,),
                schema=results_schema_without_samples,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
