import apache_beam as beam
import random
import math


def _AssignUniqueIDAsKey(row):
    return (row["sample"]["info"]["unique_id"], row)


def _MinutesToMicroseconds(minutes):
    return minutes * 60 * 1000000


def _ResetAndShift(data, lower_bound, upper_bound, seed):
    _, stream = data

    if seed != None:
        random.seed(seed)

    random_shift = _MinutesToMicroseconds(
        5 * (random.randrange(lower_bound, upper_bound,) // 5)
    )

    offset = 0
    for index, sample in enumerate(stream):
        if index == 0:
            offset = sample["simulated_time"]
            sample["simulated_time"] = sample["simulated_time"] - offset + random_shift
        else:
            sample["simulated_time"] = sample["simulated_time"] - offset + random_shift
    return stream


class _SortBySimulatedTime(beam.DoFn):
    def process(self, data):
        key, streams = data
        yield key, sorted(list(streams), key=lambda k: k["simulated_time"])


def ResetAndShiftSimulatedTime(data, configs):

    keyed_data = data | "Assign Unique ID as the Key" >> beam.Map(_AssignUniqueIDAsKey)
    per_VM_groups = keyed_data | "Group Data by Unique ID" >> beam.GroupByKey()
    sorted_data = per_VM_groups | "Sort by Simulated Time before Reset" >> beam.ParDo(
        _SortBySimulatedTime()
    )

    lower_bound = configs.reset_and_shift.random_shift.lower_bound
    upper_bound = configs.reset_and_shift.random_shift.upper_bound
    seed = (
        configs.reset_and_shift.seed
        if configs.reset_and_shift.HasField("seed")
        else None
    )

    shifted_data = sorted_data | "Reset and Shift" >> beam.Map(
        _ResetAndShift, lower_bound, upper_bound, seed
    )
    unpacked_groups = shifted_data | "Ungroup the data" >> beam.FlatMap(
        lambda elements: elements
    )
    return unpacked_groups
