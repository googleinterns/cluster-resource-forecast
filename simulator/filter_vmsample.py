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


def RemoveNonTopLevel(row):
    if row:
        return row["info"]["alloc_collection_id"] == 0
    else:
        return row


def VMsInTimeRange(row, lower_bound, upper_bound):
    assert row != {}
    if row:
        return row["time"] >= lower_bound and row["time"] <= upper_bound
    else:
        row


def VMsInPriorityRange(row, lower_bound, upper_bound):
    if row:
        return (
            row["info"]["priority"] >= lower_bound
            and row["info"]["priority"] <= upper_bound
        )
    else:
        row


def VMsInSchedulingClassRange(row, lower_bound, upper_bound):
    if row:
        return (
            row["info"]["scheduling_class"] >= lower_bound
            and row["info"]["scheduling_class"] <= upper_bound
        )
    else:
        row


def FilterVMSample(data, configs):

    if configs.filter.HasField("start_time") and configs.filter.HasField("end_time"):
        vms_in_time_range = data | "Keep VMs within time range" >> beam.Filter(
            VMsInTimeRange, configs.filter.start_time, configs.filter.end_time
        )
    else:
        vms_in_time_range = data

    if configs.filter.remove_non_top_level_vms == True:
        top_level_vms = vms_in_time_range | "Remove Non Top Level VMs" >> beam.Filter(
            RemoveNonTopLevel
        )
    else:
        top_level_vms = vms_in_time_range

    if configs.filter.priority_range.HasField(
        "lower_bound"
    ) and configs.filter.priority_range.HasField("upper_bound"):
        vms_in_priority_range = (
            top_level_vms
            | "Keep VMs within priority range"
            >> beam.Filter(
                VMsInPriorityRange,
                configs.filter.priority_range.lower_bound,
                configs.filter.priority_range.upper_bound,
            )
        )
    else:
        vms_in_priority_range = top_level_vms

    if configs.filter.scheduling_class_range.HasField(
        "lower_bound"
    ) and configs.filter.scheduling_class_range.HasField("upper_bound"):
        vms_in_scheduling_class_range = (
            vms_in_priority_range
            | "Keep VMs within scheduling class range"
            >> beam.Filter(
                VMsInSchedulingClassRange,
                configs.filter.scheduling_class_range.lower_bound,
                configs.filter.scheduling_class_range.upper_bound,
            )
        )
    else:
        vms_in_scheduling_class_range = vms_in_priority_range

    return vms_in_scheduling_class_range
