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

import numpy as np


class Oracle:
    def __init__(self, config):
        self.cap_to_limit = config.oracle.cap_to_limit
        self.percentile = min(config.oracle.percentile, 100)

    def UpdateMeasures(self, current_snapshot, future_snapshot):

        current_vm_limits = [
            usage["sample"]["abstract_metrics"]["limit"]
            for usage in vars(current_snapshot)["measures"]
        ]

        if self.cap_to_limit == True:
            current_vm_unique_ids = [
                usage["sample"]["info"]["unique_id"]
                for usage in vars(current_snapshot)["measures"]
            ]
            vm_limits = dict(zip(current_vm_unique_ids, current_vm_limits))

        current_total_limit = sum(current_vm_limits)

        future_total_usages = []
        for snapshot in future_snapshot:
            usages_in_snapshot = [
                usage["sample"]["abstract_metrics"]["usage"]
                for usage in vars(snapshot)["measures"]
            ]

            if self.cap_to_limit == True:
                unique_ids_snapshot = [
                    usage["sample"]["info"]["unique_id"]
                    for usage in vars(snapshot)["measures"]
                ]
                vm_usages = dict(zip(unique_ids_snapshot, usages_in_snapshot))
                usages_in_snapshot = [
                    vm_usages[key]
                    if vm_usages[key] < vm_limits[key]
                    else vm_limits[key]
                    for key in vm_usages.keys()
                ]

            future_total_usages.append(sum(usages_in_snapshot))

        predicted_peak = np.nanpercentile(
            np.array(future_total_usages), self.percentile
        )

        if np.isnan(predicted_peak) == True:
            predicted_peak = current_total_limit

        return (predicted_peak, current_total_limit)
