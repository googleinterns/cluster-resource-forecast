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


from simulator.predictor import StatefulPredictor
from collections import deque
import numpy as np
import statistics


class _State:
    def __init__(self, num_history_samples):
        self.num_history_samples = num_history_samples
        self.usage = deque(maxlen=self.num_history_samples)
        self.limit = deque(maxlen=self.num_history_samples)


class NSigmaPredictor(StatefulPredictor):
    def __init__(self, config):
        super().__init__(config)
        self.num_history_samples = config.num_history_samples
        self.cap_to_limit = config.cap_to_limit
        self.n = config.n

    def CreateState(self, vm_info):
        return _State(self.num_history_samples)

    def UpdateState(self, vm_measure, vm_state):
        limit = vm_measure["sample"]["abstract_metrics"]["limit"]
        usage = vm_measure["sample"]["abstract_metrics"]["usage"]
        if self.cap_to_limit == True:
            usage = min(usage, limit)
        vm_state.usage.appendleft(usage)
        vm_state.limit.appendleft(limit)

    def Predict(self, vm_states_and_num_samples):

        total_normalized_usage = []
        for idx in range(self.num_history_samples):
            total_limit = 0
            total_usage = 0
            num_vms = 0
            for vm_state_and_num_sample in vm_states_and_num_samples:
                if idx < len(vm_state_and_num_sample.vm_state.usage):
                    total_usage += vm_state_and_num_sample.vm_state.usage[idx]
                    total_limit += vm_state_and_num_sample.vm_state.limit[idx]
                    num_vms += 1
            usage_to_limit_ratio = 0
            if num_vms == 0:
                continue
            if total_limit > 0:
                usage_to_limit_ratio = total_usage / total_limit
            total_normalized_usage.append(usage_to_limit_ratio)
        mean = np.mean(total_normalized_usage)
        standard_deviation = statistics.stdev(total_normalized_usage)
        predicted_peak = min(1.0, mean + self.n * standard_deviation)
        current_total_limit = 0
        for vm_state_and_num_sample in vm_states_and_num_samples:
            if len(vm_state_and_num_sample.vm_state.limit) > 0:
                current_total_limit += vm_state_and_num_sample.vm_state.limit[0]
        return predicted_peak * current_total_limit
