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


class _State:
    def __init__(self):
        self.limit = 0


class LimitPredictor(StatefulPredictor):
    def __init__(self, config):
        super().__init__(config)

    def CreateState(self, vm_info):
        return _State()

    def UpdateState(self, vm_measure, vm_state):
        vm_state.limit = vm_measure["sample"]["abstract_metrics"]["limit"]

    def Predict(self, vm_states_and_num_samples):

        vms_limits = []
        for vm_state_and_num_sample in vm_states_and_num_samples:
            vms_limits.append(vm_state_and_num_sample.vm_state.limit)

        predicted_peak = sum(vms_limits)

        return predicted_peak
