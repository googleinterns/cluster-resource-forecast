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
        self.usage = 0


class MaxPredictor(StatefulPredictor):
    def __init__(self, config, decorated_predictors=None):
        super().__init__(config, decorated_predictors)
        self.decorated_predictors = decorated_predictors
        self.cap_to_limit = config.cap_to_limit

    def CreateState(self, vm_info):
        return _State()

    def UpdateState(self, vm_measure, vm_state):
        vm_state.usage = max(
            vm_measure["sample"]["abstract_metrics"]["usage"], vm_state.usage
        )

    def Predict(self, vm_states_and_num_samples):

        vms_peaks = []
        for vm_state_and_num_sample in vm_states_and_num_samples:
<<<<<<< HEAD
<<<<<<< HEAD
            vms_peaks.append(vm_state_and_num_sample.vm_state.usage)
=======
            vms_avgs.append(vm_state_and_num_sample.vm_state.usage)
>>>>>>> 833bafe... fortune-teller
=======
            vms_peaks.append(vm_state_and_num_sample.vm_state.usage)
>>>>>>> 8cfc3fd... update

        predicted_peak = sum(vms_peaks)

        return predicted_peak
