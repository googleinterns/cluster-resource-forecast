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

from simulator.predictor import Predictor
import numpy as np


class AvgDecorator(Predictor):
    def __init__(self, config=None, decorated_predictors=None):
        self.decorated_predictors = decorated_predictors

    def UpdateMeasures(self, snapshot):
        predictions = []
        for predictor in self.decorated_predictors:
            predictions.append(predictor.UpdateMeasures(snapshot))

        return self.Predict(predictions)

    def Predict(self, predictions):

        limits = []
        predicted_peaks = []
        for prediction_limit in predictions:
            limits.append(prediction_limit[0])
            predicted_peaks.append(prediction_limit[1])

        return (np.mean(limits), np.mean(predicted_peaks))
