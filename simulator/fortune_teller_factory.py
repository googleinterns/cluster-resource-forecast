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

from simulator.oracle import Oracle


def FortuneTellerFactory(config):
    oracle_or_predictor = config.WhichOneof("teller")
    if oracle_or_predictor == "oracle":
        return Oracle(config)

    if oracle_or_predictor == "predictor":
        return PredictorFactory().CreatePredictor(config.predictor)


class PredictorFactory(object):
    __instance = None

    def __new__(cls):
        if PredictorFactory.__instance is None:
            PredictorFactory.__instance = object.__new__(cls)
            PredictorFactory.__instance.decorator_factories = {}
            PredictorFactory.__instance.concrete_predictor_factories = {}
        return PredictorFactory.__instance

    def RegisterDecorator(self, name, factory_func):
        self.decorator_factories[name] = factory_func

    def RegisterPredictor(self, name, factory_func):
        self.concrete_predictor_factories[name] = factory_func

    def CreatePredictor(self, config):
        name = config.WhichOneof("predictor")

        if name in self.concrete_predictor_factories:
            return self.concrete_predictor_factories[name](getattr(config, name))

        elif name in self.decorator_factories:
            decorated_predictors = []
            for decorated_predictor_config in config.decorated_predictors:
                predictor = self.CreatePredictor(decorated_predictor_config)
                decorated_predictors.append(predictor)
            return self.decorator_factories[name](
                getattr(config, name), decorated_predictors
            )

        else:
            assert False, "Requested predictor or decorator not registered. "
