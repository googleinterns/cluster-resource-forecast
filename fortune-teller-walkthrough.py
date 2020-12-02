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

from __future__ import absolute_import
from past.builtins import unicode

from google.protobuf import text_format 
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
import json
import logging

from simulator.align_by_time import AlignByTime
from simulator.set_abstract_metrics import SetAbstractMetrics
from simulator.filter_vmsample import FilterVMSample
from simulator.reset_and_shift_simulated_time import ResetAndShiftSimulatedTime
from simulator.set_scheduler import SetScheduler
from simulator.fortune_teller import CallFortuneTellerRunner
from simulator.fortune_teller_factory import PredictorFactory
from simulator.config_pb2 import SimulationConfig
from simulator.avg_predictor import AvgPredictor
from simulator.max_predictor import MaxPredictor
from simulator.per_vm_percentile_predictor import PerVMPercentilePredictor
from simulator.per_machine_percentile_predictor import PerMachinePercentilePredictor
from simulator.n_sigma_predictor import NSigmaPredictor
from simulator.limit_predictor import LimitPredictor


def main(argv=None, save_main_session=True):
    """Main entry point; defines and runs the simulator pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config_file",
        dest="config_file",
        required=True,
        help="Config file based on config.proto.",
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline = beam.Pipeline(options=pipeline_options)

    f = open(known_args.config_file, "rb")
    configs = SimulationConfig()
    text_format.Parse(f.read(), configs)
    f.close()

    # Read Input Data
    input_data_query = "SELECT * FROM {}.{} ".format(
        configs.input.dataset, configs.input.table
    )
    input_data = pipeline | "Query Usage Table" >> beam.io.Read(
        beam.io.BigQuerySource(query=input_data_query, use_standard_sql=True)
    )

    # Filter VMSamples
    filtered_samples = FilterVMSample(input_data, configs)

    if configs.filtered_samples.HasField("input"):
        pipeline = beam.Pipeline(options=pipeline_options)
        filtered_samples_query = "SELECT * FROM {}.{}".format(
            configs.filtered_samples.input.dataset, configs.filtered_samples.input.table
        )
        filtered_samples = pipeline | "Query Filtered Samples Table" >> beam.io.Read(
            beam.io.BigQuerySource(query=filtered_samples_query, use_standard_sql=True)
        )

    # Time Align Samples
    time_aligned_samples = AlignByTime(filtered_samples)

    if configs.time_aligned_samples.HasField("input"):
        pipeline = beam.Pipeline(options=pipeline_options)
        time_aligned_samples_query = "SELECT * FROM {}.{}".format(
            configs.time_aligned_samples.input.dataset,
            configs.time_aligned_samples.input.table,
        )
        time_aligned_samples = (
            pipeline
            | "Query Time Aligned Samples Table"
            >> beam.io.Read(
                beam.io.BigQuerySource(
                    query=time_aligned_samples_query, use_standard_sql=True
                )
            )
        )

    # Setting Abstract Metrics
    samples_with_abstract_metrics = SetAbstractMetrics(time_aligned_samples, configs)

    if configs.samples_with_abstract_metrics.HasField("input"):
        pipeline = beam.Pipeline(options=pipeline_options)
        samples_with_abstract_metrics_query = "SELECT * FROM {}.{}".format(
            configs.samples_with_abstract_metrics.input.dataset,
            configs.samples_with_abstract_metrics.input.table,
        )
        samples_with_abstract_metrics = (
            pipeline
            | "Query Samples With Abstract Metrics Table"
            >> beam.io.Read(
                beam.io.BigQuerySource(
                    query=samples_with_abstract_metrics_query, use_standard_sql=True
                )
            )
        )

    # Resetting and Shifting
    if configs.reset_and_shift.reset_time_to_zero == True:
        samples_with_reset_and_shift = ResetAndShiftSimulatedTime(
            samples_with_abstract_metrics, configs
        )
    else:
        samples_with_reset_and_shift = samples_with_abstract_metrics

    if configs.samples_with_reset_and_shift.HasField("input"):
        pipeline = beam.Pipeline(options=pipeline_options)
        samples_with_reset_and_shift_query = "SELECT * FROM {}.{}".format(
            configs.samples_with_reset_and_shift.input.dataset,
            configs.samples_with_reset_and_shift.input.table,
        )
        samples_with_reset_and_shift = (
            pipeline
            | "Query Samples With Time Reset and Shift"
            >> beam.io.Read(
                beam.io.BigQuerySource(
                    query=samples_with_reset_and_shift_query, use_standard_sql=True
                )
            )
        )

    # Setting Scheduler
    scheduled_samples = SetScheduler(samples_with_reset_and_shift, configs)

    if configs.scheduled_samples.HasField("input"):
        pipeline = beam.Pipeline(options=pipeline_options)
        scheduled_samples_query = "SELECT * FROM {}.{}".format(
            configs.scheduled_samples.input.dataset,
            configs.scheduled_samples.input.table,
        )
        scheduled_samples = pipeline | "Query Scheduled Samples" >> beam.io.Read(
            beam.io.BigQuerySource(query=scheduled_samples_query, use_standard_sql=True)
        )

    # Calling FortuneTeller Runner
    CallFortuneTellerRunner(scheduled_samples, configs)

    # Saving Filtered Samples
    schema_vmsample_file = open("simulator/schema_vmsample.json")
    schema_vmsample = json.load(schema_vmsample_file)
    if configs.filtered_samples.HasField("output"):
        filtered_samples | "Write Filtered Samples" >> beam.io.WriteToBigQuery(
            "{}.{}".format(
                configs.filtered_samples.output.dataset,
                configs.filtered_samples.output.table,
            ),
            schema=schema_vmsample,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    # Saving Time Aligned Samples
    schema_simulated_vmsample_file = open("simulator/schema_simulated_vmsample.json")
    schema_simulated_vmsample = json.load(schema_simulated_vmsample_file)
    if configs.time_aligned_samples.HasField("output"):
        time_aligned_samples | "Write Time Aligned Samples" >> beam.io.WriteToBigQuery(
            "{}.{}".format(
                configs.time_aligned_samples.output.dataset,
                configs.time_aligned_samples.output.table,
            ),
            schema=schema_simulated_vmsample,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    # Saving Samples with Abstract Metrics
    if configs.samples_with_abstract_metrics.HasField("output"):
        samples_with_abstract_metrics | "Write Samples With Abstract Metrics" >> beam.io.WriteToBigQuery(
            "{}.{}".format(
                configs.samples_with_abstract_metrics.output.dataset,
                configs.samples_with_abstract_metrics.output.table,
            ),
            schema=schema_simulated_vmsample,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    # Saving Samples with Time Reset and Shift
    if configs.samples_with_reset_and_shift.HasField("output"):
        samples_with_reset_and_shift | "Write Reset and Shifted Samples" >> beam.io.WriteToBigQuery(
            "{}.{}".format(
                configs.samples_with_reset_and_shift.output.dataset,
                configs.samples_with_reset_and_shift.output.table,
            ),
            schema=schema_simulated_vmsample,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    # Saving Scheduled Samples
    if configs.scheduled_samples.HasField("output"):
        scheduled_samples | "Write Scheduled Samples" >> beam.io.WriteToBigQuery(
            "{}.{}".format(
                configs.scheduled_samples.output.dataset,
                configs.scheduled_samples.output.table,
            ),
            schema=schema_simulated_vmsample,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == "__main__":
    PredictorFactory().RegisterPredictor(
        "per_vm_percentile_predictor", lambda config: PerVMPercentilePredictor(config)
    )
    PredictorFactory().RegisterPredictor(
        "per_machine_percentile_predictor",
        lambda config: PerMachinePercentilePredictor(config),
    )
    PredictorFactory().RegisterPredictor(
        "n_sigma_predictor", lambda config: NSigmaPredictor(config)
    )
    PredictorFactory().RegisterPredictor(
        "max_predictor", lambda config: MaxPredictor(config)
    )
    PredictorFactory().RegisterPredictor(
        "limit_predictor", lambda config: LimitPredictor(config)
    )
    logging.getLogger().setLevel(logging.INFO)
    main()
