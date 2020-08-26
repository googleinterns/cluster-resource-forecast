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

import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery
import json

from simulator.pair_instance_event_to_usage import JoinUsageAndEvent
from simulator.map_to_schema import MapVMSampleToSchema


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the simulator pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        required=True,
        help="Input file containing SQL queries for usage and events tables.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        required=True,
        help=(
            "Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE."
        ),
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    p = beam.Pipeline(options=pipeline_options)

    queries_file = open("{}".format(known_args.input), "r")
    queries = queries_file.readlines()
    usage_query = queries[0]
    event_query = queries[1]

    input_usage = p | "Query Usage Table" >> beam.io.Read(
        beam.io.BigQuerySource(query=usage_query, use_standard_sql=True)
    )
    input_event = p | "Query Event Table" >> beam.io.Read(
        beam.io.BigQuerySource(query=event_query, use_standard_sql=True)
    )

    usage_event_stream = JoinUsageAndEvent(input_usage, input_event)
    final_table = usage_event_stream | "Map Joined Data to Schema" >> beam.Map(
        MapVMSampleToSchema
    )

    f = open("table_schema.json")
    table_schema = json.loads(f.read())

    final_table | beam.io.WriteToBigQuery(
        known_args.output,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
    )

    result = p.run()
    result.wait_until_finish()


if __name__ == "__main__":
    run()
