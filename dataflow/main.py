# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from classes.options import DataflowOptions
from classes.predicter import Predicter
def run(argv=None):
    pipeline_options = PipelineOptions()
    dataflow_options = pipeline_options.view_as(DataflowOptions)
    with beam.Pipeline(options=pipeline_options) as pipeline:
        pipeline | 'Starting' >> beam.Create(["First Step"]) | 'Predicting' >> beam.ParDo(Predicter(dataflow_options))
if __name__ == '__main__':
    logging.getLogger("lead_scoring").setLevel(logging.INFO)
    run()