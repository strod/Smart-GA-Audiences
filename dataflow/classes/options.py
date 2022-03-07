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

from apache_beam.options.pipeline_options import PipelineOptions


class DataflowOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        # OAUTH
        parser.add_value_provider_argument(
            '--name', help='Analysis name',
            default='lead_scoring')
        parser.add_value_provider_argument(
            '--source_table', help='GA export table name with dataset')
        parser.add_value_provider_argument(
            '--destination_table', help='Table with dataset to write the results')
        parser.add_value_provider_argument(
            '--trained_model_table', help='Table with training model data')
        parser.add_value_provider_argument(
            '--load_trained_model', help='Load Trained Model (true/false)', default='false')
        parser.add_value_provider_argument(
            '--prediction_type', help='Prediction type', default='transaction')
        parser.add_value_provider_argument(
            '--event_filter_type', help='Event filter type', default='eventCategory')
        parser.add_value_provider_argument(
            '--event_filter_value', help='Event filter value', default='filterValue')
        parser.add_value_provider_argument(
            '--test_size', help='Test size', default='0.5')
        parser.add_value_provider_argument(
            '--downsample_majority_class', help='Downsample majority class', default='0.01')
        parser.add_value_provider_argument(
            '--medium_class_value', help='Medium class value (0.0 ~ 1.0)', default='0.3')
        parser.add_value_provider_argument(
            '--high_class_value', help='High class value (0.0 ~ 1.0)', default='0.7')
        parser.add_value_provider_argument(
            '--index_dimension', help='Key GA dimension', default='dimension14')
        parser.add_value_provider_argument(
            '--value_dimension', help='Value GA dimension', default='dimension28')
        parser.add_value_provider_argument(
            '--training_windows_start_in_days', help='Number of days to the beggining of the training window',
            default='60')
        parser.add_value_provider_argument(
            '--classification_window_start_in_days',
            help='Number of days to the beggining of the classification window', default='30')
        parser.add_value_provider_argument(
            '--queries_location',
            help='Path in Google Cloud Storage where the queries are stored')
 