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


import datetime
import logging  
import platform

import apache_beam as beam
import numpy as np
import pandas as pd
import sklearn
from sklearn.compose import make_column_transformer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from imblearn.under_sampling import RandomUnderSampler
from xgboost import XGBClassifier
from classes.query_templates import QueryTemplates

import joblib
from io import BytesIO
import base64
import hashlib

LOG_NAME = 'lead_scoring'

class Predicter(beam.DoFn):

    def __init__(self, dataflow_options):
        super().__init__()
        
        self._name = dataflow_options.name
        self._source_table = dataflow_options.source_table
        self._destination_table = dataflow_options.destination_table
        self._index_dimension = dataflow_options.index_dimension
        self._value_dimension = dataflow_options.value_dimension
        self._training_windows_start_in_days = dataflow_options.training_windows_start_in_days
        self._classification_window_start_in_days = dataflow_options.classification_window_start_in_days
        self._trained_model_table = dataflow_options.trained_model_table
        self._load_trained_model = dataflow_options.load_trained_model
        self._prediction_type = dataflow_options.prediction_type
        self._event_filter_type = dataflow_options.event_filter_type
        self._event_filter_value = dataflow_options.event_filter_value
        self._test_size = dataflow_options.test_size
        self._downsample_majority_class = dataflow_options.downsample_majority_class
        self._medium_class_value = dataflow_options.medium_class_value
        self._high_class_value = dataflow_options.high_class_value

        self._query_templates = QueryTemplates(dataflow_options.queries_location)

    def process(self, elements, **kwargs) -> None:
        logging.getLogger(LOG_NAME).info(f'Starting at {datetime.datetime.now()}')
        logging.getLogger(LOG_NAME).info('This process is composed of 3 stages:\n1. Initialization\n2. Training\n3. Classification')
        pipe_mlp = None


        logging.getLogger(LOG_NAME).info('Step 1: Initialization')
        query_parameters = self.assemble_query_parameters()
        logging.getLogger(LOG_NAME).info(f'Initialization: Finished at {datetime.datetime.now()}.')
        
        logging.getLogger(LOG_NAME).info('Step 2: Training')
        if self._load_trained_model.get() == 'true':
            pipe_mlp = self.read_model_from_bq()
        if pipe_mlp == None:
            pipe_mlp = self.train(query_parameters)
        logging.getLogger(LOG_NAME).info(f'Training: Finished at {datetime.datetime.now()}')

        logging.getLogger(LOG_NAME).info('Step 3: Classification')
        self.predict(query_parameters, pipe_mlp)
        logging.getLogger(LOG_NAME).info(f'Classification: Finished at {datetime.datetime.now()}.')
        logging.getLogger(LOG_NAME).info('Done')

    # 1. Initialization
    def assemble_query_parameters(self):
        today = datetime.date.today()
        classification_window_start = datetime.timedelta(days=int(self._classification_window_start_in_days.get()))
        training_windows_start = datetime.timedelta(days=int(self._training_windows_start_in_days.get()))

        lookback_start_date = today - training_windows_start
        lookback_end_date = today - classification_window_start
        logging.getLogger(LOG_NAME).info(
            f'Initialization: Lookback start date: {lookback_start_date}. Lookback end date: {lookback_end_date}')

        conversion_window_start_date = today - classification_window_start
        conversion_window_end_date = today
        logging.getLogger(LOG_NAME).info(
            f'Initialization: Conversion window start date: {conversion_window_start_date}. Conversion window end date: {conversion_window_end_date}')

        classification_start_date = today - classification_window_start
        classification_end_date = today
        logging.getLogger(LOG_NAME).info(
            f'Initialization: Classification start date: {classification_start_date}. Classification end date: {classification_end_date}')

        query_parameters = {'table': self._source_table.get(),
                            'lookback_start_date': self.format_date(lookback_start_date),
                            'lookback_end_date': self.format_date(lookback_end_date),
                            'conversion_window_start_date': self.format_date(conversion_window_start_date),
                            'conversion_window_end_date': self.format_date(conversion_window_end_date),
                            'prediction_type': self._prediction_type.get(), 'event_filter_type': self._event_filter_type.get(),
                            'event_filter_value': self._event_filter_value.get(),
                            'downsample_majority_class': self._downsample_majority_class.get(),
                            'classification_start_date': self.format_date(classification_start_date),
                            'classification_end_date': self.format_date(classification_end_date)}
        return query_parameters

    # 2. Training
    def train(self, query_parameters):
        logging.getLogger(LOG_NAME).info('Training: Loading training data')  
        training_query = self._query_templates.training_query_template.format(**query_parameters)
        x_train, x_test, y_train, y_test, numbers, text = self.load_training_data(training_query)
        
        logging.getLogger(LOG_NAME).info('Training: Preparing training pipeline')  
        
        # Build and fit the pipeline
        preprocess = make_column_transformer(
            (OneHotEncoder(handle_unknown='ignore'), text),
            (StandardScaler(), numbers))

        # Build and fit the pipeline using Neural Net
        pipe_XGB = make_pipeline(
            preprocess,
            XGBClassifier(eval_metric=['logloss'])
            )

        logging.getLogger(LOG_NAME).info('Training: Starting training')
        pipe_XGB.fit(x_train, y_train)
        logging.getLogger(LOG_NAME).info('Training: Finished training')

        score_train = pipe_XGB.score(x_train, y_train)
        score_test = pipe_XGB.score(x_test, y_test)
        logging.getLogger(LOG_NAME).info(f'Training: Scores: Train={score_train}, Test={score_test}')

        self.write_model_into_bq(pipe_XGB, score_train, score_test)

        return pipe_XGB

    def load_training_data(self, training_query):
        logging.getLogger(LOG_NAME).info('Training: Starting training data query')
        training_dataframe = pd.io.gbq.read_gbq(training_query, verbose=False, dialect='standard',
                                                use_bqstorage_api=True)
        logging.getLogger(LOG_NAME).info('Training: Finished training data query')

        logging.getLogger(LOG_NAME).info(f'Training: Training size {len(training_dataframe)}')

        logging.getLogger(LOG_NAME).info(
            "Training: Dataset has {} rows and {} columns".format(training_dataframe.shape[0], training_dataframe.shape[1]))

        logging.getLogger(LOG_NAME).info(f"Training: Class distribution: {training_dataframe.y_conversions.value_counts()}")

        # Drop the label and clientid (Xs)
        X_all = training_dataframe.drop(['y_conversions', 'clientid'], axis=1)

        # Select the label to predict (Ys)
        y_all = training_dataframe['y_conversions']

        # Get all categorical columns in a list.
        text = list(X_all.select_dtypes(include=['object', 'category']).columns)

        # Get all numeric columns in a list.
        numbers = list(X_all.select_dtypes(include=np.number))

        # Convert categoricals into the proper type
        X_all.loc[:, text] = X_all.loc[:, text].astype('category')
        
        # Balancing the Data Before Stratify it
        rus = RandomUnderSampler(sampling_strategy='majority')
        X_bal, y_bal = rus.fit_resample(X_all, y_all)

        # Stratified split into train, test sets
        x_train, x_test, y_train, y_test = train_test_split(X_bal, y_bal,
                                                            test_size=float(self._test_size.get()),
                                                            random_state=4,
                                                            stratify=y_all)
        
        return x_train, x_test, y_train, y_test, numbers, text


    # 2.1. Write / Read trained model in BigQuery
    def write_model_into_bq(self, model, score_train, score_test):
        logging.getLogger(LOG_NAME).info(f'Training: Writing into BigQuery table {self._trained_model_table.get()}')
        with BytesIO() as tmp_bytes:
            joblib.dump(model, tmp_bytes)
            serialized_model = base64.urlsafe_b64encode(tmp_bytes.getvalue()).decode('ascii')
            df = pd.DataFrame(
                {
                    'timestamp': str(datetime.datetime.now()),
                    'sklearn_version': sklearn.__version__,
                    'python_version': platform.python_version(),
                    'name': self._name.get(),
                    'model': serialized_model,
                    'md5': hashlib.md5(tmp_bytes.getvalue()).hexdigest(),
                    'score_train': score_train,
                    'score_test': score_test
                },
                index=[0]
            )
            df.to_gbq(destination_table=self._trained_model_table.get(), if_exists='append')
            logging.getLogger(LOG_NAME).info(f'Training: Model written into BigQuery table {self._trained_model_table.get()}')

    def read_model_from_bq(self):
        logging.getLogger(LOG_NAME).info(f'Training: Loading model from BigQuery table {self._trained_model_table.get()}')
        model = None
        query_parameters = {
            'table_name': self._trained_model_table.get(),
            'sklearn_version': sklearn.__version__,
            'python_version': platform.python_version(),
            'name': self._name.get()
        }
        with BytesIO() as tmp_bytes:
            query = self._query_templates.trained_model_query_template.format(**query_parameters)
            df = pd.io.gbq.read_gbq(query, verbose=False, dialect='standard',
                                                use_bqstorage_api=True)
            if df.size > 0:
                tmp_bytes.write(base64.urlsafe_b64decode(df.loc[0, 'model'].encode('ascii')))
                md5 = hashlib.md5(tmp_bytes.getvalue()).hexdigest()
                if md5 == df.loc[0, 'md5']:
                    # same binary object, decoding was successful
                    model = joblib.load(tmp_bytes)
                    logging.getLogger(LOG_NAME).info(f'Training: Model loaded successfully.\n- Timestamp: {df.loc[0, "timestamp"]}\n- MD5: {df.loc[0, "md5"]}\n- Scores: Train={df.loc[0, "score_train"]}, Test={df.loc[0, "score_test"]}')
        if model == None:
            logging.getLogger(LOG_NAME).info('Training: Error on loading model from BigQuery')
        return model

    # 3. Classification    
    def predict(self, query_parameters, pipe_mlp):
        logging.getLogger(LOG_NAME).info('Classification: Starting classification')
        classification_query = self._query_templates.classification_query_template.format(**query_parameters)
        classification_dataframe = self.load_classification_data(classification_query)
        predictions = pipe_mlp.predict_proba(classification_dataframe.drop(['clientid'], 1))
        logging.getLogger(LOG_NAME).info('Classification: Finished classification')
        self.upload_to_bq(classification_dataframe, predictions)
        return predictions

    def upload_to_bq(self, classification_dataframe, predictions):
        logging.getLogger(LOG_NAME).info('Classification: Upload results to BigQuery')
        classification_dataframe['prob'] = predictions[:, 1]
        classification_dataframe['segment'] = classification_dataframe.prob.apply(
            lambda x: 'high' if x > float(self._high_class_value.get()) else ('medium' if x > float(self._medium_class_value.get()) else 'low'))

        data_import_format = classification_dataframe.loc[:, ['clientid', 'segment']]
        data_import_format.columns = [self._index_dimension.get(), self._value_dimension.get()]

        logging.getLogger(LOG_NAME).info(f"Classification: Uploading data to BigQuery table {self._destination_table.get()}")
        data_import_format.to_gbq(destination_table=self._destination_table.get(), if_exists='replace')
        logging.getLogger(LOG_NAME).info("Classification: Data uploaded to BigQuery")

    def load_classification_data(self, classification_query):
        logging.getLogger(LOG_NAME).info('Classification: Starting classification data query')

        classification_dataframe = pd.io.gbq.read_gbq(classification_query, verbose=False, dialect='standard',
                                                      use_bqstorage_api=True)

        logging.getLogger(LOG_NAME).info('Classification: Finished classification data query')
        logging.getLogger(LOG_NAME).info(f'Classification: Classification size {len(classification_dataframe)}')
        return classification_dataframe.drop(['region', 'operatingSystem'], 1)

    # 4. Auxiliary functions
    def format_date(self, date: datetime.date):
        return date.strftime('%Y%m%d')
