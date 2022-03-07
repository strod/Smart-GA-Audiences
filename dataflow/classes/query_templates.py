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

from google.cloud import storage
import tempfile
import io
import logging

LOG_NAME = 'lead_scoring'

class QueryTemplates:
  
  def __init__(self, path):
    self._path = path

  @property
  def training_query_template(self):
    return self._get_query_template('training')
  
  @property
  def classification_query_template(self):
    return self._get_query_template('classification')
  
  @property
  def trained_model_query_template(self):
    return """
    SELECT 
        timestamp, 
        sklearn_version, 
        python_version, 
        model, 
        md5,
        score_train,
        score_test
    FROM 
        {table_name} 
    WHERE 
        sklearn_version='{sklearn_version}' 
        AND python_version='{python_version}'
        AND name='{name}'
    ORDER BY 
        timestamp DESC 
    LIMIT 1
    """
  
  def _get_query_template(self, name):
    query = ''
    client = storage.Client()
    with tempfile.TemporaryFile(mode='w+b') as file:
      gs_url = self._path.get() +'/' + name + '_query_template.sql'
      logging.getLogger(LOG_NAME).info('Downloading: ' + gs_url)
      client.download_blob_to_file(gs_url, file)
      file.seek(0)
      with io.TextIOWrapper(file) as textFile:
        query = textFile.read()

    return query
