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

import setuptools

setuptools.setup(
    name='leadscoring_dataflow',
    version='0.2',
    author='Antônio Moreira, Rodrigo Teixeira',
    author_email='antoniomoreira@google.com, rodrigo.teixeira@mint.ai',
    install_requires=[
        'google-cloud-bigquery>=1.27.2',
        'google-cloud-storage>=1.35.0',
        'google-cloud-bigquery-storage==2.6.3',
        'gunicorn==20.0.4',
        'pandas==1.1.3',
        'pandas-gbq>=0.17.0',
        'scikit-learn>=1.0.1',
        'apache-beam[gcp]==2.36.0',
        'imbalanced-learn>=0.9.0',
        'xgboost==0.90',
        'pyarrow==3.0.0'
        ],
    packages=setuptools.find_packages(),
)