#!/bin/bash
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
if [ $# != 3 ]; then
    echo "Usage: $0 gcp_project_id bucket_name region"
    exit 1
fi
cd dataflow
echo "Select GCP project $1"
gcloud config set project $1
echo "Install Python requirements"
python3 -m pip install -q -r requirements.txt
echo "Install Dataflow"
python3 -m main --runner DataflowRunner --project $1 --temp_location gs://$2/tmp/ --region $3 --setup_file ./setup.py --template_location gs://$2/templates/advanced_audiences --num_workers 1 --autoscaling_algorithm=NONE
echo "Copy medata to bucket $2"
gsutil cp advanced_audiences_metadata "gs://$2/templates/advanced_audiences_metadata"
cd ..
echo "Copy SQL queries to bucket $2"
gsutil cp -r queries "gs://$2"
echo "Finished"
