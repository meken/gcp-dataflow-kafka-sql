# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
output "project_id" {
  value = var.gcp_project_id
}

output "region" {
  value = var.gcp_region
}

output "storage_bucket" {
  value = google_storage_bucket.bucket.name
}

output "database_instance" {
  value = google_sql_database_instance.sql_filter_db.name
}

output "database_root_password" {
  value     = google_sql_database_instance.sql_filter_db.root_password
  sensitive = true
}

output "database_user_sa" {
  value = google_sql_user.filter_db_sa_user.name
}

output "dataflow_worker_sa" {
  value = google_service_account.dataflow_worker_sa.email
}

output "kafka_cluster" {
  value = google_managed_kafka_cluster.cluster.cluster_id
}

output "kafka_src_topic_partition_count" {
  value = google_managed_kafka_topic.src.partition_count
}
