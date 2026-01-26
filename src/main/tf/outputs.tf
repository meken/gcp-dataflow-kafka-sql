# MIT License
#
# Copyright (c) 2026-present Murat Eken
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
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
