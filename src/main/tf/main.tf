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
resource "google_project_service" "default" {
  project = var.gcp_project_id
  for_each = toset([
    "cloudresourcemanager.googleapis.com",
    "serviceusage.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "pubsub.googleapis.com",
    "managedkafka.googleapis.com",
    "sqladmin.googleapis.com",
    "servicenetworking.googleapis.com"
  ])

  service = each.key

  disable_on_destroy = false
}

# In case a default network is not present in the project the variable `create_default_network` needs to be set.
resource "google_compute_network" "default_network_created" {
  name                    = "default"
  auto_create_subnetworks = true
  count                   = var.create_default_network ? 1 : 0
  depends_on = [
    google_project_service.default
  ]
}

resource "google_compute_firewall" "fwr_allow_custom" {
  name          = "fwr-ingress-allow-custom"
  network       = google_compute_network.default_network_created[0].self_link
  count         = var.create_default_network ? 1 : 0
  source_ranges = ["10.128.0.0/9"]
  allow {
    protocol = "all"
  }
}

resource "google_compute_firewall" "fwr_allow_iap" {
  name          = "fwr-ingress-allow-iap"
  network       = google_compute_network.default_network_created[0].self_link
  count         = var.create_default_network ? 1 : 0
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}

# This piece of code makes it possible to deal with the default network the same way, regardless of how it has
# been created. Make sure to refer to the default network through this resource when needed.
data "google_compute_network" "default_network" {
  name = "default"
  depends_on = [
    google_project_service.default,
    google_compute_network.default_network_created
  ]
}

resource "google_storage_bucket" "bucket" {
  name                        = var.gcp_project_id
  location                    = var.gcp_region
  uniform_bucket_level_access = true
  force_destroy               = true
}

locals {
  kafka_vcpus = 7
}

resource "google_managed_kafka_cluster" "cluster" {
  cluster_id = "kafka-cluster"
  location   = var.gcp_region
  capacity_config {
    vcpu_count   = local.kafka_vcpus
    memory_bytes = local.kafka_vcpus * 3 * pow(1024, 3) # 3 GiB per core
  }
  gcp_config {
    access_config {
      network_configs {
        subnet = "projects/${var.gcp_project_id}/regions/${var.gcp_region}/subnetworks/default"
      }
    }
  }

  depends_on = [google_project_service.default]
}

resource "google_managed_kafka_topic" "src" {
  topic_id           = "src"
  cluster            = google_managed_kafka_cluster.cluster.cluster_id
  location           = var.gcp_region
  partition_count    = 4
  replication_factor = 3
}

resource "google_managed_kafka_topic" "dst" {
  topic_id           = "dst"
  cluster            = google_managed_kafka_cluster.cluster.cluster_id
  location           = var.gcp_region
  partition_count    = 4
  replication_factor = 3
}

resource "google_service_account" "kafka_vm_sa" {
  account_id   = "sa-kafka-client-vm"
  display_name = "Kafka Client VM Service Account"
}

resource "google_project_iam_member" "kafka_vm_sa_roles" {
  project = var.gcp_project_id
  for_each = toset([
    "roles/managedkafka.client",
    "roles/iam.serviceAccountTokenCreator",
    "roles/iam.serviceAccountOpenIdTokenCreator",
    "roles/storage.admin",
    "roles/cloudsql.admin"
  ])
  role   = each.key
  member = "serviceAccount:${google_service_account.kafka_vm_sa.email}"
}

resource "google_compute_instance" "kafka_vm" {
  name         = "gce-lnx-kafka-client"
  machine_type = "e2-standard-2"

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
    }
  }

  shielded_instance_config {
    enable_secure_boot = true
    enable_vtpm        = true
  }

  network_interface {
    network = data.google_compute_network.default_network.self_link
    access_config {}
  }

  service_account {
    email  = google_service_account.kafka_vm_sa.email
    scopes = ["cloud-platform"]
  }

  metadata_startup_script = templatefile("${path.module}/setup.tftpl", {
    gcp_project_id    = var.gcp_project_id,
    gcp_region        = var.gcp_region,
    gcs_bucket        = google_storage_bucket.bucket.name,
    cluster_id        = google_managed_kafka_cluster.cluster.cluster_id
    database_name     = google_sql_database_instance.sql_filter_db.name
    database_root_pwd = random_string.sql_password.result
    database_sa_usr   = google_sql_user.filter_db_sa_user.name
  })

  depends_on = [
    google_project_service.default,
    google_compute_network.default_network_created
  ]
}

resource "google_service_account" "dataflow_worker_sa" {
  account_id   = "sa-dataflow-worker"
  display_name = "Dataflow Worker Service Account"
}

resource "google_project_iam_member" "dataflow_worker_sa_roles" {
  project = var.gcp_project_id
  for_each = toset([
    "roles/dataflow.worker",
    "roles/storage.admin",
    "roles/managedkafka.client",
    "roles/pubsub.editor",
    "roles/cloudsql.instanceUser",
    "roles/cloudsql.client"
  ])
  role   = each.key
  member = "serviceAccount:${google_service_account.dataflow_worker_sa.email}"
}

resource "google_compute_global_address" "private_ip_address" {
  name          = "private-ip-address"
  purpose       = "VPC_PEERING"
  address_type  = "INTERNAL"
  prefix_length = 16
  network       = data.google_compute_network.default_network.self_link
}

resource "google_service_networking_connection" "default" {
  network                 = data.google_compute_network.default_network.self_link
  service                 = "servicenetworking.googleapis.com"
  reserved_peering_ranges = [google_compute_global_address.private_ip_address.name]
}

resource "random_string" "sql_password" {
  length           = 12
  special          = true
  override_special = "._+-"
}

resource "google_sql_database_instance" "sql_filter_db" {
  name                = "sql-filter"
  database_version    = "POSTGRES_18"
  root_password       = random_string.sql_password.result
  deletion_protection = false
  settings {
    edition   = "ENTERPRISE"
    tier      = "db-custom-2-8192"
    disk_size = "100"
    disk_type = "PD_SSD"

    database_flags {
      name  = "cloudsql.iam_authentication"
      value = "on"
    }

    ip_configuration {
      ipv4_enabled    = false
      private_network = data.google_compute_network.default_network.self_link
    }
  }

  depends_on = [google_service_networking_connection.default]
}

resource "google_compute_network_peering_routes_config" "peering_routes" {
  peering              = google_service_networking_connection.default.peering
  network              = data.google_compute_network.default_network.name
  import_custom_routes = true
  export_custom_routes = true
}

resource "google_sql_database" "filter_db" {
  instance = google_sql_database_instance.sql_filter_db.name
  name     = "filter"
}

resource "google_sql_user" "filter_db_sa_user" {
  instance = google_sql_database_instance.sql_filter_db.name
  name     = trimsuffix(google_service_account.dataflow_worker_sa.email, ".gserviceaccount.com")
  type     = "CLOUD_IAM_SERVICE_ACCOUNT"
}
