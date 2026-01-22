# Dataflow with Kafka Streams

This is a sample repository that illustrates how to read/write from/to Kafka topics while filtering the data on the 
fly, using a lookup table. There are in principle two options to implement the lookup tables. If the lookup table is 
static and would fit in memory of the worker VMs, the side input pattern would be the most efficient approach. 
However, if the data is not static and/or too large to put into memory, an external service could be used. In this 
example we'll be using a Google Cloud SQL - PostgreSQL instance to do the lookups.

## Setup

This example comes with Terraform resources to create a Kafka cluster on Google Cloud, a Google Cloud SQL instance 
and a Client VM that can be use to access the Kafka cluster (and/or publish/subscribe to/from the Kafka cluster). 
During the setup of the Client VM, access rights are granted to the service account that is used to run the Dataflow 
pipeline, on the database.

Running the Terraform should be straight forward, just cd into the `src/main/tf` directory and run the required 
commands. If your project doesn't have a default network, it can create that as well, check the details of `variables.tf` 
for more information. Applying the Terraform will take ~15 minutes to complete. 

The pipeline code expects a table called `filter_data` in the `filter` database with a single column `values`. For 
each message that the pipeline processes a SQL query is fired on this table to check if the message (value) exists 
in this table. The dataset and the table are created during the setup. You'll need to populate the table with your 
data. This can be done by importing a csv file from Google Cloud Storage.

First we need to grant the required permissions to the Cloud SQL Service Agent:

```shell
cd src/main/tf
PROJECT_ID=`terraform output --raw project_id`
DB_NAME=`terraform output --raw database_instance`
BUCKET=`terraform output --raw storage_bucket`
CLOUD_SQL_SA=`gcloud sql instances describe $DB_NAME \
  --project=$PROJECT_ID \
  --format="value(serviceAccountEmailAddress)"`
gcloud storage buckets add-iam-policy-binding gs://$BUCKET \
    --member=serviceAccount:$CLOUD_SQL_SA \
    --role=roles/storage.objectViewer
```

```shell
gcloud sql import csv $DB_NAME \
  --project=$PROJECT_ID \
  --database filter \
  --table filter_data \
  gs://$BUCKET/primes-10M.txt.gz
```

Once you have the table populated, it makes sense to create an index on it, either through Google Cloud Console, or 
through `psql` (preferably after setting up Google Cloud SQL Auth Proxy, see `setup.tftpl` for an example)
```shell
CREATE INDEX "idx_filter_data_values" ON filter_data (values);
```

## Pipeline

The Dataflow pipeline is rather straight-forward, it reads from a Kafka topic, filters messages through a `ParDo` by 
looking them up in Cloud SQL and publishes the filtered messages to another Kafka topic.

## Running the pipeline

In order to submit the pipeline, make sure that the Terraform has been applied and the filter table has been 
populated. You can then use the following command to submit the pipeline to Dataflow.

```shell
# assuming that you're in src/main/tf
PROJECT_ID=`terraform output --raw project_id`
REGION=`terraform output --raw region`
BUCKET=`terraform output --raw storage_bucket`
DATAFLOW_WORKER_SA=`terraform output --raw dataflow_worker_sa`
KAFKA_CLUSTER=`terraform output --raw kafka_cluster`
BOOTSTRAP_SERVER="bootstrap.$KAFKA_CLUSTER.$REGION.managedkafka.$PROJECT_ID.cloud.goog:9092"
PARTITION_COUNT=`terraform output --raw kafka_src_topic_partition_count`
CLOUD_SQL_NAME=`terraform output --raw database_instance`
CLOUD_SQL_USER=`terraform output --raw database_user_sa`
DATABASE_INSTANCE_NAME="$PROJECT_ID:$REGION:$CLOUD_SQL_NAME"

cd ../../../ # go back to the top dir where pom.xml lives

mvn -Pdataflow-runner \
  compile exec:java -Dexec.mainClass=org.example.kafka.KafkaStream \
  "-Dexec.args=--runner=DataflowRunner \
  --project=$PROJECT_ID \
  --region=$REGION \
  --gcpTempLocation=gs://$BUCKET/temp/ \
  --stagingLocation=gs://$BUCKET/staging/ \
  --serviceAccount=$DATAFLOW_WORKER_SA \
  --dataflowServiceOptions=streaming_mode_at_least_once \
  --dataflowServiceOptions=enable_preflight_validation=false \
  --enableStreamingEngine=true \
  --maxNumWorkers=2 \
  --usePublicIps=false \
  --workerMachineType=c2-standard-4 \
  --workerLogLevelOverrides='{\"org.example.kafka.KafkaStream$CloudSqlFilter\":\"DEBUG\"}' \
  --bootstrapServer=$BOOTSTRAP_SERVER \
  --partitionCount=$PARTITION_COUNT \
  --databaseInstanceName=$DATABASE_INSTANCE_NAME \
  --databaseUser=$CLOUD_SQL_USER \
  --jobName=kafka-sql-kafka-v1"
```

Once the pipeline is running you can use your own client to send messages to the Kafka cluster. Keep in mind that 
the messages should have the same format as the data in the database so that the filtering works as expected.