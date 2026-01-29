# Dataflow with Kafka Streams

This is a sample repository that illustrates how to read/write from/to Kafka topics while filtering the data on the fly, using a lookup table. There are in principle two options to implement the lookup tables. If the lookup table is static and would fit in memory of the worker VMs, the side input pattern would be the most efficient approach. However, if the data is not static and/or too large to put into memory, an external service could be used. In this example we'll be using a Google Cloud SQL - PostgreSQL instance to do the lookups.

> [!NOTE]  
> This implementation focuses mainly on latency and hence filters messages element wise. A more throughput optimized version would require to batch the messages at the expense of end-to-end latency.

## Setup

This example comes with Terraform resources to create a Kafka cluster on Google Cloud, a Google Cloud SQL instance and a Client VM that can be use to access the Kafka cluster, and/or publish/subscribe to/from the Kafka cluster. During the setup of the Client VM, database access rights are granted to the Dataflow worker service account.

Running the Terraform scripts should be straight forward, just cd into the `src/main/tf` directory and run the required commands. If your project doesn't have a default network, there's an option to create that too, check the details of `variables.tf` for more information. Applying the Terraform scripts will take ~15 minutes to complete. 

The pipeline code expects a table called `filter_data` in the `filter` database with a single column `values`. For each message that the pipeline processes, a SQL query is fired on this table to check if the message (value) exists in this table. The dataset and the table are created and populated with sample data, during the setup. Also a sample (Python) client is put on the Client VM for generating the messages and analyzing the results (see [Client code section](#client-code)).

## Pipeline

The Dataflow pipeline is rather straight-forward, it reads from a Kafka topic, filters messages through a `ParDo` by looking them up in Cloud SQL and publishes the filtered messages to another Kafka topic.

## Running the pipeline

In order to submit the pipeline, make sure that the Terraform has been applied and the filter table has been populated.You can then use the following command to submit the pipeline to Dataflow.

> [!NOTE]  
> The pipeline code has been tested with Java 21.

```shell
# assuming that you're in top level 
PROJECT_ID=`terraform -chdir=src/main/tf output --raw project_id`
REGION=`terraform -chdir=src/main/tf output --raw region`
BUCKET=`terraform -chdir=src/main/tf output --raw storage_bucket`
DATAFLOW_WORKER_SA=`terraform -chdir=src/main/tf output --raw dataflow_worker_sa`
KAFKA_CLUSTER=`terraform -chdir=src/main/tf output --raw kafka_cluster`
BOOTSTRAP_SERVER="bootstrap.$KAFKA_CLUSTER.$REGION.managedkafka.$PROJECT_ID.cloud.goog:9092"
PARTITION_COUNT=`terraform -chdir=src/main/tf output --raw kafka_src_topic_partition_count`
CLOUD_SQL_NAME=`terraform -chdir=src/main/tf output --raw database_instance`
CLOUD_SQL_USER=`terraform -chdir=src/main/tf output --raw database_user_sa`
DATABASE_INSTANCE_NAME="$PROJECT_ID:$REGION:$CLOUD_SQL_NAME"

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
  --workerLogLevelOverrides='{\"org.example.kafka.KafkaStream\$CloudSqlFilter\":\"DEBUG\"}' \
  --bootstrapServer=$BOOTSTRAP_SERVER \
  --partitionCount=$PARTITION_COUNT \
  --databaseInstanceName=$DATABASE_INSTANCE_NAME \
  --databaseUser=$CLOUD_SQL_USER \
  --jobName=kafka-sql-kafka-v1"
```

## Client code

Once the pipeline is running you can use the client on the Client VM to send messages to the Kafka cluster. You can connect to the Client VM through using `gcloud` CLI or Google Cloud Console.

```shell
VM="gce-lnx-kafka-client"
ZONE=`gcloud compute instances list --project $PROJECT_ID  --filter="name=('$VM')" --format="value(zone)"`
gcloud compute ssh --project $PROJECT_ID --zone $ZONE --tunnel-through-iap $VM 
```

Once the SSH connection is established, you can find the client code in `/opt` directory.

```shell
# Unzip the sample client to the 'client' directory 
unzip -d client /opt/client.zip
cd client
# Install the Python dependencies in a virtual env
uv sync
# Publish 100 messages per second for 5 minutes
uv run publisher.py --tps 100 --duration 300
# Read from the source & destination topics
uv run collect.py
# Calculate the performance statistics
uv run analyze.py
```
