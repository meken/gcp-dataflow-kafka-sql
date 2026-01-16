package org.example.kafka;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.IntStream;

@SuppressWarnings("ALL")
public class KafkaStream {
    public interface KafkaStreamOptions extends StreamingOptions {
        @Description("Kafka input/source topic")
        @Default.String("src")
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Kafka output/sink topic")
        @Default.String("dst")
        String getOutputTopic();

        void setOutputTopic(String value);

        @Description("Number of partitions for the input topic")
        @Default.Integer(4)
        int getPartitionCount();

        void setPartitionCount(int value);

        @Description("Bootstrap server")
        @Default.String("bootstrap.kafka-cluster.us-central1.managedkafka.meken-dataflow-test-01.cloud.goog:9092")
        String getBootstrapServer();

        void setBootstrapServer(String value);

        @Description("JDBC url for the filter database, jdbc:postgresql:///<DB_NAME>")
        @Default.String("jdbc:postgresql:///filter")
        String getJdbcUrl();

        void setJdbcUrl(String value);

        @Description("Cloud SQL Instance Connection Name, e.g., project:region:instance")
        @Default.String("meken-dataflow-test-01:us-central1:sql-filter")
        String getDatabaseInstanceName();

        void setDatabaseInstanceName(String value);

        @Description("Cloud SQL instance user service account")
//        @Default.String("sac-dataflow-worker@meken-dataflow-test-01.iam.gserviceaccount.com")
        @Default.String("sac-dataflow-worker@meken-dataflow-test-01.iam")
        String getDatabaseUser();

        void setDatabaseUser(String value);
    }

    public static class CloudSqlFilter extends DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>> {
        private static final Logger LOG = LoggerFactory.getLogger(CloudSqlFilter.class);

        private final String jdbcUrl;
        private final String databaseInstanceName;
        private final String databaseUser;

        private transient HikariDataSource dataSource;

        public CloudSqlFilter(KafkaStreamOptions options) {
            this.jdbcUrl = options.getJdbcUrl();
            this.databaseInstanceName = options.getDatabaseInstanceName();
            this.databaseUser = options.getDatabaseUser();
        }

        @Setup
        public void setup() {
            HikariConfig config = new HikariConfig();
            config.setUsername(this.databaseUser);
            config.setPassword("42"); // ignored
            config.setJdbcUrl(this.jdbcUrl);
            config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
            config.addDataSourceProperty("cloudSqlInstance", this.databaseInstanceName);
            config.addDataSourceProperty("enableIamAuth", "true");
            config.addDataSourceProperty("sslmode", "disable");
            config.addDataSourceProperty("ipTypes", "PRIVATE");
//            config.addDataSourceProperty("cloudSqlRefreshStrategy", "lazy");

            config.setMaximumPoolSize(16);
            this.dataSource = new HikariDataSource(config);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            KV<byte[], byte[]> element = c.element();
            String lookupValue = new String(element.getValue());

            String query = "SELECT values FROM filter_data WHERE values = ?";

            try (Connection conn = dataSource.getConnection()) {
                PreparedStatement ps = conn.prepareStatement(query);
                ps.setString(1, lookupValue);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    c.output(KV.of(null, rs.getString("values").getBytes()));
                }
            } catch (SQLException e) {
                LOG.warn(e.getMessage());
            }
        }


        @Teardown
        public void teardown() {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    static void runKafkaStream(KafkaStream.KafkaStreamOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("Read from Kafka",
                        KafkaIO.<byte[], byte[]>read()
                                .withBootstrapServers(options.getBootstrapServer())
                                .withTopicPartitions(IntStream.range(0, options.getPartitionCount())
                                        .mapToObj(i -> new TopicPartition(options.getInputTopic(), i))
                                        .toList())
                                .withKeyDeserializerAndCoder(
                                        ByteArrayDeserializer.class, NullableCoder.of(ByteArrayCoder.of()))
                                .withValueDeserializerAndCoder(ByteArrayDeserializer.class, ByteArrayCoder.of())
                                .withGCPApplicationDefaultCredentials()
                                .withoutMetadata())
                .apply("Filter",
                        ParDo.of(new CloudSqlFilter(options)))
                .apply("Write to Kafka",
                        KafkaIO.<byte[], byte[]>write()
                                .withBootstrapServers(options.getBootstrapServer())
                                .withTopic(options.getOutputTopic())
                                .withKeySerializer(ByteArraySerializer.class)
                                .withValueSerializer(ByteArraySerializer.class)
                                .withGCPApplicationDefaultCredentials());
        pipeline.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        KafkaStreamOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(KafkaStreamOptions.class);
        options.setStreaming(true);
        runKafkaStream(options);
    }
}
