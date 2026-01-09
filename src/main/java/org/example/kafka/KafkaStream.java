package org.example.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.joda.time.Duration;

public class KafkaStream {


    public interface KafkaStreamOptions extends PipelineOptions {
        @Description("Kafka input/source topic")
        @Default.String("src")
        String getInputTopic();

        void setInputTopic(String value);

        @Description("Kafka output/sink topic")
        @Default.String("dst")
        String getOutputTopic();

        void setOutputTopic(String value);

        @Description("Bootstrap server")
        @Default.String("bootstrap.kafka-cluster.us-central1.managedkafka.meken-dataflow-test-01.cloud.goog:9092")
        String getBootstrapServer();

        void setBootstrapServer(String value);
    }

    static void runKafkaStream(KafkaStream.KafkaStreamOptions options) {
        Pipeline p = Pipeline.create(options);

        p
            .apply("ReadSource", KafkaIO.<byte[], byte[]>read()
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getInputTopic())
                .withDynamicRead(Duration.standardMinutes(1))
                .withGCPApplicationDefaultCredentials()
                .withKeyDeserializer(ByteArrayDeserializer.class)
                .withValueDeserializer(ByteArrayDeserializer.class)
                .withoutMetadata())
            .apply("WriteSink", KafkaIO.<byte[], byte[]>write()
                .withBootstrapServers(options.getBootstrapServer())
                .withTopic(options.getOutputTopic())
                .withKeySerializer(ByteArraySerializer.class)
                .withValueSerializer(ByteArraySerializer.class)
                .withGCPApplicationDefaultCredentials());
        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        KafkaStreamOptions options = PipelineOptionsFactory
            .fromArgs(args)
            .withValidation()
            .as(KafkaStreamOptions.class);
        runKafkaStream(options);
    }
}
