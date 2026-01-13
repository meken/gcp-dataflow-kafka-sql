package org.example.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Map;
import java.util.stream.IntStream;

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
    }

    static void runKafkaStream(KafkaStream.KafkaStreamOptions options) {
        Pipeline pipeline = Pipeline.create(options);

        Map<String, Object> auth = Map.of(
            "security.protocol", "SASL_SSL",
            "sasl.mechanism", "OAUTHBEARER",
            "sasl.login.callback.handler.class",
                    "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
            "sasl.jaas.config",
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
        );

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
                                .withConsumerConfigUpdates(auth)
                                .withoutMetadata())
                .apply("Write to Kafka",
                        KafkaIO.<byte[], byte[]>write()
                                .withBootstrapServers(options.getBootstrapServer())
                                .withTopic(options.getOutputTopic())
                                .withKeySerializer(ByteArraySerializer.class)
                                .withValueSerializer(ByteArraySerializer.class)
                                .withProducerConfigUpdates(auth));
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
