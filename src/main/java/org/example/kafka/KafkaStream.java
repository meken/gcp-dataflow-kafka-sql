package org.example.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
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
        
        @Description("Filter file GCS URI")
        @Default.String("gs://meken-dataflow-test-01/filtered-prime-10M.txt")
        String getFilterFileURI();
        
        void setFilterFileURI(String value);
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

        PCollectionView<Map<String, Integer>> filterData = pipeline
                .apply("Read from GCS filter data",
                        TextIO.read().from(options.getFilterFileURI()))
                .apply("Map to KV pairs", MapElements.via(new SimpleFunction<String, KV<String, Integer>>() {
                    @Override
                    public KV<String, Integer> apply(String input) {
                        return KV.of(input, 1);
                    }
                }))
                .apply(View.<String, Integer>asMap());


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
                .apply("Filter",
                        ParDo.of(new DoFn<KV<byte[], byte[]>, KV<byte[], byte[]>>() {
                            @ProcessElement
                            public void processElement(
                                    ProcessContext c,
                                    @Element KV<byte[], byte[]> element,
                                    @SideInput("filter") Map<String, Integer> filterData) {
                                if (filterData.containsKey(new String(element.getValue()))) {
                                    c.output(c.element());
                                }
                            }
                        }).withSideInput("filter", filterData))
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
