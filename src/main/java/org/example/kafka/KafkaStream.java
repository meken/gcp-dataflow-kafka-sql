package org.example.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.managed.Managed;
import org.joda.time.Duration;

import java.util.Map;

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

        Map<String, Object> auth = Map.<String, Object>of(
            "security.protocol", "SASL_SSL",
            "sasl.mechanism", "OAUTHBEARER",
            "sasl.login.callback.handler.class",
                    "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler",
            "sasl.jaas.config",
                    "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
        );
        Map<String, Object> inputConfig = Map.<String, Object>of(
                "bootstrap_servers", options.getBootstrapServer(),
                "topic", options.getInputTopic(),
                "format", "RAW",
                "consumer_config_updates", auth
        );
        Map<String, Object> outputConfig = Map.<String, Object>of(
                "bootstrap_servers", options.getBootstrapServer(),
                "topic", options.getOutputTopic(),
                "format", "RAW",
                "producer_config_updates", auth
        );

        p.apply(Managed.read(Managed.KAFKA).withConfig(inputConfig)).getSinglePCollection().
                apply(Managed.write(Managed.KAFKA).withConfig(outputConfig));


//        p
//            .apply("ReadSource", KafkaIO.<byte[], byte[]>read()
//                .withBootstrapServers(options.getBootstrapServer())
//                .withTopic(options.getInputTopic())
//                .withDynamicRead(Duration.standardMinutes(1))
//                .withGCPApplicationDefaultCredentials()
//                .withKeyDeserializer(ByteArrayDeserializer.class)
//                .withValueDeserializer(ByteArrayDeserializer.class)
//                .withoutMetadata())
//            .apply("WriteSink", KafkaIO.<byte[], byte[]>write()
//                .withBootstrapServers(options.getBootstrapServer())
//                .withTopic(options.getOutputTopic())
//                .withKeySerializer(ByteArraySerializer.class)
//                .withValueSerializer(ByteArraySerializer.class)
//                .withGCPApplicationDefaultCredentials());
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
