package com.marg.pipe;

import org.apache.beam.runners.spark.SparkContextOptions;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


public class Consumer {

    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        //SparkContextOptions sparkOptions = options.as(SparkContextOptions.class);
        //sparkOptions.setJobName("Consumer Test");
        //sparkOptions.setRunner(SparkRunner.class);
        Pipeline p = Pipeline.create(options);

        p.apply(KafkaIO.<Long, String>read()
                .withBootstrapServers("localhost:9092")
                .withTopic("test")
                .withKeyDeserializer(LongDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .withoutMetadata());

        p.run().waitUntilFinish();
    }
}