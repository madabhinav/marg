package com.marg.pipe;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import java.util.ArrayList;
import java.util.List;



public class Producer {


    public static void main(String[] args) throws Exception {

            PipelineOptions options = PipelineOptionsFactory.create();
            Pipeline p = Pipeline.create(options);

            // sample data
            List<KV<Long, String>> kvs = new ArrayList<KV<Long, String>>();
            kvs.add(KV.of(1L, "hi there"));
            kvs.add(KV.of(2L, "hi"));
            kvs.add(KV.of(3L, "hi sue bob"));
            kvs.add(KV.of(4L, "hi sue"));
            kvs.add(KV.of(5L, "hi bob"));

            PCollection<KV<Long, String>> input = p
                    .apply(Create.of(kvs));

            input.apply(KafkaIO.<Long, String>write()
                    .withBootstrapServers("localhost:9092")
                    .withTopic("test")

                   //.withKeySerializer(org.apache.kafka.common.serialization.StringSerializer.class)
                    .withKeySerializer(LongSerializer.class)
                    .withValueSerializer(StringSerializer.class)
            );

            p.run().waitUntilFinish();
        }

    }

