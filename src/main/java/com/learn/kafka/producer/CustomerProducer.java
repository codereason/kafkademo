package com.learn.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class CustomerProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop100:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.yuhuang.producer.CustomerPartitioner");
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>("first", Integer.toString(i), Integer.toString(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null)
                        System.out.println(recordMetadata.partition() + "--" + recordMetadata.offset());
                    else {
                        System.out.println("发送失败");
                    }
                }

            });

        producer.close();
    }
}
