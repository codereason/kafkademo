package com.learn.kafka.consumer;


import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LowlevelConsumer {
    public static void main(String[] args) {
        ArrayList<String> brokers = new ArrayList<>();
        brokers.add("hadoop100");
        brokers.add("hadoop101");
        brokers.add("hadoop102");

        //端口号
        int port = 9092;
        String topic = "first";
        int partition = 0;
        long offset = 2;
        LowlevelConsumer lowlevelConsumer = new LowlevelConsumer();
        lowlevelConsumer.getData(brokers, port, topic, partition, offset);
    }

    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {
            //创建获取分区leader的消费者对象
            SimpleConsumer getLeader = new SimpleConsumer(broker, port, 1000, 1024 * 4
                    , "getLeader");
            //创建一个主题元数据信息请求
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //获取主题元数据返回值
            TopicMetadataResponse metadataResponse = getLeader.send(topicMetadataRequest);
            //解析metadata返回值
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            //遍历主题元数据
            for (TopicMetadata topicMetadatum : topicsMetadata) {
                //获取多个分区的源数据信息
                List<PartitionMetadata> partitionsMetadata = topicMetadatum.partitionsMetadata();
                //遍历分区的源数据
                for (PartitionMetadata PartitionMetadata : partitionsMetadata) {
                    if (PartitionMetadata.partitionId() == partition) {
                        return PartitionMetadata.leader();
                    }
                }
            }

        }


        return null;
    }

    //get data
    private void getData(List<String> brokers, int port, String topic, int partition, long offset) {
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null) {
            return;
        }

        String leaderHost = leader.host();
        SimpleConsumer getData = new SimpleConsumer(leaderHost, port, 1000, 1024 * 4
                , "getData");
        FetchRequest fetchRequest = new FetchRequestBuilder().addFetch(topic, partition, offset, 100).build();
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1 + " -- " + new String(bytes));
        }
    }

}
