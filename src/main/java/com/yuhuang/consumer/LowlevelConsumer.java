package com.yuhuang.consumer;


import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

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
    private void getData() {

    }

}
