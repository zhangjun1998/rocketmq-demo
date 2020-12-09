package com.zjcoding.demo.broadcast;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

/**
 * @Description 发送广播消息
 * @Author ZhangJun
 * @Data 2020/8/10 21:18
 */

public class BroadcastProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("broadcast_producer");
        producer.setProducerGroup("ProducerGroup1");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0;i<4;i++){
            Message message = new Message("TopicB",("hello_"+i).getBytes());
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }

        producer.shutdown();
    }

}
