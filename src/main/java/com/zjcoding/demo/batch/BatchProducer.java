package com.zjcoding.demo.batch;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description 批量发送消息
 * @Author ZhangJun
 * @Data 2020/8/11 9:26
 */

public class BatchProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("batchProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        String topic = "BatchTopic";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic,"TagA","batch_helloA".getBytes(RemotingHelper.DEFAULT_CHARSET)));
        messages.add(new Message(topic,"TagA","batch_helloB".getBytes(RemotingHelper.DEFAULT_CHARSET)));

        SendResult sendResult;
        //批量发送所有消息(消息总和不超过1M的情况下)
        //sendResult = producer.send(messages);
        //System.out.println(sendResult);

        //切割消息数组，分多批次发送(消息总和超过1M的情况下)
        ListSplitter splitter = new ListSplitter(messages);
        while (splitter.hasNext()){
            sendResult = producer.send(splitter.next());
            System.out.println(sendResult);
        }

        producer.shutdown();

    }

}
