package com.zjcoding.demo.filter;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Description 发送过滤消息
 * @Author ZhangJun
 * @Data 2020/8/11 10:39
 */

public class FilterProducer {

    public static void main(String[] args) throws Exception{

        DefaultMQProducer producer = new DefaultMQProducer("filterProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0; i < 6; i++) {
            byte[] content = ("content"+i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            //tag过滤
            Message message = new Message("filterTopic","tagA",content);
            //自定义属性过滤
            message.putUserProperty("num",String.valueOf(i));
            producer.send(message);
        }

        producer.shutdown();

    }

}
