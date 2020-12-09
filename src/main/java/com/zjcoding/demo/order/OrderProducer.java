package com.zjcoding.demo.order;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.List;

/**
 * @Description 发送顺序消息
 * @Author ZhangJun
 * @Data 2020/8/10 19:30
 */

public class OrderProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("order_producer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0;i<9;i++){
            int orderId = i % 3;
            byte[] content = ("content"+i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            Message message = new Message("TopicA",content);
            //根据队列选择器选择要发送到的队列
            SendResult result = producer.send(message, new MessageQueueSelector() {
                /**
                 * @Description 参数o为send()方法传入的参数
                 * @Date 20:45 2020/8/10
                 * @Param [list, message, o]
                 * @return org.apache.rocketmq.common.message.MessageQueue
                 **/
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    Integer id = (Integer) o;
                    int index = id % list.size();
                    return list.get(index);
                }
            },orderId);
            System.out.println("消息发送结果："+result);
        }
        producer.shutdown();
    }

}

