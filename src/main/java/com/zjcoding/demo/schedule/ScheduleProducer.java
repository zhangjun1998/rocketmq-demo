package com.zjcoding.demo.schedule;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Description 发送定时消息
 * @Author ZhangJun
 * @Data 2020/8/10 21:51
 */

public class ScheduleProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("scheduleProducer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        for (int i = 0;i<4;i++){
            Message message = new Message("scheduleTopic",("schedule_"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            message.setDelayTimeLevel(3);
            SendResult sendResult = producer.send(message);
            System.out.println(sendResult);
        }
        producer.shutdown();

    }

}
