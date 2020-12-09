package com.zjcoding.demo.sync;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

/**
 * @Description 发送同步消息
 * @Author ZhangJun
 * @Data 2020/8/10 16:42
 */

public class SyncProducer {

    public static void main(String[] args) throws Exception{
        //创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("producerGroupA");
        //绑定nameServer，获取broker路由
        producer.setNamesrvAddr("localhost:9876");
        //启动
        producer.start();
        for (int i = 0;i<4;i++){
            //构造消息内容
            byte[] content = ("content"+i).getBytes(RemotingHelper.DEFAULT_CHARSET);
            //指定消息所在topic、消息过滤tag
            Message message = new Message("TopicABC","tagA || tagB",content);
            message.putUserProperty("num",String.valueOf(i));
            //发送消息到broker并获取发送结果，延迟消息
            SendResult sendResult = producer.send(message);
            System.out.println("发送结果："+sendResult);
        }
        producer.shutdown();
    }

}
