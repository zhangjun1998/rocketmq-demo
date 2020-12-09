package com.zjcoding.demo.sync;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @Description 消费同步消息
 * @Author ZhangJun
 * @Data 2020/8/10 17:21
 */

public class Consumer {

    public static void main(String[] args) throws Exception{
        //创建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumerGroupAQWAA");
        //绑定nameServer，获取broker路由
        consumer.setNamesrvAddr("localhost:9876");
        //订阅一个或者多个topic，指定属性过滤规则
        consumer.subscribe("TopicABC","*");
        // consumer.subscribe("TopicB","*");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

        //注册监听器
        consumer.registerMessageListener(
                new MessageListenerConcurrently() {
                    @Override
                    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                        for (int i = 0;i<list.size();i++){
                            System.out.println(list.get(i));
                        }
                        //返回消费结果
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                }
        );

        //启动消费
        consumer.start();
    }

}
