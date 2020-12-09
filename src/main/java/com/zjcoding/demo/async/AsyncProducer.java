package com.zjcoding.demo.async;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Description 异步发送消息
 * @Author ZhangJun
 * @Data 2020/8/10 17:38
 */

public class AsyncProducer {

    public static void main(String[] args) throws Exception{
        DefaultMQProducer producer = new DefaultMQProducer("async_producer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        producer.setRetryTimesWhenSendFailed(0);

        int count = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(count);
        for (int i = 0;i<count;i++){
            Message message = new Message("TopicA",("async_hello_"+i).getBytes());
            producer.send(message, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    System.out.println("发送成功："+sendResult);
                }

                @Override
                public void onException(Throwable throwable) {
                    System.out.println("发送失败");
                    throwable.printStackTrace();
                }
            });
        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }

}
