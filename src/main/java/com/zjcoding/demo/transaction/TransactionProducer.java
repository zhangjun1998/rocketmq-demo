package com.zjcoding.demo.transaction;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Description 发送事务消息
 * @Author ZhangJun
 * @Data 2020/8/11 13:57
 */

public class TransactionProducer {

    public static void main(String[] args) throws Exception{
        TransactionMQProducer producer = new TransactionMQProducer("transactionProducer");

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 60, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = newThread(r);
                thread.setName("transaction_thread");
                return thread;
            }
        });
        TransactionListener transactionListener = new TransactionListenerImpl();

        producer.setTransactionListener(transactionListener);
        producer.setExecutorService(executorService);
        producer.setNamesrvAddr("localhost:9876");

        producer.start();

        String[] tags = new String[]{"TagA","TagB","TagC"};
        for (int i = 0; i < 6; i++) {
            Message message = new Message("TransTopic",tags[i%tags.length],("trans_"+i).getBytes(RemotingHelper.DEFAULT_CHARSET));
            SendResult sendResult = producer.sendMessageInTransaction(message,null);
            System.out.println(sendResult);

            Thread.sleep(10);
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }

        producer.shutdown();
    }


}
