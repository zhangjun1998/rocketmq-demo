package com.zjcoding.demo.batch;

import org.apache.rocketmq.common.message.Message;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @Description 消息数组分割器
 * @Author ZhangJun
 * @Data 2020/8/11 9:37
 */

public class ListSplitter implements Iterator<List<Message>> {

    /**
     * @Description 限制每次发送的消息量总和最大为1M
     * @Date 9:50 2020/8/11
     **/
    private final int LIMIT_SIZE = 1000*1000;

    private List<Message> messages;

    private int currentIndex;

    public ListSplitter(List<Message> messages) {
        this.messages = messages;
    }

    @Override
    public boolean hasNext() {
        return currentIndex<messages.size();
    }

    @Override
    public List<Message> next() {
        int nextIndex = currentIndex;
        int totalSize = 0;

        for (;nextIndex<messages.size();nextIndex++){
            Message message = messages.get(nextIndex);
            //消息主题+消息体长度
            int messageSize = message.getTopic().length()+message.getBody().length;
            //消息内部属性key-value长度
            Map<String,String> properties = message.getProperties();
            for (Map.Entry<String,String> entry: properties.entrySet()){
                messageSize += entry.getKey().length()+entry.getValue().length();
            }
            // 日志消耗长度
            messageSize += 20;

            //如果某条消息长度超过总消息长度限制，该条消息单独发送
            if (messageSize > LIMIT_SIZE){
                //方便后面消息数组切分
                if (nextIndex-currentIndex == 0){
                    nextIndex++;
                }
                break;
            }

            if (messageSize + totalSize > LIMIT_SIZE){
                break;
            }else {
                totalSize += messageSize;
            }
        }

        //切割消息数组
        List<Message> subList = messages.subList(currentIndex,nextIndex);
        currentIndex = nextIndex;
        return subList;
    }
}
