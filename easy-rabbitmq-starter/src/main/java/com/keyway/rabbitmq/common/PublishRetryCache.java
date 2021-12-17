package com.keyway.rabbitmq.common;

import com.keyway.rabbitmq.producer.MessageSender;
import com.keyway.rabbitmq.producer.MessageWithTime;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;


/**
 * retryCache的容器
 * 增长重试时间
 * @author liuchunqing
 */
@Slf4j
public class PublishRetryCache {
    private MessageSender sender;
    private boolean stop = false;
    private Map<Long, MessageWithTime> map = new ConcurrentSkipListMap<>();
    private AtomicLong id = new AtomicLong();
    private ExecutorService executorService = RabbitProcessThreadPool.newCachedThreadPool(2, 4, 10, 50);

    public void setSender(MessageSender sender) {
        this.sender = sender;
        startRetry();
    }

    public long generateId() {
        return id.incrementAndGet();
    }

    public void add(MessageWithTime messageWithTime) {
        map.putIfAbsent(messageWithTime.getId(), messageWithTime);
    }

    public void del(long id) {
        map.remove(id);
    }

    private void startRetry() {
        executorService.submit(new Retry());
    }

    private class Retry implements Runnable {
        @Override
        public void run() {
            while (!stop) {
                try {
                    Thread.sleep(EasyRabbitConstants.RETRY_TIME_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                long now = System.currentTimeMillis();

                for (Map.Entry<Long, MessageWithTime> entry : map.entrySet()) {
                    MessageWithTime messageWithTime = entry.getValue();

                    if (null != messageWithTime) {
                        if (messageWithTime.getTime() + 3 * EasyRabbitConstants.ONE_MINUTE < now) {
                            log.info("send message {} failed after 3 min，sending give up", messageWithTime);
                            //重试次数达到上限，放弃重试
                            del(entry.getKey());
                        } else if (messageWithTime.getTime() + EasyRabbitConstants.ONE_MINUTE < now) {
                            DetailResponse res = sender.send(messageWithTime);

                            if (!res.isIfSuccess()) {
                                log.info("retry send message failed {} errMsg {}", messageWithTime, res.getErrMsg());
                            }
                        }
                    }
                }
            }
        }
    }
}