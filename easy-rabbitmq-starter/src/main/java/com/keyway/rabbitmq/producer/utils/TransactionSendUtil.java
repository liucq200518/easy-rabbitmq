package com.keyway.rabbitmq.producer.utils;

import cn.hutool.json.JSONUtil;
import com.keyway.rabbitmq.producer.MessageWithTime;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;

import java.io.IOException;
import java.util.List;

/**
 * 事务型消息队列工具
 *
 * @author liuchunqing
 */
@Slf4j
@Data
    public abstract class TransactionSendUtil {

    public static TransactionSendUtil init(ConnectionFactory connectionFactory) throws IOException {
        TransactionSendUtil transactionSendUtil = new TransactionSendUtil() {
            private String exchange;
            private String routingKey;
            private Channel channel;

            @Override
            public TransactionSendUtil exchange(String exchange) throws IOException {
                this.exchange = exchange;
                channel.exchangeDeclare(exchange, "topic", true, false, null);
                return this;
            }

            @Override
            public TransactionSendUtil routingKey(String routingKey) {
                this.routingKey = routingKey;
                return this;
            }

            @Override
            TransactionSendUtil multi() throws IOException {
                Connection connection = connectionFactory.createConnection();
                channel = connection.createChannel(true);
                channel.txSelect();
                return this;
            }

            @Override
            public TransactionSendUtil sendMessage(MessageWithTime messageWithTime) throws IOException {
                // 发送消息
                channel.basicPublish(exchange, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN,
                        JSONUtil.toJsonStr(messageWithTime.getMessage()).getBytes("UTF-8"));
                return this;
            }

            @Override
            public TransactionSendUtil sendMessage(List<MessageWithTime> messageWithTimes) throws IOException {
                for (MessageWithTime messageWithTime : messageWithTimes) {
                    // 发送消息
                    channel.basicPublish(exchange, routingKey, MessageProperties.PERSISTENT_TEXT_PLAIN,
                            JSONUtil.toJsonStr(messageWithTime.getMessage()).getBytes("UTF-8"));
                }
                return this;
            }

            @Override
            public void exec() throws IOException {
                try {
                    channel.txCommit();
                } catch (Exception e) {
                    channel.txRollback();
                    throw e;
                }
                log.info("message send success");
            }
        };
        //初始化开启事务
        transactionSendUtil.multi();
        return transactionSendUtil;
    }

    abstract TransactionSendUtil multi() throws IOException;

    public abstract TransactionSendUtil exchange(String exchange) throws IOException;

    public abstract TransactionSendUtil routingKey(String routingKey);

    public abstract TransactionSendUtil sendMessage(MessageWithTime messageWithTime) throws IOException;

    public abstract TransactionSendUtil sendMessage(List<MessageWithTime> messageWithTimes) throws IOException;

    public abstract void exec() throws IOException;

}
