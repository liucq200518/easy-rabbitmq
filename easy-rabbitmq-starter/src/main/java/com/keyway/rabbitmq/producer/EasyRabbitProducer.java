package com.keyway.rabbitmq.producer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.support.CorrelationData;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;

import com.keyway.rabbitmq.common.PublishRetryCache;
import com.keyway.rabbitmq.common.DetailResponse;
import com.keyway.rabbitmq.common.EasyRabbitConstants;
import com.rabbitmq.client.Channel;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;

import static org.springframework.amqp.core.ExchangeTypes.DIRECT;
import static org.springframework.amqp.core.ExchangeTypes.TOPIC;

/**
 * @author liuchunqing
 * @Description 消息生产者，带重试
 **/

@Slf4j
public class EasyRabbitProducer {

    private ConnectionFactory connectionFactory;
    public Boolean publisherConfirms;
    @Autowired
    private AmqpAdmin amqpAdmin;
    private PublishRetryCache publishRetryCache = new PublishRetryCache();

    public EasyRabbitProducer(ConnectionFactory connectionFactory, Boolean publisherConfirms) {
        this.connectionFactory = connectionFactory;
        this.publisherConfirms = publisherConfirms;
    }

    public MessageSender buildDirectMessageSender(final String exchange, final String routingKey, final String queue) throws IOException {
        return buildMessageSender(exchange, routingKey, queue, DIRECT, null);
    }

    public MessageSender buildTopicMessageSender(final String exchange, final String routingKey) throws IOException {
        return buildMessageSender(exchange, routingKey, null, TOPIC, null);
    }

    public MessageSender buildDirectMessageSender(final String exchange, final String routingKey, final String queue,
                                                  Map<String, Object> args) throws IOException {
        return buildMessageSender(exchange, routingKey, queue, DIRECT, args);
    }

    /**
     * 发送消息
     *
     * @param exchange   消息交换机
     * @param routingKey 消息路由key
     * @param queue      消息队列
     * @param type       消息类型
     *                   return
     */
    private MessageSender buildMessageSender(final String exchange, final String routingKey, final String queue,
                                             final String type, Map<String, Object> args) throws IOException {
        Connection connection = connectionFactory.createConnection();
        //1
        if (DIRECT.equals(type)) {
            buildQueue(exchange, routingKey, queue, connection, args);
        } else if (TOPIC.equals(type)) {
            buildTopic(exchange, connection, args);
        }
        //创建RabbitTemplate，并配置参数
        final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setExchange(exchange);
        rabbitTemplate.setRoutingKey(routingKey);
        rabbitTemplate.setMessageConverter(new Jackson2JsonMessageConverter());
        //消息确认监听器
        //如果消息没有到exchange,则confirm回调,ack=false,此时通过retryCathe重试（默认一分钟一次，三次后放弃发送）
        //如果消息到达exchange,则confirm回调,ack=true
        rabbitTemplate.setConfirmCallback((correlationData, ack, cause) -> {
            if (!ack) {
                log.error("send message failed: " + cause + correlationData.toString());
            } else {
                //发送成功后在重试缓存中删除该条记录
                try {
                    publishRetryCache.del(Long.parseLong(correlationData.getId()));
                } catch (NullPointerException ignored) {
                }
                log.info("send message success，delete from retry cache！");
            }
        });
        //消息失败监听器
        //exchange到queue成功,则不回调return
        //exchange到queue失败,则回调return(需设置mandatory=true,否则不会回调,消息就丢了)，休眠后继续发送
        rabbitTemplate.setMandatory(true);
        rabbitTemplate.setReturnCallback((message, replyCode, replyText, tmpExchange, tmpRoutingKey) -> {
            try {
                Thread.sleep(EasyRabbitConstants.ONE_SECOND);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            log.info("send message failed: " + replyCode + " " + replyText);
            rabbitTemplate.send(message);
        });

        return new MessageSender() {
            {
                publishRetryCache.setSender(this);
            }

            @Override
            public Channel channel() {
                return null;
            }

            @Override
            public DetailResponse send(Object message) {
                long id = publishRetryCache.generateId();
                long time = System.currentTimeMillis();

                return send(new MessageWithTime(id, time, message));
            }

            @Override
            public DetailResponse send(MessageWithTime messageWithTime) {
                try {
                    long id = StringUtils.isEmpty(messageWithTime.getId()) ? publishRetryCache.generateId() : messageWithTime.getId();
                    long time = StringUtils.isEmpty(messageWithTime.getTime()) ? System.currentTimeMillis() : messageWithTime.getTime();
                    messageWithTime.setTime(time);
                    messageWithTime.setId(id);
                    if (publisherConfirms) {
                        publishRetryCache.add(messageWithTime);
                    }
                    Message message = MessageBuilder.withBody(Convert.toStr(messageWithTime.getMessage()).getBytes())
                            .setContentType(MessageProperties.CONTENT_TYPE_TEXT_PLAIN)
                            .setContentEncoding(StandardCharsets.UTF_8.displayName())
                            .setMessageId(Convert.toStr(id))
                            .build();
                    rabbitTemplate.correlationConvertAndSend(message, new CorrelationData(String.valueOf(id)));
                } catch (Exception e) {
                    return new DetailResponse(false, "", "");
                }
                log.info("send message success");
                return new DetailResponse(true, "", "");
            }
        };
    }


    private void buildQueue(String exchange, String routingKey,
                            final String queue, Connection connection, Map<String, Object> args) throws IOException {
        Channel channel = connection.createChannel(false);
        if (!"".equals(exchange)) {
            //1 自定义exchange
            channel.exchangeDeclare(exchange, DIRECT, true, false, args);
            channel.queueDeclare(queue, true, false, false, args);
            channel.queueBind(queue, exchange, routingKey);
        } else {
            //2 默认exchange
            Binding binding = BindingBuilder.bind(new Queue(queue)).to(DirectExchange.DEFAULT).withQueueName();
            amqpAdmin.declareBinding(binding);
        }
        try {
            channel.close();
        } catch (TimeoutException e) {
            log.info("close channel time out ", e);
        }
    }

    private void buildTopic(String exchange, Connection connection, Map<String, Object> args) throws IOException {
        Channel channel = connection.createChannel(false);
        channel.exchangeDeclare(exchange, TOPIC, true, false, null);
        try {
            channel.close();
        } catch (TimeoutException e) {
            log.info("close channel time out ", e);
        }
    }


}
