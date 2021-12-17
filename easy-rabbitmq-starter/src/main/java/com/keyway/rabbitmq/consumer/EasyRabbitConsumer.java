package com.keyway.rabbitmq.consumer;

import com.keyway.rabbitmq.common.DetailResponse;
import com.keyway.rabbitmq.common.EasyRabbitConstants;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.ShutdownSignalException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.support.DefaultMessagePropertiesConverter;
import org.springframework.amqp.rabbit.support.MessagePropertiesConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.springframework.amqp.core.ExchangeTypes.DIRECT;
import static org.springframework.amqp.core.ExchangeTypes.TOPIC;

/**
 * @author liuchunqing
 * @Description: 消息消费者
 * @date 2019/8/2715:14
 */
@Slf4j
public class EasyRabbitConsumer {

    private ConnectionFactory connectionFactory;
    @Autowired
    private AmqpAdmin amqpAdmin;
    private final Pattern pattern = Pattern.compile("^[\\s\\S]*no queue '[\\s\\S]*' in vhos[\\s\\S]*$");

    public EasyRabbitConsumer(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }


    public <T> MessageConsumer buildMessageConsumer(String exchange, String routingKey, final String queue,
                                                    final MessageProcess<T> messageProcess, String type) throws IOException {
        final Connection connection = connectionFactory.createConnection();

        //1 创建连接和channel
        buildQueue(exchange, routingKey, queue, connection, type);

        //2 设置message序列化方法
        final MessagePropertiesConverter messagePropertiesConverter = new DefaultMessagePropertiesConverter();
        final MessageConverter messageConverter = new Gson2JsonMessageConverter();
        //3 consume
        return new MessageConsumer() {
            Channel channel;

            {
                channel = connection.createChannel(false);
                channel.basicQos(25, true);
                if (log.isDebugEnabled()) {
                    log.debug("Rabbitmq:监听通道{}@{}连接成功！", queue, channel.getChannelNumber());
                }
            }

            @Override
            public DetailResponse consume() {
                try {
                    //1 通过basicGet获取原始数据
                    GetResponse response = channel.basicGet(queue, false);

                    while (response == null) {
                        response = channel.basicGet(queue, false);
                        Thread.sleep(EasyRabbitConstants.ONE_SECOND);
                    }
                    log.info("接收到消息：" + Thread.currentThread() + new String(response.getBody()));
                    Message message = new Message(response.getBody(),
                            messagePropertiesConverter.toMessageProperties(response.getProps(), response.getEnvelope(), "UTF-8"));
                    //2 将原始数据转换为特定类型的包
                    T messageBean = (T) messageConverter.fromMessage(message);

                    //3 处理数据
                    DetailResponse detailRes;

                    try {
                        detailRes = messageProcess.process(messageBean);
                    } catch (Exception e) {
                        log.error("exception", e);
                        detailRes = new DetailResponse(false, "process exception: " + e, "");
                    }

                    //4 手动发送ack确认
                    if (detailRes.isIfSuccess()) {
                        channel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    } else {
                        //避免过多失败log
                        Thread.sleep(EasyRabbitConstants.ONE_SECOND);
                        log.info("process message failed: " + detailRes.getErrMsg());
                        channel.basicNack(response.getEnvelope().getDeliveryTag(), false, true);
                    }

                    return detailRes;
                } catch (InterruptedException e) {
                    log.error("exception", e);
                    return new DetailResponse(false, "interrupted exception " + e.toString(), "");
                } catch (ShutdownSignalException | ConsumerCancelledException | IOException e) {
                    log.error("exception", e);

                    try {
                        channel.close();
                    } catch (IOException | TimeoutException ex) {
                        log.error("close channel throw exception", ex);
                    }
                    //channel重建
                    channel = connection.createChannel(false);
                    //queue重建(防止queue被误删后一直报错)
                    try {
                        Throwable cause = getCause(e);
                        String str = cause.getMessage();
                        log.error("exception class: {}", cause.getClass().getSimpleName());
                        log.error("exception message: {}", str);
                        Matcher m = pattern.matcher(str);
                        if (cause instanceof ShutdownSignalException && m.find()) {
                            log.info("rebuild queue");
                            //1 创建连接和channel
                            buildQueue(exchange, routingKey, queue, connection, type);
                        }
                    } catch (IOException ex) {
                        log.error("rebuild queue throw exception", ex);
                    }
                    return new DetailResponse(false, "shutdown or cancelled exception " + e.toString(), "");
                } catch (Exception e) {
                    log.info("exception : ", e);
                    try {
                        channel.close();
                    } catch (IOException | TimeoutException ex) {
                        ex.printStackTrace();
                    }
                    channel = connection.createChannel(false);
                    return new DetailResponse(false, "exception " + e.toString(), "");
                }
            }

            /**
             * 获取根异常
             * @param e
             * @return
             */
            private Throwable getCause(Throwable e) {
                if (e.getCause() == null) {
                    return e;
                }
                return getCause(e.getCause());
            }
        };
    }


    private void buildQueue(String exchange, String routingKey,
                            final String queue, Connection connection, String type) throws IOException {
        Channel channel = connection.createChannel(false);
        if (DIRECT.equals(type)) {
            if (!"".equals(exchange)) {
                declareBindingQueue(exchange, routingKey, queue, channel, DIRECT);
            } else {
                Binding binding = BindingBuilder.bind(new Queue(queue)).to(DirectExchange.DEFAULT).withQueueName();
                amqpAdmin.declareBinding(binding);
                try {
                    channel.close();
                } catch (TimeoutException e) {
                    log.info("close channel time out ", e);
                }
                return;
            }
        }
        if (TOPIC.equals(type)) {
            declareBindingQueue(exchange, routingKey, queue, channel, TOPIC);
        }
    }

    /**
     * 绑定queue到自定义交换机
     *
     * @param exchange
     * @param routingKey
     * @param queue
     * @param channel
     * @param direct
     * @throws IOException
     */
    private void declareBindingQueue(String exchange, String routingKey, String queue, Channel channel, String direct) throws IOException {
        channel.exchangeDeclare(exchange, direct, true, false, null);
        channel.queueDeclare(queue, true, false, false, null);
        channel.queueBind(queue, exchange, routingKey);
        try {
            channel.close();
        } catch (TimeoutException e) {
            log.info("close channel time out ", e);
        }
    }
}
