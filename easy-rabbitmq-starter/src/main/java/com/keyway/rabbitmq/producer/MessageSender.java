package com.keyway.rabbitmq.producer;

import com.keyway.rabbitmq.common.DetailResponse;
import com.rabbitmq.client.Channel;

/**
 * @author liuchunqing
 */
public interface MessageSender {


    Channel channel();

    DetailResponse send(Object message);

    DetailResponse send(MessageWithTime messageWithTime);
}