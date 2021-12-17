package com.keyway.rabbitmq.consumer;


import com.keyway.rabbitmq.common.DetailResponse;

/**
 *
 * @author liuchunqing
 */
public interface MessageConsumer {
    DetailResponse consume();
}
