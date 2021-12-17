package com.keyway.rabbitmq.consumer;

import com.keyway.rabbitmq.annotation.EasyRabbitListener;

import java.lang.annotation.*;

/**
 * @author liuchunqing
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface EasyRabbitListens {
    EasyRabbitListener[] value();
}
