package com.keyway.rabbitmq.annotation;

import com.keyway.rabbitmq.consumer.EasyRabbitListens;

import java.lang.annotation.Documented;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author liuchunqing
 * rabbitmq消息消费者监听，无需声明队列及建立绑定，可以直接接收队列消息
 */
@Documented
@Retention(RUNTIME)
@Target(METHOD)
@Repeatable(EasyRabbitListens.class)
public @interface EasyRabbitListener {
    /**
     * 交换机名称
     * @return
     */
    String exchange() default "";

    /**
     * 路由健
     * @return
     */
    String routingKey();

    /**
     * 队列名
     * @return
     */
    String queue();

    /**
     * 类型
     * @return
     */
    String type();

    /**
     * 消费线程数
     *
     * @return
     */
    String concurrency() default "1";

    /**
     * 一次请求取消息数
     */
    String prefetch() default "1";
}
