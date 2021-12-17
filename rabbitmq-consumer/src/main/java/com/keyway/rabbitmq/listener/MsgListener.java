package com.keyway.rabbitmq.listener;

import com.keyway.rabbitmq.annotation.EasyRabbitListener;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Component;
@Component
public class MsgListener {
    @EasyRabbitListener(exchange = "demo", routingKey = "demo", queue = "demo",type = "direct",concurrency = "2")
    public void consume(Message message) {
        byte[] body = message.getBody();
        System.out.println("接收到消息：" +Thread.currentThread()+ new String(body));
    }
}
