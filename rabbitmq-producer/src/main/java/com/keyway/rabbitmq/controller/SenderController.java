package com.keyway.rabbitmq.controller;

import com.keyway.rabbitmq.producer.EasyRabbitProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;

@RequestMapping("/")
@RestController
public class SenderController {
    @Autowired
    private EasyRabbitProducer easyRabbitProducer;

    @RequestMapping("/send")
    public void senderMsg(String msg) throws IOException {
        easyRabbitProducer.buildDirectMessageSender("demo", "demo", "demo").send(msg);
    }
}
