package com.keyway.rabbitmq.config;

import com.keyway.rabbitmq.consumer.EasyRabbitConsumer;
import com.keyway.rabbitmq.consumer.EasyRabbitConsumerProcessor;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.keyway.rabbitmq.producer.EasyRabbitProducer;

import javax.annotation.PostConstruct;

/**
 * @author liuchunqing
 */

@Configuration
@ConditionalOnClass({EasyRabbitProducer.class, EasyRabbitConsumer.class})
@EnableConfigurationProperties(EasyRabbitProperties.class)
public class EasyRabbitAutoConfigure {


    @Autowired
    private EasyRabbitProperties easyRabbitProperties;

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        return new RabbitTemplate(connectionFactory);
    }

    @ConditionalOnMissingBean
    @PostConstruct
    public ConnectionFactory connectionFactory() {
        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setAddresses(easyRabbitProperties.getAddresses());
        connectionFactory.setUsername(easyRabbitProperties.getUsername());
        connectionFactory.setPassword(easyRabbitProperties.getPassword());
        connectionFactory.setVirtualHost(easyRabbitProperties.getVirtualHost());
        connectionFactory.setPublisherConfirms(easyRabbitProperties.isPublisherConfirms());
        return connectionFactory;
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "spring.rabbitmq.enable", havingValue = "true")
    public EasyRabbitConsumerProcessor easyRabbitConsumerProcessor() {
        return new EasyRabbitConsumerProcessor();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "spring.rabbitmq.enable", havingValue = "true")
    public EasyRabbitProducer easyBuildRabbitMqProducer(ConnectionFactory connectionFactory) {
        return new EasyRabbitProducer(connectionFactory, easyRabbitProperties.isPublisherConfirms());
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(name = "spring.rabbitmq.enable", havingValue = "true")
    public EasyRabbitConsumer easyBuildRabbitMqConsumer(ConnectionFactory connectionFactory) {
        return new EasyRabbitConsumer(connectionFactory);
    }
}
