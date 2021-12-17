package com.keyway.rabbitmq.consumer;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.thread.ThreadUtil;
import cn.hutool.json.JSONUtil;
import com.keyway.rabbitmq.annotation.EasyRabbitListener;
import com.keyway.rabbitmq.common.DetailResponse;
import com.keyway.rabbitmq.config.EasyRabbitAutoConfigure;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.env.Environment;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * @author liuchunqing
 * 队列监听处理器，监听所有@EasyRabbitListener方法
 */
@AutoConfigureAfter(EasyRabbitAutoConfigure.class)
@Slf4j
public class EasyRabbitConsumerProcessor implements BeanPostProcessor {
    @Autowired
    EasyRabbitConsumer easyRabbitConsumer;
    private Set<Class<?>> nonAnnotatedClasses = new HashSet<>();
    @Autowired
    private Environment environment;

    @Override
    public Object postProcessAfterInitialization(final Object bean, String beanName) {
        Class<?> targetClass = AopUtils.getTargetClass(bean);
        if (!this.nonAnnotatedClasses.contains(targetClass)) {
            //扫描bean内带有Scheduled注解的方法
            Map<Method, Set<EasyRabbitListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass,
                    new MethodIntrospector.MetadataLookup<Set<EasyRabbitListener>>() {
                        @Override
                        public Set<EasyRabbitListener> inspect(Method method) {
                            Set<EasyRabbitListener> annotatedMethod =
                                    AnnotatedElementUtils.getMergedRepeatableAnnotations(method,
                                            EasyRabbitListener.class, null);
                            return (!annotatedMethod.isEmpty() ? annotatedMethod : null);
                        }
                    });
            if (annotatedMethods.isEmpty()) {
                //如果这个class没有注解的方法，缓存下来，因为一个class可能有多个bean
                this.nonAnnotatedClasses.add(targetClass);
                if (log.isTraceEnabled()) {
                    log.trace("No @EasyRabbitListener annotations found on bean class: " + bean.getClass());
                }
            } else {
                // Non-empty set of methods
                Set<Map.Entry<Method, Set<EasyRabbitListener>>> entries = annotatedMethods.entrySet();
                //根据@EasyRabbitListener注解方法的数量创建线程池
                for (Map.Entry<Method, Set<EasyRabbitListener>> entry : entries) {
                    Method method = entry.getKey();
                    for (EasyRabbitListener easyRabbitListener : entry.getValue()) {
                        //处理这些有Scheduled的方法
                        process(easyRabbitListener, method, bean);
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug(annotatedMethods.size() + " @EasyRabbitListener methods processed on bean '" + beanName +
                            "': " + annotatedMethods);
                }
            }
        }
        return bean;
    }

    private void process(EasyRabbitListener easyRabbitListener, Method method, Object bean) {
        //concurrency处理
        Integer multi = Convert.toInt(easyRabbitListener.concurrency());
        multi = multi > 500 ? 500 : multi;
        for (int i = 0; i < multi; i++) {
            ExecutorService threadPoolExecutor = ThreadUtil.newSingleExecutor();
            threadPoolExecutor.execute(new ConsumerBuilder(easyRabbitListener, method, bean));
        }
    }

    private class ConsumerBuilder implements Runnable {
        EasyRabbitListener easyRabbitListener;
        Method method;
        Object bean;

        public ConsumerBuilder(EasyRabbitListener easyRabbitListener, Method method, Object bean) {
            this.easyRabbitListener = easyRabbitListener;
            this.method = method;
            this.bean = bean;
        }

        @Override
        public void run() {
            Boolean flag = true;
            while (flag) {
                MessageConsumer consumer = null;
                try {
                    consumer = easyRabbitConsumer.buildMessageConsumer(
                            environment.resolvePlaceholders(easyRabbitListener.exchange()),
                            environment.resolvePlaceholders(easyRabbitListener.routingKey()),
                            environment.resolvePlaceholders(easyRabbitListener.queue()),
                            new MessageProcess<Object>() {
                                @Override
                                public DetailResponse process(Object message) {
                                    Method invocableMethod = AopUtils.selectInvocableMethod(method,
                                            bean.getClass());
                                    try {
                                        if (message instanceof byte[]) {
                                            invocableMethod.invoke(bean,
                                                    new Message(new String((byte[]) message).getBytes(), null));
                                        } else {
                                            invocableMethod.invoke(bean,
                                                    new Message(JSONUtil.toJsonStr(message).getBytes(), null));
                                        }
                                        return new DetailResponse() {{
                                            setIfSuccess(true);
                                        }};
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        return new DetailResponse() {{
                                            setIfSuccess(false);
                                            setErrMsg(e.getMessage());
                                        }};
                                    }
                                }
                            }, environment.resolvePlaceholders(easyRabbitListener.type()));
                    if (log.isDebugEnabled()) {
                        log.debug("Rabbitmq:监听通道{}连接成功！", environment.resolvePlaceholders(easyRabbitListener.queue()));
                    }
                    Integer prefetch = Convert.toInt(easyRabbitListener.prefetch());
                    prefetch = prefetch > 30 ? 30 : prefetch;
                    //prefetch处理
                    for (int j = 0; j < prefetch; j++) {
                        ExecutorService threadPoolExecutor = ThreadUtil.newSingleExecutor();
                        threadPoolExecutor.execute(new RabbitmqListenerHander(easyRabbitListener, consumer));
                    }
                    flag = false;
                } catch (IOException e) {
                    log.error("Rabbitmq:重连失败！10秒后重连！");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                } catch (Exception e) {
                    flag = true;
                }
            }
        }
    }


    private class RabbitmqListenerHander implements Runnable {
        private EasyRabbitListener easyRabbitListener;
        private MessageConsumer consumer;

        RabbitmqListenerHander(EasyRabbitListener easyRabbitListener, MessageConsumer consumer) {
            this.easyRabbitListener = easyRabbitListener;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    consumer.consume();
                } catch (Exception e) {
                    log.error("Rabbitmq:连接{}断开,尝试重连....",
                            environment.resolvePlaceholders(easyRabbitListener.queue()));
                    break;
                }
            }
        }
    }
}
