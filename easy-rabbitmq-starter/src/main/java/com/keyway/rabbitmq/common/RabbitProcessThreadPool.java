package com.keyway.rabbitmq.common;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 * @author 网上找的
 */
public class RabbitProcessThreadPool {

    /**
     * 自定义线程池
     */
    public static ExecutorService newCachedThreadPool(int corePoolSize,int maximumPoolSize,long keepAliveTimeMs,int capacity) {
        TimeUnit unit = TimeUnit.SECONDS;
        BlockingQueue<Runnable> workQueue = new ArrayBlockingQueue<>(capacity);
        ThreadFactory threadFactory = new NameTreadFactory();
        RejectedExecutionHandler handler = new MyIgnorePolicy();
        ThreadPoolExecutor executor  = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTimeMs, unit, workQueue, threadFactory, handler);
        // 预启动所有核心线程
        executor.prestartAllCoreThreads();
        return executor;
    }

    private static class NameTreadFactory implements ThreadFactory {

        private final AtomicInteger mThreadNum = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "mq-thread-" + mThreadNum.getAndIncrement());
        }
    }

    private static class MyIgnorePolicy implements RejectedExecutionHandler {

        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            doLog(r, e);
        }

        private void doLog(Runnable r, ThreadPoolExecutor e) {
            // 可做日志记录等
            System.err.println(r.toString() + " rejected");
        }
    }
}
