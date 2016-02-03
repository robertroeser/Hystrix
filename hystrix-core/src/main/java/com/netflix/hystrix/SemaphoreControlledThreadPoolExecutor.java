package com.netflix.hystrix;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 2/2/16.
 */
public class SemaphoreControlledThreadPoolExecutor extends AbstractExecutorService {
    private static final List<Runnable> EMPTY_LIST = new ArrayList<Runnable>();
    private static final AtomicLong threadCount = new AtomicLong();
    private static final ThreadPoolExecutor CACHED_THREAD_POOL =  new ThreadPoolExecutor(0, Integer.MAX_VALUE,
        60L, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r, "hystrix-cached-thread-" + threadCount.incrementAndGet());
            }
        });

    private String name;

    private AtomicBoolean running;

    private AtomicLong maxConcurrent;

    private AtomicLong currentCount;

    private AtomicLong completedTasks;

    public SemaphoreControlledThreadPoolExecutor(String name, long maxConcurrent) {
        this.name = name;
        this.running = new AtomicBoolean();
        this.maxConcurrent = new AtomicLong(maxConcurrent);
        this.completedTasks = new AtomicLong();
        this.currentCount = new AtomicLong();
    }

    @Override
    public void shutdown() {
        running.set(false);
    }

    @Override
    public List<Runnable> shutdownNow() {
        return EMPTY_LIST;
    }

    @Override
    public boolean isShutdown() {
        return !running.get();
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    public void setMaxConcurrent(long maxConcurrent) {
        this.maxConcurrent.set(maxConcurrent);
    }

    public Long getActiveCount() {
        return currentCount.get();
    }

    public Long getCompletedTaskCount() {
        return completedTasks.get();
    }

    public Long getCorePoolSize() {
        return currentCount.get();
    }

    public Long getLargestPoolSize() {
        return maxConcurrent.get();
    }

    public Long getMaximumPoolSize() {
        return maxConcurrent.get();
    }

    public Long getPoolSize() {
        return currentCount.get();
    }

    public long getTaskCount() {
        return 0;
    }

    public BlockingQueue<Runnable> getQueue() {
        return CACHED_THREAD_POOL.getQueue();
    }

    @Override
    public void execute(final Runnable command) {
        if (currentCount.get() >= maxConcurrent.get()) {
            String message = "Can not execute more work in Executor "
                + name + " - max concurrent requests "
                + maxConcurrent.get() + "  - current requests " + currentCount.get();
            throw new RejectedExecutionException(message);
        }

        CACHED_THREAD_POOL.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    currentCount.incrementAndGet();
                    command.run();
                    completedTasks.incrementAndGet();
                } finally {
                    currentCount.decrementAndGet();
                }
            }
        });
    }
}
