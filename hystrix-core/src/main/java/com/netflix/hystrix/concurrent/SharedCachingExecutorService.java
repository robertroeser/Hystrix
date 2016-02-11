package com.netflix.hystrix.concurrent;

import rx.internal.util.unsafe.ConcurrentCircularArrayQueue;
import rx.internal.util.unsafe.MpmcArrayQueue;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Implementation of an {@link java.util.concurrent.ExecutorService} that shares threads between Executors but still
 * provides thread isolation. It will keep a consent shared pool of threads upo to the number of CPUs on the system, but
 * will create more threads on demand if needed.
 *
 * @author Robert Roeser
 */
public class SharedCachingExecutorService extends AbstractExecutorService {
    private ConcurrentCircularArrayQueue<Runnable> runnables;

    private ConcurrentHashMap<Thread,Object> threads = new ConcurrentHashMap<Thread, Object>();

    private static final Object OBJECT = new Object();

    private static final Executor executor = new Executor();

    private volatile int maxConcurrent;

    private volatile boolean shutdown = false;

    private final int queueDepth;

    private final String name;

    private AtomicLong activeCount;

    private AtomicLong completedTaskCount;

    private AtomicLong taskCount;

    public SharedCachingExecutorService(String name, int maxConcurrent, int queueDepth) {
        this.name = name;
        this.runnables = new MpmcArrayQueue<Runnable>(queueDepth);
        this.threads = new ConcurrentHashMap<Thread, Object>();
        this.maxConcurrent = maxConcurrent;
        this.activeCount = new AtomicLong();
        this.completedTaskCount = new AtomicLong();
        this.taskCount = new AtomicLong();
        this.queueDepth = queueDepth;
        executor.add(runnables);
    }

    @Override
    public void shutdown() {
        shutdown = true;
        executor.remove(runnables);
        Enumeration<Thread> keys = threads.keys();
        while (keys.hasMoreElements()) {
            Thread thread = keys.nextElement();
            System.out.println("interrupting thread => " + thread.getName());
            thread.interrupt();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        List<Runnable> runnableList = new ArrayList<Runnable>();

        while (runnables.isEmpty()) {
            Runnable poll = runnables.poll();
            if (poll != null) {
                runnableList.add(poll);
            }
        }

        return runnableList;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && runnables.isEmpty();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        final long start = System.currentTimeMillis();
        boolean terminated = isTerminated();

        while ((System.currentTimeMillis() - start) < unit.toMillis(timeout) ) {
            terminated = isTerminated();
            LockSupport.parkNanos(1000);
        }

        return terminated;
    }

    @Override
    public void execute(final Runnable command) {
        if (shutdown) {
            throw new IllegalStateException("Can not add work to an executor that is shutdown");
        }

        if (runnables.size() >= maxConcurrent) {
            throw new RejectedExecutionException("Can not execute more work as the maxCurrent of " + maxConcurrent + " is reached for " + name);
        }

        boolean offer = runnables.offer(new Runnable() {
            @Override
            public void run() {
                Thread thread = Thread.currentThread();
                threads.putIfAbsent(thread, OBJECT);
                try {
                    activeCount.incrementAndGet();
                    command.run();
                    completedTaskCount.incrementAndGet();
                } finally {
                    threads.remove(thread);
                }
            }
        });

        if (offer) {
            taskCount.incrementAndGet();
            executor.unpark();
        } else {
            throw new RejectedExecutionException("Work queue did not except for " + name);
        }
    }

    public void setMaxConcurrent(int dynamicCoreSize) {
        maxConcurrent = dynamicCoreSize;
    }

    public int getMaxConcurrent() {
        return maxConcurrent;
    }

    public Queue getQueue() {
        return runnables;
    }

    public int getQueueDepth() {
        return queueDepth;
    }

    public AtomicLong getActiveCount() {
        return activeCount;
    }

    public void setActiveCount(AtomicLong activeCount) {
        this.activeCount = activeCount;
    }

    public AtomicLong getCompletedTaskCount() {
        return completedTaskCount;
    }

    public void setCompletedTaskCount(AtomicLong completedTaskCount) {
        this.completedTaskCount = completedTaskCount;
    }

    public AtomicLong getTaskCount() {
        return taskCount;
    }

    public void setTaskCount(AtomicLong taskCount) {
        this.taskCount = taskCount;
    }

    static class Executor {
        static final List<ConcurrentCircularArrayQueue<Runnable>> runnables = new CopyOnWriteArrayList<ConcurrentCircularArrayQueue<Runnable>>();

        static final ConcurrentCircularArrayQueue<Thread> parkedThreads = new MpmcArrayQueue<Thread>(Runtime.getRuntime().availableProcessors());

        static final AtomicLong counter = new AtomicLong();

        public void add(ConcurrentCircularArrayQueue<Runnable> circularArrayQueue) {
            runnables.add(circularArrayQueue);
        }

        public void remove(ConcurrentCircularArrayQueue<Runnable> circularArrayQueue) {
            runnables.remove(circularArrayQueue);
        }

        /**
         * Looks in a queue for threads - if there is one available it will unpark that thread which will
         * start draining work queues. If doesn't find one it will create a new thread which will start draining work.
         */
        public void unpark() {
            Thread thread = parkedThreads.poll();

            if (thread != null) {
                LockSupport.unpark(thread);
            } else {
                thread = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (;;) {
                            for (ConcurrentCircularArrayQueue<Runnable> c : runnables) {
                                while (!c.isEmpty()) {
                                    Runnable poll = c.poll();
                                    poll.run();
                                }
                            }
                            Thread currentThread = Thread.currentThread();
                            boolean offer = parkedThreads.offer(currentThread);
                            if (offer) {
                                LockSupport.park();
                            } else {
                                return;
                            }
                        }
                    }
                });

                thread.setDaemon(true);
                thread.setName("hystrix-thread-" + counter.incrementAndGet());
                thread.start();
            }
        }
    }
}
