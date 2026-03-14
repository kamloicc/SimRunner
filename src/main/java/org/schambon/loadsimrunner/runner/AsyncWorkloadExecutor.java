package org.schambon.loadsimrunner.runner;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.schambon.loadsimrunner.report.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncWorkloadExecutor {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncWorkloadExecutor.class);
    
    private final ExecutorService executor;
    private final Semaphore semaphore;
    private final int concurrency;
    private final String workloadName;
    private final Reporter reporter;
    
    public AsyncWorkloadExecutor(String workloadName, int concurrency, Reporter reporter) {
        this.workloadName = workloadName;
        this.concurrency = concurrency;
        this.reporter = reporter;
        this.semaphore = new Semaphore(concurrency);
        
        if (isVirtualThreadsAvailable()) {
            LOGGER.info("Using virtual threads for workload: {}", workloadName);
            this.executor = createVirtualThreadExecutor();
        } else {
            LOGGER.info("Using platform threads for workload: {} with concurrency: {}", workloadName, concurrency);
            this.executor = Executors.newFixedThreadPool(concurrency, new WorkloadThreadFactory(workloadName));
        }
    }
    
    public CompletableFuture<Void> submit(AsyncOperation operation) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return CompletableFuture.failedFuture(e);
        }
        
        return CompletableFuture.runAsync(() -> {
            try {
                long start = System.currentTimeMillis();
                int recordCount = operation.execute();
                long duration = System.currentTimeMillis() - start;
                reporter.reportOp(operation.getMetricName(), recordCount, duration);
            } catch (Exception e) {
                LOGGER.error("Error executing async operation", e);
            } finally {
                semaphore.release();
            }
        }, executor);
    }
    
    public void shutdown() {
        executor.shutdown();
    }
    
    private boolean isVirtualThreadsAvailable() {
        try {
            Class.forName("java.lang.Thread$Builder");
            var version = Runtime.version().feature();
            return version >= 21;
        } catch (ClassNotFoundException e) {
            return false;
        }
    }
    
    private ExecutorService createVirtualThreadExecutor() {
        try {
            var builderMethod = Executors.class.getMethod("newVirtualThreadPerTaskExecutor");
            return (ExecutorService) builderMethod.invoke(null);
        } catch (Exception e) {
            LOGGER.warn("Failed to create virtual thread executor, falling back to platform threads", e);
            return Executors.newFixedThreadPool(concurrency, new WorkloadThreadFactory(workloadName));
        }
    }
    
    public interface AsyncOperation {
        int execute();
        String getMetricName();
    }
    
    private static class WorkloadThreadFactory implements ThreadFactory {
        private final AtomicInteger counter = new AtomicInteger(0);
        private final String workloadName;
        
        WorkloadThreadFactory(String workloadName) {
            this.workloadName = workloadName;
        }
        
        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(workloadName + "-async-" + counter.getAndIncrement());
            thread.setDaemon(true);
            return thread;
        }
    }
}
