package org.schambon.loadsimrunner.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.bson.Document;
import org.schambon.loadsimrunner.WorkloadManager;
import org.schambon.loadsimrunner.report.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncRunner extends AbstractRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncRunner.class);
    
    private AsyncWorkloadExecutor executor;
    private int targetConcurrency;
    private String operationType;

    public AsyncRunner(WorkloadManager workloadConfiguration, Reporter reporter, int concurrency) {
        super(workloadConfiguration, reporter);
        this.targetConcurrency = concurrency;
        this.operationType = workloadConfiguration.getName();
        this.executor = new AsyncWorkloadExecutor(name, concurrency, reporter);
    }

    @Override
    public void run() {
        var keepGoing = true;
        long totalDuration = 0;
        long counter = 0;

        if (this.startAfterDuration > 0) {
            try {
                Thread.sleep(this.startAfterDuration);
            } catch (InterruptedException e) {
                LOGGER.error(String.format("Workload %s: Error caught in execution", name), e);
            }
        }

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        while (keepGoing) {
            long iterationStart = System.currentTimeMillis();
            
            try {
                template.setVariables(variables);
                
                CompletableFuture<Void> future = executor.submit(createOperation());
                futures.add(future);
                
                counter++;

                LOGGER.debug("Counter: {}, stopAfter: {}", counter, stopAfter);
                if (stopAfter > 0 && counter >= stopAfter) {
                    LOGGER.info("Workload {} stopping.", name);
                    keepGoing = false;
                }
                
                long iterationDuration = System.currentTimeMillis() - iterationStart;
                totalDuration += iterationDuration;
                
                if (stopAfterDuration > 0 && totalDuration >= stopAfterDuration) {
                    LOGGER.info("Workload {} stopping.", name);
                    keepGoing = false;
                }
            } catch (Exception e) {
                LOGGER.error(String.format("Workload %s: Error caught in execution", name), e);
            } finally {
                template.clearVariables();
            }

            if (pace != 0) {
                long iterationDuration = System.currentTimeMillis() - iterationStart;
                long wait = Math.max(0, pace - iterationDuration);
                try {
                    Thread.sleep(wait);
                } catch (InterruptedException e) {
                }
            }
        }
        
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        executor.shutdown();
    }

    private AsyncWorkloadExecutor.AsyncOperation createOperation() {
        final Document generatedDoc = batch > 0 ? null : template.generate();
        final List<Document> batchDocs = batch > 0 ? generateBatch() : null;
        
        return new AsyncWorkloadExecutor.AsyncOperation() {
            @Override
            public int execute() {
                if (batch > 0) {
                    mongoColl.insertMany(batchDocs);
                    return batch;
                } else {
                    mongoColl.insertOne(generatedDoc);
                    return 1;
                }
            }
            
            @Override
            public String getMetricName() {
                return name;
            }
        };
    }
    
    private List<Document> generateBatch() {
        var docs = new ArrayList<Document>();
        for (int i = 0; i < batch; i++) {
            docs.add(template.generate());
        }
        return docs;
    }

    @Override
    protected long doRun() {
        return 0;
    }
}
