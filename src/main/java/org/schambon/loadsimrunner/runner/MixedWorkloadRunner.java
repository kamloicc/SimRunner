package org.schambon.loadsimrunner.runner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.schambon.loadsimrunner.TemplateManager;
import org.schambon.loadsimrunner.WorkloadManager;
import org.schambon.loadsimrunner.errors.InvalidConfigException;
import org.schambon.loadsimrunner.report.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;

public class MixedWorkloadRunner extends AbstractRunner {

    private static final Logger LOGGER = LoggerFactory.getLogger(MixedWorkloadRunner.class);

    private WeightedOperationSelector selector;
    private Map<String, OperationExecutor> executors;
    private WorkloadManager workloadConfig;

    public MixedWorkloadRunner(WorkloadManager workloadConfiguration, Reporter reporter, List<Document> mixConfig) {
        super(workloadConfiguration, reporter);
        this.workloadConfig = workloadConfiguration;
        this.selector = new WeightedOperationSelector(mixConfig);
        this.executors = new HashMap<>();
        
        for (Document opConfig : mixConfig) {
            String op = opConfig.getString("op");
            if (!executors.containsKey(op)) {
                executors.put(op, createExecutor(op));
            }
        }
    }

    private OperationExecutor createExecutor(String op) {
        switch (op) {
            case "find":
                return new FindExecutor();
            case "insert":
                return new InsertExecutor();
            case "updateOne":
                return new UpdateOneExecutor();
            case "updateMany":
                return new UpdateManyExecutor();
            case "deleteOne":
                return new DeleteOneExecutor();
            case "deleteMany":
                return new DeleteManyExecutor();
            case "replaceOne":
                return new ReplaceOneExecutor();
            case "replaceWithNew":
                return new ReplaceWithNewExecutor();
            case "aggregate":
                return new AggregateExecutor();
            default:
                throw new InvalidConfigException("Unsupported operation in mix: " + op);
        }
    }

    @Override
    protected long doRun() {
        WeightedOperationSelector.SelectedOperation selected = selector.selectOperation();
        OperationExecutor executor = executors.get(selected.op);
        
        String metricName = name + ":" + selected.op;
        
        return executor.execute(selected.params, metricName);
    }

    private interface OperationExecutor {
        long execute(Document params, String metricName);
    }

    private class FindExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            filter = template.generate(filter);

            var limit = opParams.getInteger("limit", -1);
            var cursor = mongoColl.find(filter)
                .sort((Document) opParams.get("sort"))
                .projection((Document) opParams.get("project"));
            
            if (limit != -1) {
                cursor = cursor.limit(limit);
            }

            int count = 0;
            var start = System.currentTimeMillis();
            var iterator = cursor.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, count, duration);
            return duration;
        }
    }

    private class InsertExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            var start = System.currentTimeMillis();
            
            if (batch > 0) {
                var docs = new java.util.ArrayList<Document>();
                for (int i = 0; i < batch; i++) {
                    docs.add(template.generate());
                }
                mongoColl.insertMany(docs);
                var duration = System.currentTimeMillis() - start;
                reporter.reportOp(metricName, batch, duration);
                return duration;
            } else {
                var doc = template.generate();
                mongoColl.insertOne(doc);
                var duration = System.currentTimeMillis() - start;
                reporter.reportOp(metricName, 1, duration);
                return duration;
            }
        }
    }

    private class UpdateOneExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            Document update = (Document) opParams.get("update");
            filter = template.generate(filter);
            update = template.generate(update);

            var start = System.currentTimeMillis();
            var result = mongoColl.updateOne(filter, update);
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, (int) result.getModifiedCount(), duration);
            return duration;
        }
    }

    private class UpdateManyExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            Document update = (Document) opParams.get("update");
            filter = template.generate(filter);
            update = template.generate(update);

            var start = System.currentTimeMillis();
            var result = mongoColl.updateMany(filter, update);
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, (int) result.getModifiedCount(), duration);
            return duration;
        }
    }

    private class DeleteOneExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            filter = template.generate(filter);

            var start = System.currentTimeMillis();
            var result = mongoColl.deleteOne(filter);
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, (int) result.getDeletedCount(), duration);
            return duration;
        }
    }

    private class DeleteManyExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            filter = template.generate(filter);

            var start = System.currentTimeMillis();
            var result = mongoColl.deleteMany(filter);
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, (int) result.getDeletedCount(), duration);
            return duration;
        }
    }

    private class ReplaceOneExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            Document replacement = (Document) opParams.get("update");
            filter = template.generate(filter);
            replacement = template.generate(replacement);
            replacement.remove("_id");

            var start = System.currentTimeMillis();
            var result = mongoColl.replaceOne(filter, replacement);
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, (int) result.getModifiedCount(), duration);
            return duration;
        }
    }

    private class ReplaceWithNewExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            Document filter = (Document) opParams.get("filter");
            filter = template.generate(filter);
            Document replacement = template.generate();

            var start = System.currentTimeMillis();
            var result = mongoColl.replaceOne(filter, replacement);
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, (int) result.getModifiedCount(), duration);
            return duration;
        }
    }

    private class AggregateExecutor implements OperationExecutor {
        @Override
        public long execute(Document opParams, String metricName) {
            @SuppressWarnings("unchecked")
            List<Document> pipeline = (List<Document>) opParams.get("pipeline");
            if (pipeline == null) {
                pipeline = new java.util.ArrayList<>();
            }
            
            var generatedPipeline = new java.util.ArrayList<Document>();
            for (var stage : pipeline) {
                generatedPipeline.add(template.generate(stage));
            }

            int count = 0;
            var start = System.currentTimeMillis();
            var iterator = mongoColl.aggregate(generatedPipeline).iterator();
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            var duration = System.currentTimeMillis() - start;

            reporter.reportOp(metricName, count, duration);
            return duration;
        }
    }
}
