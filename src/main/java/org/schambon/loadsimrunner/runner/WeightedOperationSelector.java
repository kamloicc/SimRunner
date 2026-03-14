package org.schambon.loadsimrunner.runner;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.bson.Document;
import org.schambon.loadsimrunner.errors.InvalidConfigException;

public class WeightedOperationSelector {
    
    private static class WeightedOperation {
        String op;
        Document params;
        int cumulativeWeight;
        
        WeightedOperation(String op, Document params, int cumulativeWeight) {
            this.op = op;
            this.params = params;
            this.cumulativeWeight = cumulativeWeight;
        }
    }
    
    private List<WeightedOperation> operations;
    private int totalWeight;
    
    public WeightedOperationSelector(List<Document> mixConfig) {
        if (mixConfig == null || mixConfig.isEmpty()) {
            throw new InvalidConfigException("Mix configuration cannot be empty");
        }
        
        operations = new ArrayList<>();
        int cumulative = 0;
        
        for (Document opConfig : mixConfig) {
            String op = opConfig.getString("op");
            Integer weight = opConfig.getInteger("weight");
            Document params = (Document) opConfig.get("params");
            
            if (op == null) {
                throw new InvalidConfigException("Each mix operation must have an 'op' field");
            }
            if (weight == null || weight <= 0) {
                throw new InvalidConfigException("Each mix operation must have a positive 'weight'");
            }
            
            cumulative += weight;
            operations.add(new WeightedOperation(op, params != null ? params : new Document(), cumulative));
        }
        
        totalWeight = cumulative;
        
        if (totalWeight == 0) {
            throw new InvalidConfigException("Total weight must be greater than 0");
        }
    }
    
    public SelectedOperation selectOperation() {
        int random = ThreadLocalRandom.current().nextInt(totalWeight);
        
        for (WeightedOperation op : operations) {
            if (random < op.cumulativeWeight) {
                return new SelectedOperation(op.op, op.params);
            }
        }
        
        return new SelectedOperation(operations.get(operations.size() - 1).op, 
                                     operations.get(operations.size() - 1).params);
    }
    
    public static class SelectedOperation {
        public final String op;
        public final Document params;
        
        SelectedOperation(String op, Document params) {
            this.op = op;
            this.params = params;
        }
    }
}
