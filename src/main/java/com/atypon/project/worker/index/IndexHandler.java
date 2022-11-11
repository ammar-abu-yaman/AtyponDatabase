package com.atypon.project.worker.index;

import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.databind.JsonNode;
import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.query.QueryHandler;
import com.atypon.project.worker.query.Query.Originator;

import java.util.*;
import java.util.stream.Collectors;

public class IndexHandler extends QueryHandler {

    private IndexService indexService;

    public IndexHandler(IndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    public void handle(Query request) {
        switch (request.getQueryType()) {
            case FindDocument:
            case FindDocuments:
                handleRead(request);
                return;
            case AddDocument:
                handleAddDocument(request);
                return;
            case UpdateDocument:
                handleUpdate(request);
                return;
            case DeleteDocument:
                handleDelete(request);
                return;
            case CreateIndex:
                handleCreateIndex(request);
                return;
            case DeleteIndex:
                handleDeleteIndex(request);
                return;
            case DeleteDatabase:
                handleDeleteDatabase(request);
                return;
            default:
                pass(request);
                return;
        }
    }

    private void handleRead(Query request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();
        if (filterKey == null) {
            pass(request);
            return;
        }

        IndexKey key = new IndexKey(request.getDatabaseName(), filterKey.getKey());
        if (indexService.containsIndex(key))
            request.setIndex(indexService.getIndex(key).get());
        pass(request);
    }

    private void handleAddDocument(Query request) {
        pass(request);
        if (request.getStatus() == Query.Status.Rejected)
            return;
        Map<String, Index> affectedIndexes = getAffectedIndexes(request);
        String documentIndex = request.getUsedDocuments().stream().collect(Collectors.toList()).get(0);
        for (Map.Entry<String, Index> entry : affectedIndexes.entrySet()) {
            entry.getValue().add(request.getPayload().get(entry.getKey()), documentIndex);
        }

        for (Map.Entry<String, Index> entry : affectedIndexes.entrySet()) {
            indexService.saveToFile(new IndexKey(request.getDatabaseName(), entry.getKey()), entry.getValue());
        }
    }

    private void handleUpdate(Query request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();

        IndexKey key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if (indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }

        pass(request);
        if (request.getStatus() == Query.Status.Accepted)
            recalculateIndexes(request.getDatabaseName());
    }

    private void handleDelete(Query request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();

        IndexKey key = null;
        if (request.getOriginator() == Originator.User)
            key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if (indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }

        pass(request);
        if (request.getStatus() == Query.Status.Accepted)
            recalculateIndexes(request.getDatabaseName());
    }

    private void handleDeleteDatabase(Query request) {
        pass(request);
        if (request.getStatus() == Query.Status.Rejected)
            return;
        indexService.deleteDatabaseIndices(request.getDatabaseName());
    }

    private void handleCreateIndex(Query request) {
        DatabaseService databaseService = getDatabaseService();
        IndexKey key = createKey(request);
        if (indexService.containsIndex(key)) {
            request.setStatus(Query.Status.Rejected);
            request.getRequestOutput().append("Index already exists");
            return;
        }

        if (!databaseService.containsDatabase(request.getDatabaseName())) {
            request.setStatus(Query.Status.Rejected);
            request.getRequestOutput().append("Database Doesn't exist");
            return;
        }

        indexService.createIndex(key);
        pass(request);

        request.setStatus(Query.Status.Accepted);
        return;
    }

    private void handleDeleteIndex(Query request) {
        IndexKey key = createKey(request);
        if (!indexService.containsIndex(key)) {
            request.setStatus(Query.Status.Rejected);
            request.getRequestOutput().append("Index doesn't exist");
            return;
        }

        indexService.deleteIndex(key);
        pass(request);

        request.setStatus(Query.Status.Accepted);
    }

    private Map<String, Index> getAffectedIndexes(Query request) {
        Map<String, Index> indexes = new HashMap<>();
        Iterator<String> iterator = request.getPayload().fieldNames();
        while (iterator.hasNext()) {
            String fieldName = iterator.next();
            IndexKey key = new IndexKey(request.getDatabaseName(), fieldName);
            if (indexService.containsIndex(key)) {
                indexes.put(fieldName, indexService.getIndex(key).get());
            }
        }
        return indexes;
    }

    private void recalculateIndexes(String databaseName) {
        {
            System.out.println("recalculating " + databaseName + " indexes");
        }
        for (IndexKey key : indexService.getIndexesKeys()) {
            if (!key.getDatabaseName().equals(databaseName))
                continue;
            Index index = indexService.getIndex(key).get();
            index.clear();
            indexService.calculateIndex(key, index);
            indexService.saveToFile(key, index);
        }
    }

    private IndexKey createKey(Query request) {
        return new IndexKey(request.getDatabaseName(), request.getIndexFieldName());
    }

    private DatabaseService getDatabaseService() {
        return DatabaseManager.getInstance().getDatabaseService();
    }

}
