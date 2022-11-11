package com.atypon.project.worker.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;

import java.util.*;
import java.util.stream.Collectors;

public class IndexHandler extends RequestHandler {

    private IndexService indexService;

    public IndexHandler(IndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    public void handleRequest(DatabaseRequest request) {
        switch (request.getRequestType()) {
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
                passRequest(request);
                return;
        }
    }

    private void handleRead(DatabaseRequest request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();
        if(filterKey == null) {
            passRequest(request);
            return;
        }

        IndexKey key = new IndexKey(request.getDatabaseName(), filterKey.getKey());
        if(indexService.containsIndex(key))
            request.setIndex(indexService.getIndex(key).get());
        passRequest(request);
    }

    private void handleAddDocument(DatabaseRequest request) {
        passRequest(request);
        if(request.getStatus() == DatabaseRequest.Status.Rejected)
            return;
        Map<String, Index> affectedIndexes = getAffectedIndexes(request);
        String documentIndex = request.getUsedDocuments().stream().collect(Collectors.toList()).get(0);
        for(Map.Entry<String, Index> entry: affectedIndexes.entrySet()) {
            entry.getValue().add(request.getPayload().get(entry.getKey()), documentIndex);
        }

        for(Map.Entry<String, Index> entry: affectedIndexes.entrySet()) {
            indexService.saveToFile(new IndexKey(request.getDatabaseName(), entry.getKey()), entry.getValue());
        }
    }

    private void handleUpdate(DatabaseRequest request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();

        IndexKey key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if(indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }

        passRequest(request);
        if(request.getStatus() == DatabaseRequest.Status.Accepted)
            recalculateIndexes(request.getDatabaseName());
    }

    private void handleDelete(DatabaseRequest request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();

        IndexKey key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if(indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }

        passRequest(request);
        if(request.getStatus() == DatabaseRequest.Status.Accepted)
            recalculateIndexes(request.getDatabaseName());
    }

    private void handleDeleteDatabase(DatabaseRequest request) {
        passRequest(request);
        if(request.getStatus() == DatabaseRequest.Status.Rejected)
            return;
        indexService.deleteDatabaseIndices(request.getDatabaseName());
    }

    private void handleCreateIndex(DatabaseRequest request) {
        DatabaseService databaseService = getDatabaseService();
        IndexKey key = createKey(request);
        if(indexService.containsIndex(key)) {
            request.setStatus(DatabaseRequest.Status.Rejected);
            request.getRequestOutput().append("Index already exists");
            return;
        }

        if(!databaseService.containsDatabase(request.getDatabaseName())) {
            request.setStatus(DatabaseRequest.Status.Rejected);
            request.getRequestOutput().append("Database Doesn't exist");
            return;
        }

        indexService.createIndex(key);
        passRequest(request);

        request.setStatus(DatabaseRequest.Status.Accepted);
        return;
    }

    private void handleDeleteIndex(DatabaseRequest request) {
        IndexKey key = createKey(request);
        if(!indexService.containsIndex(key)) {
            request.setStatus(DatabaseRequest.Status.Rejected);
            request.getRequestOutput().append("Index doesn't exist");
            return;
        }

        indexService.deleteIndex(key);
        passRequest(request);

        request.setStatus(DatabaseRequest.Status.Accepted);
    }


    private Map<String, Index> getAffectedIndexes(DatabaseRequest request) {
        Map<String, Index> indexes = new HashMap<>();
        Iterator<String> iterator =  request.getPayload().fieldNames();
        while(iterator.hasNext()) {
            String fieldName = iterator.next();
            IndexKey key = new IndexKey(request.getDatabaseName(), fieldName);
            if(indexService.containsIndex(key)) {
                indexes.put(fieldName, indexService.getIndex(key).get());
            }
        }
        return indexes;
    }


    private void recalculateIndexes(String databaseName) {
        {
            System.out.println("recalculating " + databaseName + " indexes");
        }
        for(IndexKey key: indexService.getIndexesKeys()) {
            if(!key.getDatabaseName().equals(databaseName))
                continue;
            Index index = indexService.getIndex(key).get();
            index.clear();
            indexService.calculateIndex(key, index);
            indexService.saveToFile(key, index);
        }
    }



    private IndexKey createKey(DatabaseRequest request) {
        return new IndexKey(request.getDatabaseName(), request.getIndexFieldName());
    }

    private DatabaseService getDatabaseService() {
        return DatabaseManager.getInstance().getDatabaseService();
    }

}
