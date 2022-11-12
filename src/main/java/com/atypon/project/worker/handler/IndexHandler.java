package com.atypon.project.worker.handler;

import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.index.Index;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.query.Query.Originator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.util.*;
import java.util.stream.Collectors;

public class IndexHandler extends QueryHandler {

    private IndexService indexService;
    ObjectMapper mapper = new ObjectMapper();

    public IndexHandler(IndexService indexService) {
        this.indexService = indexService;
    }

    @Override
    public void handle(Query query) {
        switch (query.getQueryType()) {
            case FindDocument:
            case FindDocuments:
                handleRead(query);
                return;
            case AddDocument:
                handleAddDocument(query);
                return;
            case UpdateDocument:
                handleUpdate(query);
                return;
            case DeleteDocument:
                handleDelete(query);
                return;
            case CreateIndex:
                handleCreateIndex(query);
                return;
            case DeleteIndex:
                handleDeleteIndex(query);
                return;
            case DeleteDatabase:
                handleDeleteDatabase(query);
                return;
            case RegisterUser:
                handlerRegisterUser(query);
                return;
            default:
                pass(query);
                return;
        }
    }

    private void handlerRegisterUser(Query query) {
        IndexKey key = new IndexKey("_Users", "username");
        Index index = indexService.getIndex(key).get();
        query.setIndex(index);

        pass(query);
        index.add(query.getPayload().get("username"), query.getPayload().get("_id").asText());
        indexService.saveToFile(key, index);
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

        IndexKey key = null;

        // there is only a filter key in user requests
        if (request.getOriginator() == Originator.User)
            key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if (indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }


        pass(request);
        if (request.getStatus() != Query.Status.Accepted)
            return;

        removeFromIndex(request.getDatabaseName(), request.getOldData(), request.getOldData().get("_id").asText());
        if(request.getOriginator() == Query.Originator.SelfUpdate || request.getOriginator() == Originator.User) {
            addToIndex(request.getDatabaseName(), request.getPayload(), request.getOldData().get("_id").asText());
        } else if(request.getOriginator() == Originator.Broadcaster || request.getOriginator() == Originator.Deferrer) {
            addToIndex(request.getDatabaseName(), request.getPayload().get("payload"), request.getOldData().get("_id").asText());
        }
    }

    private void handleDelete(Query request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();

        IndexKey key = null;

        // there is only a filter key in user requests
        if (request.getOriginator() == Originator.User)
            key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if (indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }

        pass(request);
        if (request.getStatus() != Query.Status.Accepted)
            return;

        try {
            List<JsonNode> oldData = mapper.readValue(request.getOldData().toString(), TypeFactory.defaultInstance().constructCollectionType(List.class, JsonNode.class));
            for(JsonNode json: oldData)
                removeFromIndex(request.getDatabaseName(), json, json.get("_id").asText());
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }

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

    private void removeFromIndex(String databaseName, JsonNode document, String documentIndex) {
        for (Iterator<String> it = document.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            IndexKey key = new IndexKey(databaseName, field);
            if(!indexService.containsIndex(key))
                continue;
            Index index = indexService.getIndex(key).get();
            index.delete(document.get(field), documentIndex);
            indexService.saveToFile(key, index);
        }
    }

    private void addToIndex(String databaseName, JsonNode document, String documentIndex) {
        for (Iterator<String> it = document.fieldNames(); it.hasNext(); ) {
            String field = it.next();
            IndexKey key = new IndexKey(databaseName, field);
            if(!indexService.containsIndex(key))
                continue;
            Index index = indexService.getIndex(key).get();
            index.add(document.get(field), documentIndex);
            indexService.saveToFile(key, index);
        }
    }

//    private void recalculateIndexes(String databaseName) {
//        {
//            System.out.println("recalculating " + databaseName + " indexes");
//        }
//        for (IndexKey key : indexService.getIndexesKeys()) {
//            if (!key.getDatabaseName().equals(databaseName))
//                continue;
//            Index index = indexService.getIndex(key).get();
//            index.clear();
//            indexService.calculateIndex(key, index);
//            indexService.saveToFile(key, index);
//        }
//    }

    private IndexKey createKey(Query request) {
        return new IndexKey(request.getDatabaseName(), request.getIndexFieldName());
    }

    private DatabaseService getDatabaseService() {
        return DatabaseManager.getInstance().getDatabaseService();
    }

}
