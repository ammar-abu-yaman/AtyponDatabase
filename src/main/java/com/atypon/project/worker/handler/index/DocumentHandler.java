package com.atypon.project.worker.handler.index;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.index.Index;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class DocumentHandler extends QueryHandler {

    ObjectMapper mapper = new ObjectMapper();
    IndexService indexService = DatabaseManager.getInstance().getIndexService();

    @Override
    public void handle(Query request) {
        if(request.getQueryType() == QueryType.AddDocument)
            handleAdd(request);
        else if(request.getQueryType() == QueryType.DeleteDocument)
            handleDelete(request);
        else
            handleUpdate(request);
    }


    private void handleAdd(Query request) {
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
        if (request.getOriginator() == Query.Originator.User)
            key = new IndexKey(request.getDatabaseName(), filterKey.getKey());

        if (indexService.containsIndex(key)) {
            Index index = indexService.getIndex(key).get();
            request.setIndex(index);
        }


        pass(request);
        if (request.getStatus() != Query.Status.Accepted)
            return;

        removeFromIndex(request.getDatabaseName(), request.getOldData(), request.getOldData().get("_id").asText());
        if(request.getOriginator() == Query.Originator.SelfUpdate || request.getOriginator() == Query.Originator.User) {
            addToIndex(request.getDatabaseName(), request.getPayload(), request.getOldData().get("_id").asText());
        } else if(request.getOriginator() == Query.Originator.Broadcaster || request.getOriginator() == Query.Originator.Deferrer) {
            addToIndex(request.getDatabaseName(), request.getPayload().get("payload"), request.getOldData().get("_id").asText());
        }
    }

    private void handleDelete(Query request) {
        Entry<String, JsonNode> filterKey = request.getFilterKey();

        IndexKey key = null;

        // there is only a filter key in user requests
        if (request.getOriginator() == Query.Originator.User)
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

}
