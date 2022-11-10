package com.atypon.project.worker.database;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.atypon.project.worker.request.RequestType;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class DatabaseHandler extends RequestHandler {

    private DatabaseService manager;
    private IdCreator idCreator;

    public DatabaseHandler(DatabaseService manager, IdCreator idCreator) {
        this.manager = manager;
        this.idCreator = idCreator;
    }

    @Override
    public void handleRequest(DatabaseRequest request) {
        if(request.getRequestType() != RequestType.CreateDatabase
                && !manager.containsDatabase(request.getDatabaseName())) {
            request.getRequestOutput().append("No Such Database Exists");
            request.setStatus(DatabaseRequest.Status.Rejected);
            return;
        }

        switch (request.getRequestType()) {
            case CreateDatabase:
                createDatabase(request);
                break;
            case DeleteDatabase:
                deleteDatabase(request);
                break;
            case AddDocument:
                addDocument(request);
                break;
            case DeleteDocument:
                deleteDocument(request);
                break;
            case UpdateDocument:
                updateDocument(request);
                break;
            case FindDocument:
                findDocument(request);
                break;
            case FindDocuments:
                findDocuments(request);
                break;
        }
    }

    private void createDatabase(DatabaseRequest request) {
        if(manager.containsDatabase(request.getDatabaseName())) {
            request.getRequestOutput().append("Database Already Exists");
            request.setStatus(DatabaseRequest.Status.Rejected);
            return;
        }
        manager.createDatabase(request.getDatabaseName());
    }

    private void deleteDatabase(DatabaseRequest request) {
        manager.deleteDatabase(request.getDatabaseName());
    }

    private void addDocument(DatabaseRequest request) {
        String documentIndex = idCreator.createId(request);
        JsonNode payload = request.getPayload();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> result = mapper.convertValue(payload, new TypeReference<Map<String, Object>>(){});
        if(!result.containsKey("_id"))
            result.put("_id", documentIndex);
        payload = mapper.valueToTree(result);
        manager.getDatabase(request.getDatabaseName()).addDocument(documentIndex, payload);
        request.setUsedDocuments(new HashSet<>(Arrays.asList(documentIndex)));
    }

    private void findDocument(DatabaseRequest request) {
        Database database = manager.getDatabase(request.getDatabaseName());
        List<String> documentIndices =  indexRequest(request, database)
                .stream()
                .limit(1)
                .collect(Collectors.toList());

        Set<String> usedDocuments = documentIndices.stream().collect(Collectors.toSet());
        request.setUsedDocuments(usedDocuments);
        List<JsonNode> documents = database
                .getDocuments(documentIndices)
                .map(document -> filterFields(document, request.getRequiredProperties()))
                .collect(Collectors.toList());
        if(!documents.isEmpty())
            request.getRequestOutput().append(documents.get(0).toPrettyString());
    }

    private void findDocuments(DatabaseRequest request) {
        Database database = manager.getDatabase(request.getDatabaseName());
        Set<String> documentIndices = indexRequest(request, database).stream().collect(Collectors.toSet());
        Stream<JsonNode> documents = database.getDocuments(documentIndices);
        StringBuilder output = request.getRequestOutput();
        request.setUsedDocuments(documentIndices);
        output.append("[\n");
        documents
                .map(document -> filterFields(document, request.getRequiredProperties()))
                .forEach(json -> output.append(json.toPrettyString() + ",\n"));
        if(output.length() > 1)
            output.delete(output.length()-2,output.length()).append("\n");
        output.append("]");
    }

    private void deleteDocument(DatabaseRequest request) {
        Database database = manager.getDatabase(request.getDatabaseName());
        Set<String> usedDocuments = new HashSet<>();
        indexRequest(request, database)
                .stream()
                .limit(1)
                .forEach(documentIndex -> {
                    database.deleteDocument(documentIndex);
                    usedDocuments.add(documentIndex);
                });
        request.setUsedDocuments(usedDocuments);
    }

    private void updateDocument(DatabaseRequest request) {
        Database database = manager.getDatabase(request.getDatabaseName());
        Set<String> usedDocuments = new HashSet<>();
        indexRequest(request, database)
                .stream()
                .limit(1)
                .forEach(documentIndex -> {
                    database.updateDocument(request.getPayload(), documentIndex);
                    usedDocuments.add(documentIndex);
                });
        request.setUsedDocuments(usedDocuments);
    }
    
    private List<String> indexRequest(DatabaseRequest request, Database database) {
        if(request.getIndex() != null) {
            {
                System.out.println("using index for " + request.getFilterKey() );
            }
            return request.getIndex().search(request.getFilterKey().getValue());
        }

        Stream<JsonNode> stream = database.getAllDocuments();
        Entry<String, JsonNode> filterKey = request.getFilterKey();
        if(request.getFilterKey() != null)
            stream = stream
                        .filter(document -> document.has(filterKey.getKey())
                            && document.get(filterKey.getKey()).equals(filterKey.getValue()));
        return stream
                .map(document -> document.get("_id").asText())
                .collect(Collectors.toList());
    }

    private static JsonNode filterFields(JsonNode node, List<String> requiredProperties) {
        if(requiredProperties == null || requiredProperties.isEmpty())
            return node;
        Map<String, Object> properties = new HashMap<>();
        for(String field: requiredProperties) {
            if(!node.has(field))
                continue;
            properties.put(field, node.get(field));
        }

        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(properties);
    }


    
}
