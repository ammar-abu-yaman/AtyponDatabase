package com.atypon.project.worker.handler.broadcast;

import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.atypon.project.worker.handler.broadcast.BroadcastUtils.broadcast;

public class SimpleBroadcast extends QueryHandler {

    ObjectMapper mapper = new ObjectMapper();

    @Override
    public void handle(Query query) {
        switch (query.getQueryType()) {
            case AddDocument:
                addDocument(query);
                return;
            case DeleteDocument:
                deleteDocument(query);
                return;
            case DeleteDatabase:
                deleteDatabase(query);
                return;
            case CreateDatabase:
                createDatabase(query);
                return;
            case CreateIndex:
                createIndex(query);
                return;
            case DeleteIndex:
                deleteIndex(query);
                return;
            case RegisterUser:
                registerUser(query);
                return;
        }
    }

    public void addDocument(Query request) {
        String payload = request.getPayload().toString();
        String databaseName = request.getDatabaseName();
        // async broadcasting

        broadcast("add_document", databaseName, payload);
        request.setStatus(Query.Status.Accepted);
    }

    private void registerUser(Query query) {
        broadcast("add_user", "", query.getPayload().toString());
        query.setStatus(Query.Status.Accepted);
    }

    private void createIndex(Query request) {
        broadcast("create_index", request.getDatabaseName() + "/" + request.getIndexFieldName(), "{}");
        request.setStatus(Query.Status.Accepted);
    }

    private void deleteIndex(Query request) {
        broadcast("delete_index", request.getDatabaseName() + "/" + request.getIndexFieldName(), "{}");
        request.setStatus(Query.Status.Accepted);
    }

    private void createDatabase(Query request) {
        broadcast("create_database", request.getDatabaseName(), request.getPayload().toString());
        request.setStatus(Query.Status.Accepted);
    }

    private void deleteDatabase(Query request) {
        broadcast("delete_database", request.getDatabaseName(), "{}");
        request.setStatus(Query.Status.Accepted);
    }

    public void deleteDocument(Query request) {
        List<String> ids = request.getUsedDocuments().stream().collect(Collectors.toList());
        String databaseName = request.getDatabaseName();
        Map<String, Object> map = new HashMap<>();
        map.put("_ids", ids);
        JsonNode json = mapper.valueToTree(map);

        // async broadcasting
        broadcast("delete_document", databaseName, json.toString());
        request.setStatus(Query.Status.Accepted);
    }

}
