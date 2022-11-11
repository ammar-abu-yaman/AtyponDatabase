package com.atypon.project.worker.brodcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import static java.lang.String.format;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BroadcastHandler extends QueryHandler {

    DatabaseManager manager = DatabaseManager.getInstance();
    ObjectMapper mapper = new ObjectMapper();
    List<Node> nodes = manager.getConfiguration().getNodes();

    final static String URL = "http://%s:8080/_internal/%s/%s";

    @Override
    public void handle(Query request) {
        switch (request.getQueryType()) {
            case AddDocument:
                addDocument(request);
                return;
            case DeleteDocument:
                deleteDocument(request);
                return;
            case UpdateDocument:
                updateDocument(request);
                return;
            case DeleteDatabase:
                deleteDatabase(request);
                return;
            case CreateDatabase:
                createDatabase(request);
                return;
            case CreateIndex:
                createIndex(request);
                return;
            case DeleteIndex:
                deleteIndex(request);
                return;

        }
    }

    private void updateDocument(Query query) {
        JsonNode oldData = query.getOldData();
        JsonNode payload = query.getOriginator() == Query.Originator.User ? query.getPayload() : query.getPayload().get("payload");
        ObjectNode body = mapper.createObjectNode();
        body.set("old", oldData);
        body.set("payload", payload);

        // node have affinity then do a regular broadcast to all other nodes
        if(query.hasAffinity()) {
            broadcast("update_document", query.getDatabaseName(), body.toString());
            query.setStatus(Query.Status.Accepted);
            return;
        }
        // node doesn't have affinity, defer the write to the node with the affinity for the document
        String documentId = oldData.get("_id").asText();
        Node node = nodes.stream().filter(n -> n.getId().equals(documentId)).findFirst().get();
        try {
            ResponseEntity<String> response = defer(body.toString(), node);
            while(response.getStatusCode() != HttpStatus.ACCEPTED) {
                JsonNode newData = mapper.readTree(response.getBody());
                Query rewrite = Query.builder()
                        .originator(Query.Originator.SelfUpdate)
                        .databaseName(query.getDatabaseName())
                        .payload(newData)
                        .build();
                // update self with new value
                manager.getHandlersFactory().getHandler(rewrite).handle(rewrite);
                // replace old data with the rewrite old data
                body.replace("old", rewrite.getOldData());
                // try to defer again
                response = defer(body.toString(), node);
            }

            query.setStatus(Query.Status.Accepted);
            return;
        }
        catch (RestClientException | JsonProcessingException e) { // network error or json parse error
            query.setStatus(Query.Status.Rejected);
            e.printStackTrace();
            return;
        }

    }

    private ResponseEntity<String> defer(String body, Node node) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        return new RestTemplate().postForEntity(format(URL, node.getAddress(), "defer_update", ""), entity, String.class);
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
        broadcast("create_database", request.getDatabaseName(), "{}");
        request.setStatus(Query.Status.Accepted);
    }

    private void deleteDatabase(Query request) {
        broadcast("delete_database", request.getDatabaseName(), "{}");
        request.setStatus(Query.Status.Accepted);
    }

    // async broadcasting
    private void broadcast(String action, String info, String body) {
        launch(() -> sendToNodes(action, info, body));
    }

    public void addDocument(Query request) {
        String payload = request.getPayload().toString();
        String databaseName = request.getDatabaseName();
        // async broadcasting

        broadcast("add_document", databaseName, payload);
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

    private void sendToNodes(String action, String info, String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        for (Node node : nodes) {
            try {
                new RestTemplate().postForEntity(format(URL, node.getAddress(), action, info), entity,
                        String.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void launch(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
    }
    
}
