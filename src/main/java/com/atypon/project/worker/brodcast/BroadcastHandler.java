package com.atypon.project.worker.brodcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.request.Query;
import com.atypon.project.worker.request.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import static java.lang.String.format;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BroadcastHandler extends RequestHandler {

    DatabaseManager manager = DatabaseManager.getInstance();
    ObjectMapper mapper = new ObjectMapper();
    List<Node> nodes = manager.getConfiguration().getNodes();

    final static String URL = "http://%s:8080/_internal/%s/%s";

    @Override
    public void handleRequest(Query request) {
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
        JsonNode payload = query.getPayload();
        ObjectNode body = mapper.createObjectNode();
        body.set("old", oldData);
        body.set("payload", payload);
        if(query.hasAffinity()) {
            launch(() -> {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
                for (Node node : nodes) {
                    try {
                        new RestTemplate().postForEntity(format(URL, node.getAddress(), "update_document", query.getDatabaseName()), entity, String.class);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        } else {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(body.toString(), headers);
            for (Node node : nodes) {
                try {
                    new RestTemplate().postForEntity(format(URL, node.getAddress(), "update_document", query.getDatabaseName()), entity, String.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
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

    private void broadcast(String action, String info, String body) {
        // async broadcasting
        launch(() -> sendToNodes(action, info, body));
    }

    public void addDocument(Query request) {
        String payload = request.getPayload().toString();
        String databaseName = request.getDatabaseName();
        // async broadcasting
        launch(() -> {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(payload, headers);
            for (Node node : nodes) {
                try {
                    new RestTemplate().postForEntity(format(URL, node.getAddress(), "add_document", databaseName),
                            entity, String.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    public void deleteDocument(Query request) {
        List<String> ids = request.getUsedDocuments().stream().collect(Collectors.toList());
        String databaseName = request.getDatabaseName();
        // async broadcasting
        launch(() -> {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            Map<String, Object> map = new HashMap<>();
            map.put("_ids", ids);
            JsonNode json = mapper.valueToTree(map);
            HttpEntity<String> entity = new HttpEntity<>(json.toString(), headers);
            for (Node node : nodes) {
                try {
                    new RestTemplate().postForEntity(format(URL, node.getAddress(), "delete_document", databaseName),
                            entity, String.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
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
