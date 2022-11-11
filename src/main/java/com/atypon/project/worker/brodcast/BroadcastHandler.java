package com.atypon.project.worker.brodcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
    public void handleRequest(DatabaseRequest request) {
        switch (request.getRequestType()) {
            case AddDocument:
                addDocument(request);
                return;
            case DeleteDocument:
                deleteDocument(request);
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

        }
    }

    private void createIndex(DatabaseRequest request) {
        broadcastHelper(request, "create_index", request.getDatabaseName() + "/" + request.getIndexFieldName());
    }

    private void deleteIndex(DatabaseRequest request) {
        broadcastHelper(request, "delete_index", request.getDatabaseName() + "/" + request.getIndexFieldName());
    }

    private void createDatabase(DatabaseRequest request) {
        broadcastHelper(request, "create_database", request.getDatabaseName());
    }

    private void deleteDatabase(DatabaseRequest request) {
        broadcastHelper(request, "delete_database", request.getDatabaseName());
    }

    private void broadcastHelper(DatabaseRequest request, String action, String info) {
        // async broadcasting
        broadcast(() -> {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>("{}", headers);
            for (Node node : nodes) {
                try {
                    new RestTemplate().postForEntity(format(URL, node.getAddress(), action, info), entity,
                            String.class);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        request.setStatus(DatabaseRequest.Status.Accepted);
    }

    public void addDocument(DatabaseRequest request) {
        String payload = request.getPayload().toString();
        String databaseName = request.getDatabaseName();
        // async broadcasting
        broadcast(() -> {
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
        request.setStatus(DatabaseRequest.Status.Accepted);
    }

    public void deleteDocument(DatabaseRequest request) {
        List<String> ids = request.getUsedDocuments().stream().collect(Collectors.toList());
        String databaseName = request.getDatabaseName();
        // async broadcasting
        broadcast(() -> {
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
        request.setStatus(DatabaseRequest.Status.Accepted);
    }

    private void broadcast(Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setDaemon(true);
        thread.start();
    }


}
