package com.atypon.project.worker.brodcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import static java.lang.String.format;

import java.util.List;

public class BroadcastHandler extends RequestHandler {

    DatabaseManager manager = DatabaseManager.getInstance();
    ObjectMapper mapper = new ObjectMapper();
    List<Node> nodes = manager.getConfiguration().getNodes();

    final static String URL = "http:%s:8080/_internal/%s/%s";

    @Override
    public void handleRequest(DatabaseRequest request) {
        switch (request.getRequestType()) {
            case AddDocument:
                addDocument(request);
                return;
        }
    }

    public void addDocument(DatabaseRequest request) {
        String payload = request.getPayload().toString();
        String databaseName = request.getDatabaseName();
        // async broadcasting
        Thread thread = new Thread(() -> {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(payload, headers);
            for(Node node: nodes)
                new RestTemplate().postForEntity(format(URL, node.getAddress(), "add_document", databaseName), entity, String.class);
        });
        thread.setDaemon(true);
        thread.start();
        request.setStatus(DatabaseRequest.Status.Accepted);
    }
}
