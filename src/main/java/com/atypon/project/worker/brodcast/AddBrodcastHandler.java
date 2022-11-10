package com.atypon.project.worker.brodcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;
import static java.lang.String.format;

import java.util.List;

public class AddBrodcastHandler extends RequestHandler {

    DatabaseManager manager = DatabaseManager.getInstance();
    final static String URL = "http:%s:8080/_internal/add_document";

    @Override
    public void handleRequest(DatabaseRequest request) {
        List<Node> nodes = manager.getConfiguration().getNodes();
        String thisId = manager.getConfiguration().getNodeId();

        nodes
                .stream()
                .filter(node -> !node.getId().equals(thisId))
                .forEach(node -> {
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.APPLICATION_JSON);
                    HttpEntity<String> entity = new HttpEntity<>(request.getPayload().toString(), headers);
                    new RestTemplate().postForEntity(format(URL, node.getAddress()), entity, String.class);
                });
        request.setStatus(DatabaseRequest.Status.Accepted);

    }
}
