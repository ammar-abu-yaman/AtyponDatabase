package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class InternalController {

    DatabaseManager manager = DatabaseManager.getInstance();
    ObjectMapper mapper = new ObjectMapper();
    List<Node> nodes = DatabaseManager
            .getInstance()
            .getConfiguration()
            .getNodes()
            .stream()
            .filter(node -> !node.getId().equals(DatabaseManager.getInstance().getConfiguration().getNodeId()))
            .collect(Collectors.toList());

    @PostMapping("/_internal/add_document/{database}")
    public String addDocument(HttpServletRequest httpRequest,
                              @RequestBody Map<String, Object> requestBody,
                              @PathVariable("database") String databaseName) {
        if(!validateNode(httpRequest))
            return "Rejected";
        DatabaseRequest request = DatabaseRequest.builder()
                .originator(DatabaseRequest.Originator.Broadcaster)
                .requestType(RequestType.AddDocument)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .build();
        manager.getHandlersFactory().getHandler(request).handleRequest(request);
        return "Accepted";
    }


    boolean validateNode(HttpServletRequest httpRequest) {
        String address = httpRequest.getRemoteAddr();
        return nodes.stream().anyMatch(node -> node.getAddress().equals(address));
    }


}
