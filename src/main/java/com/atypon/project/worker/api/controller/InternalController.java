package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@RestController
public class InternalController {

    DatabaseManager manager = DatabaseManager.getInstance();
    ObjectMapper mapper = new ObjectMapper();
    List<Node> nodes = DatabaseManager.getInstance().getConfiguration().getNodes();

    @PostMapping("/_internal/add_document/{database}")
    public String addDocument(HttpServletRequest httpRequest,
            @RequestBody Map<String, Object> requestBody,
            @PathVariable("database") String databaseName) {
        if (!validateNode(httpRequest))
            return "Rejected";
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(QueryType.AddDocument)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    @PostMapping("/_internal/delete_document/{database}")
    public String deleteDocument(HttpServletRequest httpRequest,
            @RequestBody Map<String, Object> requestBody,
            @PathVariable("database") String databaseName) {
        if (!validateNode(httpRequest))
            return "Rejected";
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(QueryType.DeleteDocument)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    @PostMapping("/_internal/update_document/{database}")
    public String updateDocument(HttpServletRequest httpRequest, @RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        if (!validateNode(httpRequest))
            return "Rejected";

        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .queryType(QueryType.UpdateDocument)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }
    @PostMapping("/_internal/defer_update/{database}")
    public String updateDefer(HttpServletRequest httpRequest, HttpServletResponse resp, @RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        if (!validateNode(httpRequest))
            return "Rejected";
        Query query = Query.builder()
                .originator(Query.Originator.Deferrer)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .queryType(QueryType.UpdateDocument)
                .build();
        manager.getHandlersFactory().getHandler(query).handle(query);
        if(query.getStatus() == Query.Status.Accepted) {
            return "";
        } else {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return query.getOldData().toString();
        }
    }


    @PostMapping("/_internal/create_database/{database}")
    public String createDatabase(HttpServletRequest httpRequest, @PathVariable("database") String databaseName) {
        return databaseHelper(httpRequest, databaseName, QueryType.CreateDatabase);
    }

    @PostMapping("/_internal/delete_database/{database}")
    public String deleteDatabase(HttpServletRequest httpRequest, @PathVariable("database") String databaseName) {
        return databaseHelper(httpRequest, databaseName, QueryType.DeleteDatabase);
    }

    @PostMapping("/_internal/create_index/{database}/{index}")
    public String createIndex(HttpServletRequest httpRequest, @PathVariable("database") String database,
            @PathVariable("index") String index) {
        return indexHelper(httpRequest, database, index, QueryType.CreateIndex);
    }

    @PostMapping("/_internal/delete_index/{database}/{index}")
    public String deleteIndex(HttpServletRequest httpRequest, @PathVariable("database") String database,
            @PathVariable("index") String index) {
        return indexHelper(httpRequest, database, index, QueryType.DeleteIndex);
    }

    private String databaseHelper(HttpServletRequest httpRequest, String databaseName, QueryType type) {
        if (!validateNode(httpRequest))
            return "Rejected";
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(type)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    private String indexHelper(HttpServletRequest httpRequest, String database, String index, QueryType type) {
        if (!validateNode(httpRequest))
            return "Rejected";
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(type)
                .databaseName(database)
                .indexFieldName(index)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    boolean validateNode(HttpServletRequest httpRequest) {
        String address = httpRequest.getRemoteAddr();
        return nodes.stream().anyMatch(node -> node.getAddress().equals(address));
    }

}
