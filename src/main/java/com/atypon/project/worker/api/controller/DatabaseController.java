package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class DatabaseController {

    ObjectMapper mapper = new ObjectMapper();
    DatabaseManager manager = DatabaseManager.getInstance();

    @PostMapping("/database/create/{database}")
    public String createDatabase(@RequestBody Map<String, Object> schema, @PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.CreateDatabase)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(schema))
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return request.getRequestOutput().toString();
    }

    @PostMapping("/database/delete/{database}")
    public String deleteDatabase(@PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.DeleteDatabase)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return request.getRequestOutput().toString();
    }

}
