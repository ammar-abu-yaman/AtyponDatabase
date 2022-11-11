package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DatabaseController {

    DatabaseManager manager = DatabaseManager.getInstance();

    @PostMapping("/database/create/{database}")
    public String createDatabase(@PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.CreateDatabase)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

    @PostMapping("/database/delete/{database}")
    public String deleteDatabase(@PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.DeleteDatabase)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

}
