package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.query.Query;

import com.atypon.project.worker.query.QueryType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    DatabaseManager manager = DatabaseManager.getInstance();

    @PostMapping("/index/create/{database}/{index}")
    public String createIndex(@PathVariable("database") String databaseName, @PathVariable("index") String indexFieldName) {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.CreateIndex)
                .databaseName(databaseName)
                .indexFieldName(indexFieldName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return request.getRequestOutput().toString();
    }

    @PostMapping("/index/delete/{database}/{index}")
    public String deleteIndex(@PathVariable("database") String databaseName, @PathVariable("index") String indexFieldName) {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.DeleteIndex)
                .databaseName(databaseName)
                .indexFieldName(indexFieldName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

}
