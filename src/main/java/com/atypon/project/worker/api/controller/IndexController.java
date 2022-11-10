package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.request.DatabaseRequest;

import com.atypon.project.worker.request.RequestType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class IndexController {

    DatabaseManager manager = DatabaseManager.getInstance();

    @PostMapping("/index/create/{database}/{index}")
    public String createIndex(@PathVariable("database") String databaseName, @PathVariable("index") String indexFieldName) {
        DatabaseRequest request = DatabaseRequest.builder()
                .originator(DatabaseRequest.Originator.User)
                .requestType(RequestType.CreateIndex)
                .databaseName(databaseName)
                .indexFieldName(indexFieldName)
                .build();
        manager.getHandlersFactory().getHandler(request).handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

    @PostMapping("/index/delete/{database}/{index}")
    public String deleteIndex(@PathVariable("database") String databaseName, @PathVariable("index") String indexFieldName) {
        DatabaseRequest request = DatabaseRequest.builder()
                .originator(DatabaseRequest.Originator.User)
                .requestType(RequestType.DeleteIndex)
                .databaseName(databaseName)
                .indexFieldName(indexFieldName)
                .build();
        manager.getHandlersFactory().getHandler(request).handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

}
