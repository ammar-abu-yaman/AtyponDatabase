package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpSession;

@RestController
public class DatabaseController {

    DatabaseManager manager = DatabaseManager.getInstance();

    @PostMapping("/database/create/{database}")
    public String createDatabase(@PathVariable("database") String databaseName) {
        DatabaseRequest request = DatabaseRequest.builder()
                .originator(DatabaseRequest.Originator.User)
                .requestType(RequestType.CreateDatabase)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

    @PostMapping("/database/delete/{database}")
    public String deleteDatabase(@PathVariable("database") String databaseName) {
        DatabaseRequest request = DatabaseRequest.builder()
                .originator(DatabaseRequest.Originator.User)
                .requestType(RequestType.DeleteDatabase)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

}
