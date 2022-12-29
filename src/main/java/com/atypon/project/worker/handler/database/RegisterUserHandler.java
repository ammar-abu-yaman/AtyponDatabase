package com.atypon.project.worker.handler.database;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.databind.JsonNode;

public class RegisterUserHandler extends QueryHandler {

    private DatabaseService databaseService = DatabaseManager.getInstance().getDatabaseService();

    @Override
    public void handle(Query query) {
        JsonNode user = query.getPayload();
        System.out.println(user.toPrettyString());
        Database users = databaseService.getDatabase("_Users");
        users.addDocument(user.get("_id").asText(), user);
        pass(query);
    }

}
