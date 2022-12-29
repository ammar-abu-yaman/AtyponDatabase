package com.atypon.project.worker.handler.database;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class CreationHandler extends QueryHandler {

    DatabaseService databaseService = DatabaseManager.getInstance().getDatabaseService();

    @Override
    public void handle(Query query) {
        if(query.getQueryType() == QueryType.CreateDatabase)
            createDatabase(query);
        else
            deleteDatabase(query);
    }

    private void createDatabase(Query query) {
        if (databaseService.containsDatabase(query.getDatabaseName())) {
            query.getRequestOutput().append("Database Already Exists");
            query.setStatus(Query.Status.Rejected);
            return;
        }
        databaseService.createDatabase(query.getDatabaseName());
        pass(query);
    }

    private void deleteDatabase(Query request) {
        Database database = databaseService.getDatabase(request.getDatabaseName());
        List<String> ids = database.getAllDocuments()
                .map(document -> document.get("_id").asText())
                .collect(Collectors.toList());
        DatabaseUtil.decrementAffinity(database, ids);
        databaseService.deleteDatabase(request.getDatabaseName());
        pass(request);
    }


}
