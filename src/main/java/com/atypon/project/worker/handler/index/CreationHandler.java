package com.atypon.project.worker.handler.index;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;

public class CreationHandler extends QueryHandler {

    private IndexService indexService = DatabaseManager.getInstance().getIndexService();
    private DatabaseService databaseService = DatabaseManager.getInstance().getDatabaseService();

    @Override
    public void handle(Query query) {

        if(query.getQueryType() == QueryType.CreateIndex)
            handleCreateIndex(query);
        else
            handleDeleteIndex(query);

    }

    private void handleCreateIndex(Query query) {
        IndexKey key = createKey(query);
        if (indexService.containsIndex(key)) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Index already exists");
            return;
        }

        if (!databaseService.containsDatabase(query.getDatabaseName())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Database Doesn't exist");
            return;
        }

        indexService.createIndex(key);
        pass(query);

        query.setStatus(Query.Status.Accepted);
        return;
    }

    private void handleDeleteIndex(Query query) {
        IndexKey key = createKey(query);
        if (!indexService.containsIndex(key)) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Index doesn't exist");
            return;
        }

        indexService.deleteIndex(key);
        pass(query);

        query.setStatus(Query.Status.Accepted);
    }

    private IndexKey createKey(Query request) {
        return new IndexKey(request.getDatabaseName(), request.getIndexFieldName());
    }
}
