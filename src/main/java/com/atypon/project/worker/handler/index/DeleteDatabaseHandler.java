package com.atypon.project.worker.handler.index;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.query.Query;

public class DeleteDatabaseHandler extends QueryHandler {

    IndexService indexService = DatabaseManager.getInstance().getIndexService();

    @Override
    public void handle(Query query) {
        pass(query);
        if (query.getStatus() == Query.Status.Rejected)
            return;
        indexService.deleteDatabaseIndices(query.getDatabaseName());
    }


}
