package com.atypon.project.worker.handler.index;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.index.Index;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.query.Query;

public class RegisterUserHandler extends QueryHandler {

    IndexService indexService = DatabaseManager.getInstance().getIndexService();

    @Override
    public void handle(Query query) {
        // set the index to the name of the user
        IndexKey key = new IndexKey("_Users", "username");
        Index index = indexService.getIndex(key).get();
        query.setIndex(index);

        pass(query);
        index.add(query.getPayload().get("username"), query.getPayload().get("_id").asText());
        indexService.saveToFile(key, index);
    }
}
