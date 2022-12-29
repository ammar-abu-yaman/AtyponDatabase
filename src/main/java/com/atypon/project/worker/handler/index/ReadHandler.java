package com.atypon.project.worker.handler.index;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.databind.JsonNode;

public class ReadHandler extends QueryHandler {

    private IndexService indexService = DatabaseManager.getInstance().getIndexService();

    @Override
    public void handle(Query query) {
        Entry<String, JsonNode> filterKey = query.getFilterKey();
        if (filterKey == null) {
            pass(query);
            return;
        }

        IndexKey key = new IndexKey(query.getDatabaseName(), filterKey.getKey());
        if (indexService.containsIndex(key))
            query.setIndex(indexService.getIndex(key).get());
        pass(query);
    }
}
