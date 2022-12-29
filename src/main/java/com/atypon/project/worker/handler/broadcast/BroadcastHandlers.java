package com.atypon.project.worker.handler.broadcast;

import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;

public class BroadcastHandlers {

    public static QueryHandler getHandler(Query query, QueryHandler nextHandler) {
        QueryHandler handler = query.getQueryType() == QueryType.UpdateDocument
                ? new UpdateBroadcast()
                : new SimpleBroadcast();
        handler.setNext(nextHandler);
        return handler;
    }

}
