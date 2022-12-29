package com.atypon.project.worker.handler;

import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.atypon.project.worker.core.DatabaseManager;
import java.util.Arrays;

public class HandlerFactory {
    DatabaseManager manager = DatabaseManager.getInstance();

    public QueryHandler getHandler(Query query) {
        switch (query.getOriginator()) {
            case User:
                return getUserHandler(query);
            case Broadcaster:
                return getBroadcastHandler(query);
            case Deferrer:
                return getDeferrerHandler(query);
            case SelfUpdate:
                return getSelfUpdateHandler(query);
        }
        return null;
    }

    private QueryHandler getSelfUpdateHandler(Query query) {
        QueryHandler handlerChain = manager.getCacheService().getHandler();
        handlerChain
                .setNext(manager.getIndexService().getHandler())
                .setNext(manager.getDatabaseService().getHandler());
        return handlerChain;
    }

    private QueryHandler getUserHandler(Query request) {
        QueryHandler handlerChain = manager.getLockService().getHandler();

        // case of a login request
        if(request.getQueryType() == QueryType.Login) {
            handlerChain.setNext(new LoginHandler());
            return handlerChain;
        }

        if(request.getQueryType() == QueryType.RegisterUser) {
            handlerChain
                    .setNext(new RegisterHandler())
                    .setNext(manager.getIndexService().getHandler())
                    .setNext(manager.getDatabaseService().getHandler())
                    .setNext(new BroadcastHandler());
            return handlerChain;
        }

        // case of creating of deleting indexes
        if(oneOf(request, QueryType.CreateIndex, QueryType.DeleteIndex)) {
            handlerChain
                    .setNext(manager.getCacheService().getHandler())
                    .setNext(manager.getIndexService().getHandler())
                    .setNext(new BroadcastHandler());
        } else {
            handlerChain
                    .setNext(manager.getCacheService().getHandler())
                    .setNext(new SchemaHandler())
                    .setNext(manager.getIndexService().getHandler())
                    .setNext(manager.getDatabaseService().getHandler())
                    .setNext(new BroadcastHandler());
        }
        return handlerChain;
    }

    private QueryHandler getBroadcastHandler(Query query) {
        QueryHandler handlerChain = manager.getLockService().getHandler();

        // case of creating of deleting indexes
        if(oneOf(query, QueryType.CreateIndex, QueryType.DeleteIndex)) {
            handlerChain
                    .setNext(manager.getCacheService().getHandler())
                    .setNext(manager.getIndexService().getHandler());
        } else {
            handlerChain
                    .setNext(manager.getCacheService().getHandler())
                    .setNext(new SchemaHandler())
                    .setNext(manager.getIndexService().getHandler())
                    .setNext(manager.getDatabaseService().getHandler());
        }
        return handlerChain;
    }

    private QueryHandler getDeferrerHandler(Query query) {
        QueryHandler handlerChain = manager.getLockService().getHandler();
        handlerChain
                .setNext(manager.getCacheService().getHandler())
                .setNext(new SchemaHandler())
                .setNext(manager.getIndexService().getHandler())
                .setNext(manager.getDatabaseService().getHandler())
                .setNext(new BroadcastHandler());
        return handlerChain;
    }


    private boolean oneOf(Query request, QueryType... types) {
        return Arrays.stream(types).anyMatch(t -> request.getQueryType() == t);
    }


}
