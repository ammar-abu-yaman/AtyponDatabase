package com.atypon.project.worker.request;

import com.atypon.project.worker.user.LoginHandler;
import com.atypon.project.worker.brodcast.BroadcastHandler;
import com.atypon.project.worker.core.DatabaseManager;

import java.util.Arrays;

public class HandlerFactory {
    DatabaseManager manager = DatabaseManager.getInstance();

    public RequestHandler getHandler(DatabaseRequest request) {
        switch (request.getOriginator()) {
            case User:
                return getUserHandler(request);
            case Broadcaster:
                return getBroadcastHandler(request);
            case Deferrer:
                return getDeferrerHandler(request);
        }

        return null;
    }

    private RequestHandler getUserHandler(DatabaseRequest request) {
        RequestHandler handlerChain = manager.getLockService().getHandler();

        // case of a login request
        if(request.getRequestType() == RequestType.Login) {
            handlerChain.setNextHandler(new LoginHandler());
            return handlerChain;
        }

        // case of creating of deleting indexes
        if(oneOf(request, RequestType.CreateIndex, RequestType.DeleteIndex)) {
            handlerChain
                    .setNextHandler(manager.getCacheService().getHandler())
                    .setNextHandler(manager.getIndexService().getHandler())
                    .setNextHandler(new BroadcastHandler());
        } else {
            handlerChain
                    .setNextHandler(manager.getCacheService().getHandler())
                    .setNextHandler(manager.getIndexService().getHandler())
                    .setNextHandler(manager.getDatabaseService().getHandler())
                    .setNextHandler(new BroadcastHandler());
        }
        return handlerChain;
    }

    private RequestHandler getBroadcastHandler(DatabaseRequest request) {
        RequestHandler handlerChain = manager.getLockService().getHandler();

        // case of creating of deleting indexes
        if(oneOf(request, RequestType.CreateIndex, RequestType.DeleteIndex)) {
            handlerChain
                    .setNextHandler(manager.getCacheService().getHandler())
                    .setNextHandler(manager.getIndexService().getHandler());
        } else {
            handlerChain
                    .setNextHandler(manager.getCacheService().getHandler())
                    .setNextHandler(manager.getIndexService().getHandler())
                    .setNextHandler(manager.getDatabaseService().getHandler());
        }
        return handlerChain;
    }

    //TODO: implement deferred handler
    private RequestHandler getDeferrerHandler(DatabaseRequest request) {
        return null;
    }


    private boolean oneOf(DatabaseRequest request, RequestType... types) {
        return Arrays.stream(types).anyMatch(t -> request.getRequestType() == t);
    }


}
