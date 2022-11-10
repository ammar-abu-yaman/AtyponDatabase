package com.atypon.project.worker.request;

import com.atypon.project.worker.auth.LoginHandler;
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
        if(request.getRequestType() == RequestType.Login) {
            handlerChain.setNextHandler(new LoginHandler());
            return handlerChain;
        }

        handlerChain
                .setNextHandler(manager.getCacheService().getHandler())
                .setNextHandler(manager.getIndexService().getHandler())
                .setNextHandler(manager.getDatabaseService().getHandler())
                .setNextHandler(new BroadcastHandler());


        return handlerChain;
    }

    private RequestHandler getBroadcastHandler(DatabaseRequest request) {
        RequestHandler handlerChain = manager.getLockService().getHandler();
        handlerChain
                .setNextHandler(manager.getCacheService().getHandler())
                .setNextHandler(manager.getIndexService().getHandler())
                .setNextHandler(manager.getDatabaseService().getHandler())
                .setNextHandler(new BroadcastHandler());
        return handlerChain;
    }

    //TODO: implement deferred handler
    private RequestHandler getDeferrerHandler(DatabaseRequest request) {
        return null;
    }


    private boolean oneOf(RequestType type, RequestType... types) {
        return Arrays.stream(types).anyMatch(t -> type == t);
    }


}
