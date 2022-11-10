package com.atypon.project.worker.request;

import com.atypon.project.worker.auth.LoginHandler;
import com.atypon.project.worker.core.DatabaseManager;

public class HandlerFactory {

    public RequestHandler getHandler(DatabaseRequest request) {
        DatabaseManager manager = DatabaseManager.getInstance();
        RequestHandler handlerChain = manager.getLockService().getHandler();
        if(request.getRequestType() == RequestType.Login)
            handlerChain.setNextHandler(new LoginHandler());
        else
            handlerChain
                .setNextHandler(manager.getCacheService().getHandler())
                .setNextHandler(manager.getIndexService().getHandler())
                .setNextHandler(manager.getDatabaseService().getHandler());
        return handlerChain;
    }

}
