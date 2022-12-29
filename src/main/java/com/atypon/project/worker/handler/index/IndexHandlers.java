package com.atypon.project.worker.handler.index;

import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;

import java.util.Optional;

public class IndexHandlers {

    private IndexHandlers() {}

    public static Optional<QueryHandler> getHandler(Query query, QueryHandler nextHandler) {
        QueryHandler handler = null;
        switch (query.getQueryType()) {
            case FindDocument:
            case FindDocuments:
                handler = new ReadHandler();
                break;
            case AddDocument:
            case UpdateDocument:
            case DeleteDocument:
                handler = new DocumentHandler();
                break;
            case CreateIndex:
            case DeleteIndex:
                handler = new CreationHandler();
                break;
            case DeleteDatabase:
                handler = new DeleteDatabaseHandler();
                break;
            case RegisterUser:
                handler = new RegisterUserHandler();
                break;
        }

        if(handler == null)
            return Optional.empty();

        handler.setNext(nextHandler);
        return Optional.of(handler);
    }


}
