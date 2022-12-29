package com.atypon.project.worker.handler.database;

import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;

import java.util.Map;

public class DatabaseHandlers {

    public static QueryHandler getHandler(Query query, QueryHandler nextHandler) {
        QueryHandler handler = null;
        switch (query.getQueryType()) {
            case CreateDatabase:
            case DeleteDatabase:
                handler = new CreationHandler();
                break;
            case AddDocument:
            case DeleteDocument:
            case UpdateDocument:
                handler = new DocumentHandler();
                break;
            case FindDocument:
            case FindDocuments:
                handler = new FindDocumentHandler();
                break;
            case RegisterUser:
                handler = new RegisterUserHandler();
                break;
        }

        handler.setNext(nextHandler);
        return handler;
    }
}
