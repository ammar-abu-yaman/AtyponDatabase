package com.atypon.project.worker.handler.schema;

import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;

import java.util.Optional;

public class SchemaHandlers {

    public static Optional<QueryHandler> getHandlers(Query query, QueryHandler nextHandler) {

        QueryHandler handler = null;

        switch (query.getQueryType()) {
            case CreateDatabase:
            case DeleteDatabase:
                handler = new CreationHandler();
                break;
            case AddDocument:
            case UpdateDocument:
                handler = new DocumentHandler();
                break;
        }

        if(handler != null) {
            handler.setNext(nextHandler);
            return Optional.of(handler);
        } else {
            return Optional.empty();
        }
    }

}
