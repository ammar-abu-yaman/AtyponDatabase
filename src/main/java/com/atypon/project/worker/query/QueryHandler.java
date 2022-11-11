package com.atypon.project.worker.query;


public abstract class QueryHandler {

    protected QueryHandler nextHandler;

    public abstract void handle(Query request);

    public void pass(Query request) {
        if(nextHandler != null)
            nextHandler.handle(request);
    }

    public QueryHandler setNext(QueryHandler handler) {
        nextHandler = handler;
        return handler;
    }
}
