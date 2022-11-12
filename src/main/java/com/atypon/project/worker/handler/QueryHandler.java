package com.atypon.project.worker.handler;


import com.atypon.project.worker.query.Query;

public abstract class QueryHandler {

    protected QueryHandler nextHandler;

    public abstract void handle(Query query);

    public QueryHandler setNext(QueryHandler handler) {
        nextHandler = handler;
        return handler;
    }

    protected void pass(Query request) {
        if(nextHandler != null)
            nextHandler.handle(request);
    }

    protected boolean updateNotNeeded(Query request){
        return request.getStatus() != Query.Status.Accepted;
    }

}
