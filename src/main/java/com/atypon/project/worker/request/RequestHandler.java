package com.atypon.project.worker.request;


public abstract class RequestHandler {

    protected RequestHandler nextHandler;

    public abstract void handleRequest(DatabaseRequest request);
    public RequestHandler setNextHandler(RequestHandler handler) {
        nextHandler = handler;
        return handler;
    }
    public void passRequest(DatabaseRequest request) {
        if(nextHandler != null)
            nextHandler.handleRequest(request);
    }

}
