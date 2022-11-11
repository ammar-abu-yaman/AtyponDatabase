package com.atypon.project.worker.cache;

import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryHandler;
import com.atypon.project.worker.query.QueryType;
import java.util.*;

public class CacheHandler extends QueryHandler {

    CacheService service;

    public CacheHandler(CacheService service) {
        this.service = service;
    }

    @Override
    public void handle(Query request) {
        switch (request.getQueryType()) {
            case FindDocument:
            case FindDocuments:
                handleRead(request);
                return;
            case CreateDatabase:
            case DeleteDatabase:
                handleDatabaseCreation(request);
                return;
            case AddDocument:
            case DeleteDocument:
            case UpdateDocument:
                handleWrite(request);
                return;
            default:
                pass(request);
                return;
        }
    }


    private void handleRead(Query request) {

        // case where there is no database with the name provided by the request
        if(!service.containsCache(request.getDatabaseName())) {
            pass(request);
            return;
        }

        Cache<CacheEntry, String> cache = service.getCache(request.getDatabaseName());
        CacheEntry entry = new CacheEntry(request);
        Optional<String> cachedOutput = cache.get(entry);

        // Cache hit case
        if(cachedOutput.isPresent()) {
            request.getRequestOutput().append(cachedOutput.get());
            request.setStatus(Query.Status.Accepted);
            System.out.println("Cache Hit");
            return;
        }

        pass(request);
        if(isFailedRequest(request))
            return;
        System.out.println(request.getUsedDocuments());
        cache.put(new CacheEntry(request), request.getRequestOutput().toString());
    }

    private void handleWrite(Query request) {
        pass(request);

        if(isFailedRequest(request))
            return;

        Cache<CacheEntry, String> cache = service.getCache(request.getDatabaseName());
        cache.clear();
    }

    private void handleDatabaseCreation(Query request) {
        pass(request);
        if(isFailedRequest(request))
            return;
        if(request.getQueryType() == QueryType.CreateDatabase)
            service.createCache(request.getDatabaseName());
        else if(request.getQueryType() == QueryType.DeleteDatabase)
            service.deleteCache(request.getDatabaseName());
    }

    private boolean isFailedRequest(Query request) {
        return request.getStatus() != Query.Status.Accepted;
    }


}
