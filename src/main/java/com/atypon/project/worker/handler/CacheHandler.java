package com.atypon.project.worker.handler;

import com.atypon.project.worker.cache.Cache;
import com.atypon.project.worker.cache.CacheEntry;
import com.atypon.project.worker.cache.CacheService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import java.util.*;

public class CacheHandler extends QueryHandler {

    CacheService service;

    public CacheHandler(CacheService service) {
        this.service = service;
    }

    @Override
    public void handle(Query query) {
        switch (query.getQueryType()) {
            case FindDocument:
            case FindDocuments:
                handleRead(query);
                return;
            case CreateDatabase:
            case DeleteDatabase:
                handleDatabaseCreation(query);
                return;
            case AddDocument:
            case DeleteDocument:
            case UpdateDocument:
                handleWrite(query);
                return;
            default:
                pass(query);
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
        if(updateNotNeeded(request))
            return;
        System.out.println(request.getUsedDocuments());
        cache.put(new CacheEntry(request), request.getRequestOutput().toString());
    }

    private void handleWrite(Query request) {
        pass(request);

        if(updateNotNeeded(request))
            return;

        // clear cache if exists
        if(service.containsCache(request.getDatabaseName())) {
            Cache<CacheEntry, String> cache = service.getCache(request.getDatabaseName());
            cache.clear();
        }
    }

    private void handleDatabaseCreation(Query request) {
        pass(request);
        if(updateNotNeeded(request))
            return;
        if(request.getQueryType() == QueryType.CreateDatabase)
            service.createCache(request.getDatabaseName());
        else if(request.getQueryType() == QueryType.DeleteDatabase)
            service.deleteCache(request.getDatabaseName());
    }

}
