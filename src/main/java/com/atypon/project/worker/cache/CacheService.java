package com.atypon.project.worker.cache;

import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.handler.CacheHandler;
import com.atypon.project.worker.handler.QueryHandler;

import java.util.HashMap;
import java.util.Map;

public class CacheService {

    long databaseCapacity;
    Map<String, Cache<CacheEntry, String>> caches;

    public CacheService(long databaseCapacity, MetaData metaData) {
        this.databaseCapacity = databaseCapacity;
        this.caches = new HashMap<>();

        createInitialCaches(metaData);
    }

    private void createInitialCaches(MetaData metaData) {
        metaData
                .getDatabasesNames()
                .forEach(name -> createCache(name));
    }

    public void createCache(String databaseName) {
        caches.put(databaseName, new SyncLRUCache<>(databaseCapacity));
    }

    public void deleteCache(String databaseName) {
        caches.remove(databaseName);
    }

    public Cache<CacheEntry, String> getCache(String databaseName) {
        return caches.get(databaseName);
    }

    public boolean containsCache(String databaseName) {
        return caches.containsKey(databaseName);
    }

    public QueryHandler getHandler() {
        return new CacheHandler(this);
    }

}
