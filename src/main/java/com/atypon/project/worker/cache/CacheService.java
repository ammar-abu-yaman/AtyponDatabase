package com.atypon.project.worker.cache;

import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.CacheHandler;
import com.atypon.project.worker.handler.QueryHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CacheService {
    private static final int CACHE_SIZE = 1024;
    private static CacheService INSTANCE;

    public static CacheService getInstance() throws IOException, ClassNotFoundException {
        if(INSTANCE != null)
            return INSTANCE;
        return INSTANCE = new CacheService(CACHE_SIZE, MetaData.getInstance());
    }

    private long databaseCapacity;
    private Map<String, Cache<CacheEntry, String>> caches;

    private CacheService(long databaseCapacity, MetaData metaData) {
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
