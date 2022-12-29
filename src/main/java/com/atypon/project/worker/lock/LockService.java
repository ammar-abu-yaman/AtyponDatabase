package com.atypon.project.worker.lock;

import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.LockHandler;
import com.atypon.project.worker.handler.QueryHandler;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LockService {

    private final ReadWriteLock globalLock;
    private HashMap<String, ReadWriteLock> locks;

    private static LockService INSTANCE;

    public static LockService getInstance() throws IOException, ClassNotFoundException {
        if(INSTANCE != null)
            return INSTANCE;
        return INSTANCE = new LockService(MetaData.getInstance());
    }

    private LockService(MetaData metaData) {
        globalLock = new ReentrantReadWriteLock();
        locks = new HashMap<>();
        createInitialLocks(metaData);
    }

    private void createInitialLocks(MetaData metaData) {
        metaData
                .getDatabasesNames()
                .forEach(name -> createLock(name));
    }

    public void createLock(String databaseName) {
        locks.put(databaseName, new ReentrantReadWriteLock());
    }

    public void deleteLock(String databaseName) {
        locks.remove(databaseName);
    }

    public ReadWriteLock getLock(String databaseName) {
        return locks.get(databaseName);
    }

    public boolean containsLock(String databaseName) {
        return locks.containsKey(databaseName);
    }

    public ReadWriteLock getGlobalLock() {
        return globalLock;
    }

    public QueryHandler getHandler() {
        return new LockHandler();
    }
}
