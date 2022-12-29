package com.atypon.project.worker.handler;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.lock.LockService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;

import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LockHandler extends QueryHandler {

    private static final Logger logger = Logger.getLogger(LockHandler.class.getName());

    static {
        try {
            FileHandler fileHandler = new FileHandler("db/database.log", true);
            logger.setLevel(Level.WARNING);
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private LockService lockService = DatabaseManager.getInstance().getLockService();

    @Override
    public void handle(Query query) {
        try {
            switch (query.getQueryType()) {
                case FindDocument:
                case FindDocuments:
                    handleRead(query);
                    return;
                case AddDocument:
                case DeleteDocument:
                case UpdateDocument:
                    handleWrite(query);
                    return;
                case Login:
                    handleLogin(query);
                    return;
                case RegisterUser:
                    handleRegister(query);
                    return;
                default:
                    handleSensitive(query);
                    return;
            }
        } catch (Exception e) {
            e.printStackTrace();
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append(e.getMessage());
            logger.warning(e.getMessage());
        }
    }

    private void handleRegister(Query query)  {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = lockService.getLock("_Users").readLock();
        try {
            lock.lock();
            globalLock.lock();
            pass(query);
        } finally {
            globalLock.unlock();
            lock.unlock();
        }
    }

    private void handleLogin(Query request)  {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = lockService.getLock("_Users").readLock();
        try {
            globalLock.lock();
            lock.lock();

            pass(request);

        } finally {
            globalLock.unlock();
            lock.unlock();
        }
    }

    private void handleRead(Query request)  {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = null;
        globalLock.lock();
        try {
            if (!lockService.containsLock(request.getDatabaseName())) {
                pass(request);
                return;
            }
            lock = lockService.getLock(request.getDatabaseName()).readLock();
            lock.lock();
            pass(request);
        } finally {
            globalLock.unlock();
            if (lock != null) {
                lock.unlock();
            }
        }

    }

    private void handleWrite(Query request)  {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = null;
        globalLock.lock();
        try {
            if (!lockService.containsLock(request.getDatabaseName())) {
                pass(request);
                return;
            }
            lock = lockService.getLock(request.getDatabaseName()).writeLock();
            lock.lock();
            pass(request);
        } finally {
            globalLock.unlock();
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    private void handleSensitive(Query request)  {
        Lock globalLock = lockService.getGlobalLock().writeLock();
        globalLock.lock();
        try {
            pass(request);
            if (request.getStatus() == Query.Status.Rejected)
                return;
            if (request.getQueryType() == QueryType.CreateDatabase) {
                lockService.createLock(request.getDatabaseName());
            }
            if (request.getQueryType() == QueryType.DeleteDatabase)
                lockService.deleteLock(request.getDatabaseName());
        } finally {
            globalLock.unlock();
        }
    }

}
