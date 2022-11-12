package com.atypon.project.worker.handler;

import com.atypon.project.worker.lock.LockService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;

import java.util.concurrent.locks.Lock;

public class LockHandler extends QueryHandler {

    LockService lockService;

    public LockHandler(LockService lockService) {
        this.lockService = lockService;
    }

    @Override
    public void handle(Query query) {
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
    }

    private void handleRegister(Query query) {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = lockService.getLock("_Users").readLock();
        try {
            System.out.println("locked for Registering");
            lock.lock();
            globalLock.lock();
            pass(query);
        } finally {
            System.out.println("unlocked for registering");
            globalLock.unlock();
            lock.unlock();
        }
    }

    private void handleLogin(Query request) {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = lockService.getLock("_Users").readLock();
        try {
            globalLock.lock();
            lock.lock();

            System.out.println("locked for login");

            pass(request);

        } finally {
            System.out.println("unlocked login");
            globalLock.unlock();
            lock.unlock();
        }
    }

    private void handleRead(Query request) {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = null;
        globalLock.lock();
        try {
            if (!lockService.containsLock(request.getDatabaseName())) {
                pass(request);
                return;
            }
            System.out.println("lock for reading");
            lock = lockService.getLock(request.getDatabaseName()).readLock();
            lock.lock();
            pass(request);
        } finally {
            System.out.println("unlock for reading");
            globalLock.unlock();
            if (lock != null) {
                lock.unlock();
            }
        }

    }

    private void handleWrite(Query request) {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = null;
        globalLock.lock();
        try {
            System.out.println("lock for writing");
            if (!lockService.containsLock(request.getDatabaseName())) {
                pass(request);
                return;
            }
            lock = lockService.getLock(request.getDatabaseName()).writeLock();
            lock.lock();
            pass(request);
        } finally {
            System.out.println("unlock for writing");
            globalLock.unlock();
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    private void handleSensitive(Query request) {
        Lock globalLock = lockService.getGlobalLock().writeLock();
        globalLock.lock();
        try {
            System.out.println("lock for sensitive");
            pass(request);
            if (request.getStatus() == Query.Status.Rejected)
                return;
            if (request.getQueryType() == QueryType.CreateDatabase) {
                lockService.createLock(request.getDatabaseName());
            }
            if (request.getQueryType() == QueryType.DeleteDatabase)
                lockService.deleteLock(request.getDatabaseName());
        } finally {
            System.out.println("unlock for sensitive");
            globalLock.unlock();
        }
    }

}
