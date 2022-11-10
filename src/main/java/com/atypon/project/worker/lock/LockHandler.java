package com.atypon.project.worker.lock;

import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.atypon.project.worker.request.RequestType;
import java.util.concurrent.locks.Lock;

public class LockHandler extends RequestHandler {

    LockService lockService;

    public LockHandler(LockService lockService) {
        this.lockService = lockService;
    }

    @Override
    public void handleRequest(DatabaseRequest request) {
        switch (request.getRequestType()) {
            case FindDocument:
            case FindDocuments:
                handleRead(request);
                return;
            case AddDocument:
            case DeleteDocument:
            case UpdateDocument:
                handleWrite(request);
                return;
            case Login:
                handleLogin(request);
                return;
            default:
                handleSensitive(request);
                return;
        }
    }

    private void handleLogin(DatabaseRequest request) {
        Lock lock = lockService.getLock("_Users").readLock();
        try {
            System.out.println("locked for login");
            lock.lock();
            passRequest(request);

        } finally {
            System.out.println("unlocked login");
            lock.unlock();
        }
    }

    private void handleRead(DatabaseRequest request) {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = null;
        globalLock.lock();
        try {
            if (!lockService.containsLock(request.getDatabaseName())) {
                passRequest(request);
                return;
            }
            System.out.println("lock for reading");
            lock = lockService.getLock(request.getDatabaseName()).readLock();
            lock.lock();
            passRequest(request);
        } finally {
            System.out.println("unlock for reading");
            globalLock.unlock();
            if (lock != null) {
                lock.unlock();
            }
        }

    }

    private void handleWrite(DatabaseRequest request) {
        Lock globalLock = lockService.getGlobalLock().readLock();
        Lock lock = null;
        globalLock.lock();
        try {
            System.out.println("lock for writing");
            if (!lockService.containsLock(request.getDatabaseName())) {
                passRequest(request);
                return;
            }
            lock = lockService.getLock(request.getDatabaseName()).writeLock();
            lock.lock();
            passRequest(request);
        } finally {
            System.out.println("unlock for writing");
            globalLock.unlock();
            if (lock != null) {
                lock.unlock();
            }
        }
    }

    private void handleSensitive(DatabaseRequest request) {
        Lock globalLock = lockService.getGlobalLock().writeLock();
        globalLock.lock();
        try {
            System.out.println("lock for sensitive");
            passRequest(request);
            if (request.getStatus() == DatabaseRequest.Status.Rejected)
                return;
            if (request.getRequestType() == RequestType.CreateDatabase) {
                lockService.createLock(request.getDatabaseName());
            }
            if (request.getRequestType() == RequestType.DeleteDatabase)
                lockService.deleteLock(request.getDatabaseName());
        } finally {
            System.out.println("unlock for sensitive");
            globalLock.unlock();
        }
    }

}
