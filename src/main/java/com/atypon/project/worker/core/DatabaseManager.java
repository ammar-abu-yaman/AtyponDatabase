package com.atypon.project.worker.core;

import com.atypon.project.worker.auth.User;
import com.atypon.project.worker.cache.CacheService;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.lock.LockService;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.HandlerFactory;
import com.atypon.project.worker.request.RequestType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mindrot.jbcrypt.BCrypt;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Paths;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabaseManager {

    private static DatabaseManager INSTANCE;

    private MetaData metaData;
    private Lock metaDataLock;
    private DatabaseService databaseService;
    private IndexService indexService;
    private CacheService cacheService;
    private LockService lockService;

    private HandlerFactory handlersFactory;

    private DatabaseManager() {}

    public static void initialize() {
        INSTANCE = new DatabaseManager();
        INSTANCE.metaDataLock = new ReentrantLock();
        INSTANCE.metaData = MetaData
                .builder()
                .databasesNames(Stream.of("_Users", "Books", "Movies").collect(Collectors.toList()))
                .indexesIdentifiers(Stream.of("_Users:username").collect(Collectors.toList()))
                .nodeId("1")
                .savePath("C:\\Users\\ammar\\Desktop\\Data\\config.dat")
                .dataDirectory("C:\\Users\\ammar\\Desktop\\Data\\data")
                .indexesDirectory("C:\\Users\\ammar\\Desktop\\Data\\indexes")
                .build();
        INSTANCE.databaseService = new DatabaseService(INSTANCE.metaData);
        INSTANCE.lockService = new LockService(INSTANCE.metaData);
        INSTANCE.indexService = new IndexService(INSTANCE.metaData);
        INSTANCE.cacheService = new CacheService(1024, INSTANCE.metaData);
        INSTANCE.handlersFactory = new HandlerFactory();

        INSTANCE.populateUsers();

    }

    private void populateUsers() {
        Stream.of("ammar", "ahmad", "hadeel").
                map(name -> DatabaseRequest
                        .builder()
                        .originator(DatabaseRequest.Originator.User)
                        .databaseName("_Users")
                        .requestType(RequestType.AddDocument)
                        .payload(new ObjectMapper().valueToTree(
                                new User(
                                        name,
                                        BCrypt.hashpw("12345", BCrypt.gensalt()),
                                        User.Role.Standard,
                                        name.equals("hadeel") ? "2" : "1")))
                        .build()
                ).forEach(request -> handlersFactory.getHandler(request).handleRequest(request));

    }

    public MetaData getConfiguration() {
        return metaData;
    }

    public void lockMetaData() {
        metaDataLock.lock();
    }

    public void unlockMetaData() {
        metaDataLock.unlock();
    }

    public void saveMetaData() {
        lockMetaData();
        try(ObjectOutputStream stream = new ObjectOutputStream(new FileOutputStream(Paths.get(metaData.getSavePath()).toFile()))) {
            stream.writeObject(metaData);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            unlockMetaData();
        }
    }

    public DatabaseService getDatabaseService() {
        return databaseService;
    }

    public IndexService getIndexService() {
        return indexService;
    }

    public CacheService getCacheService() {
        return cacheService;
    }

    public LockService getLockService() {
        return lockService;
    }

    public HandlerFactory getHandlersFactory() {
        return handlersFactory;
    }

    public static DatabaseManager getInstance() {
        if(INSTANCE == null)
            initialize();
        return INSTANCE;
    }


}
