package com.atypon.project.worker.core;

import com.atypon.project.worker.user.User;
import com.atypon.project.worker.cache.CacheService;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.lock.LockService;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.HandlerFactory;
import com.atypon.project.worker.request.RequestHandler;
import com.atypon.project.worker.request.RequestType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.mindrot.jbcrypt.BCrypt;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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

    private DatabaseManager() {
    }

    public static void initialize() {
        INSTANCE = new DatabaseManager();
        INSTANCE.metaDataLock = new ReentrantLock();
        INSTANCE.metaData = MetaData
                .builder()
                .databasesNames(Stream.of("_Users", "Books", "Movies").collect(Collectors.toList()))
                .indexesIdentifiers(Stream.of("_Users:username").collect(Collectors.toList()))
                .nodeId(System.getenv("NODE_ID") != null ? System.getenv("NODE_ID") : "node_1") // assumed to be a
                .savePath("db/")
                .dataDirectory("db/data")
                .indexesDirectory("db/index")
                .build();

        File savePath = Paths.get(INSTANCE.metaData.getSavePath()).toFile();
        if (!savePath.exists()) {
            savePath.mkdirs();
        }

        List<Node> nodes = INSTANCE.getConfiguration().getNodes();

        if (INSTANCE.getConfiguration().getNodeId().equals("node_1")) {
            nodes.add(new Node("node_2", "10.1.4.2", 10));
        } else {
            nodes.add(new Node("node_1", "10.1.4.1", 9));
        }

        INSTANCE.databaseService = new DatabaseService(INSTANCE.metaData);
        INSTANCE.lockService = new LockService(INSTANCE.metaData);
        INSTANCE.indexService = new IndexService(INSTANCE.metaData);
        INSTANCE.cacheService = new CacheService(1024, INSTANCE.metaData);
        INSTANCE.handlersFactory = new HandlerFactory();

        INSTANCE.populateUsers();
        System.out.println(Arrays.asList(savePath.list()));
    }

    private void populateUsers() {
        Stream.of("ammar", "ahmad", "hadeel")
                .forEach(name -> {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode json = new ObjectMapper().valueToTree(
                            new User(
                                    name,
                                    BCrypt.hashpw("12345", BCrypt.gensalt()),
                                    User.Role.Standard,
                                    name.equals("hadeel") ? "node_2" : "node_1"));
                    Map<String, Object> result = mapper.convertValue(json, new TypeReference<Map<String, Object>>() {
                    });
                    result.put("_id", name);
                    result.put("_affinity", name);
                    json = mapper.valueToTree(result);
                    DatabaseRequest request = DatabaseRequest.builder()
                            .originator(DatabaseRequest.Originator.Broadcaster)
                            .databaseName("_Users")
                            .requestType(RequestType.AddDocument)
                            .payload(json)
                            .build();
                    handlersFactory.getHandler(request).handleRequest(request);
                });
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
        try (ObjectOutputStream stream = new ObjectOutputStream(
                new FileOutputStream(Paths.get(metaData.getSavePath()).resolve("config.dat").toFile()))) {
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
        if (INSTANCE == null)
            initialize();
        return INSTANCE;
    }

}
