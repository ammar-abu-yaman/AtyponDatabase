package com.atypon.project.worker.core;

import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.handler.RegisterHandler;
import com.atypon.project.worker.user.User;
import com.atypon.project.worker.cache.CacheService;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.index.IndexService;
import com.atypon.project.worker.lock.LockService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.handler.HandlerFactory;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.mindrot.jbcrypt.BCrypt;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

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
                .isBootstrap(System.getenv("BOOTSTRAP") != null) // TODO: Change this
                .bootstrapAddress("10.1.4.0")
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

        INSTANCE.databaseService = new DatabaseService(INSTANCE.metaData);
        INSTANCE.lockService = new LockService(INSTANCE.metaData);
        INSTANCE.indexService = new IndexService(INSTANCE.metaData);
        INSTANCE.cacheService = new CacheService(1024, INSTANCE.metaData);
        INSTANCE.handlersFactory = new HandlerFactory();

        INSTANCE.debugInitialize();

//        try {
//            if(INSTANCE.getConfiguration().isBootstrap()) {
//                INSTANCE.initializeAsBootstrap();
//            } else {
//                INSTANCE.initializeAsWorker();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            System.out.println("Something went wrong while initializing node");
//            System.exit(1);
//        }

    }

    private void debugInitialize() {
        List<Node> nodes = INSTANCE.getConfiguration().getNodes();
        if (INSTANCE.getConfiguration().getNodeId().equals("node_1")) {
            nodes.add(new Node("node_2", "10.1.4.2", 0, 0));
        } else {
            nodes.add(new Node("node_1", "10.1.4.1", 0, 0));
        }

        Stream.of("ammar", "ahmad", "hadeel").forEach(name -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = new ObjectMapper().valueToTree(
                    new User(
                            name,
                            BCrypt.hashpw("12345", BCrypt.gensalt()),
                            User.Role.Viewer,
                                    name.equals("hadeel") ? "node_2" : "node_1"));
            Map<String, Object> result = mapper.convertValue(json, new TypeReference<Map<String, Object>>() {
            });
            result.put("_id", name);
            result.put("_affinity", name);
            json = mapper.valueToTree(result);
            Query request = Query.builder()
                    .originator(Query.Originator.Broadcaster)
                    .databaseName("_Users")
                    .queryType(QueryType.AddDocument)
                    .payload(json)
                    .build();
            handlersFactory.getHandler(request).handle(request);
        });
    }

    private void initializeAsWorker() throws JsonProcessingException {
        initializeUsers();
        initializeNodes();
    }


    private void initializeAsBootstrap() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        int numNodes = Integer.parseInt(System.getenv("NUM_NODES") != null ? System.getenv("NUM_NODES") : "2");
        List<Node> nodes = new ArrayList<>();
        for(int i = 1; i <= numNodes; i++)
            nodes.add(new Node("node_" + i, "10.1.4." + i, 0, i == 1 ? 1 : 0));
        metaData.setNodes(nodes);

        String adminUsername = "admin";
        String adminPassword = "admin";

        JsonNode json = mapper.valueToTree(new User(adminUsername, BCrypt.hashpw(adminPassword, BCrypt.gensalt()), User.Role.Admin, "node_1"));

        Map<String, Object> result = mapper.convertValue(json, new TypeReference<Map<String, Object>>() {});
        result.put("_id", "admin");
        result.put("_affinity", "node_1");
        json = mapper.valueToTree(result);
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .databaseName("_Users")
                .queryType(QueryType.AddDocument)
                .payload(json)
                .build();
        handlersFactory.getHandler(request).handle(request);

        List<JsonNode> users = mapper.readValue("[\n" +
                    "    {\"username\": \"ammar\", \"password\": \"12345\", \"role\": \"Standard\"},\n" +
                    "    {\"username\": \"ahmad\", \"password\": \"12345\", \"role\": \"Standard\"},\n" +
                    "    {\"username\": \"jamal\", \"password\": \"12345\", \"role\": \"Standard\"}\n" +
                    "]", TypeFactory.defaultInstance().constructCollectionType(List.class, JsonNode.class));
        System.out.println(users.toString());
        QueryHandler handler = new RegisterHandler();
        handler
                .setNext(getIndexService().getHandler())
                .setNext(getDatabaseService().getHandler());

        for(JsonNode user: users) {
            handler.handle(Query.builder()
                    .databaseName("_Users")
                    .originator(Query.Originator.User)
                    .queryType(QueryType.RegisterUser)
                    .payload(user)
                    .build());
        }

        saveMetaData();
    }

    private void initializeUsers() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String bootstrapAddress = getConfiguration().getBootstrapAddress();
        String url = "http://" + bootstrapAddress + ":8080" + "/_internal/get_users";
        ResponseEntity<String> response = new RestTemplate().getForEntity(url, String.class);

        List<JsonNode> users = mapper.readValue(response.getBody(), TypeFactory.defaultInstance().constructCollectionType(List.class, JsonNode.class));
        for(JsonNode user: users) {
            Query request = Query.builder()
                    .originator(Query.Originator.Broadcaster)
                    .databaseName("_Users")
                    .queryType(QueryType.AddDocument)
                    .payload(user)
                    .build();
            handlersFactory.getHandler(request).handle(request);
        }
    }

    private void initializeNodes() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        String bootstrapAddress = getConfiguration().getBootstrapAddress();
        String url = "http://" + bootstrapAddress + ":8080" + "/_internal/get_nodes";
        ResponseEntity<String> response = new RestTemplate().getForEntity(url, String.class);
        List<Node> nodes = mapper.readValue(response.getBody(), TypeFactory.defaultInstance().constructCollectionType(List.class, Node.class));
        metaData.setNodes(nodes.stream().filter(node -> !node.getId().equals(metaData.getNodeId())).collect(Collectors.toList()));
        saveMetaData();
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
