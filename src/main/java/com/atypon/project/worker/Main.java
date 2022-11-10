package com.atypon.project.worker;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.atypon.project.worker.cache.SyncLRUCache;
import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.index.BTreeIndex;
import com.atypon.project.worker.index.JsonComparator;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.atypon.project.worker.request.RequestType;

import java.util.List;
import java.util.function.Function;

public class Main {
    public static void main(String[] args) throws Exception {
        DatabaseManager.initialize();
        testCache();
    }


    public static void testCache() throws JsonProcessingException {
        loadData();
        DatabaseManager manager = DatabaseManager.getInstance();
        RequestHandler chain = manager.getHandlersFactory().getHandler(null);

        Function<Integer, DatabaseRequest> view = (i) -> {
            try {
                DatabaseRequest request = DatabaseRequest.builder()
                        .requestType(RequestType.FindDocuments)
                        .databaseName("Books")
                        .filterKey(new Entry<>("price", new ObjectMapper().readTree("13.5")))
                        .build();
                chain.handleRequest(request);
                System.out.println(request.getRequestOutput() + "\n---------");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };

        view.apply(1);
        view.apply(1);

        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.DeleteDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"Jamaica\"")))
                .build());

        view.apply(1);


        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.UpdateDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"C++\"")))
                .payload(new ObjectMapper().readTree("{\"price\": 13.5}"))
                .build());


        view.apply(1);

        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.DeleteDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.CreateDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

    }


    public static void testDatabase() throws JsonProcessingException {
        loadData();
        DatabaseManager manager = DatabaseManager.getInstance();
        RequestHandler chain = manager.getHandlersFactory().getHandler(null);

        Function<Integer, DatabaseRequest> view = (i) -> {
            try {
                DatabaseRequest request = DatabaseRequest.builder()
                        .requestType(RequestType.FindDocuments)
                        .databaseName("Books")
                        .filterKey(new Entry<>("price", new ObjectMapper().readTree("13.5")))
                        .build();
                chain.handleRequest(request);
                System.out.println(request.getRequestOutput() + "\n---------");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };

        view.apply(1);

        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.DeleteDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"Jamaica\"")))
                .build());

        view.apply(1);


        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.UpdateDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"C++\"")))
                .payload(new ObjectMapper().readTree("{\"price\": 13.5}"))
                .build());


        view.apply(1);

        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.DeleteDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

        chain.handleRequest(DatabaseRequest.builder()
                .requestType(RequestType.CreateDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

    }

    static void loadData() throws JsonProcessingException {
        DatabaseManager manager = DatabaseManager.getInstance();

        DatabaseRequest request = DatabaseRequest.builder()
                .requestType(RequestType.CreateIndex)
                .databaseName("Books")
                .indexFieldName("price")
                .build();

        RequestHandler chain = manager.getHandlersFactory().getHandler(request);
        chain.handleRequest(request);

        for(String content: new String[] {
                "{\"title\": \"Harry Potter\", \"price\": 13.5}",
                "{\"title\": \"Jamaica\", \"price\": 13.5}",
                "{\"title\": \"C++\", \"price\": 200}",
                "{\"title\": \"Java\", \"price\": 200}",
        }) {
            chain.handleRequest(DatabaseRequest
                    .builder()
                    .databaseName("Books")
                    .requestType(RequestType.AddDocument)
                    .payload(new ObjectMapper().readTree(content))
                    .build()
            );
        }

    }



    public static void testBTree() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        BTreeIndex index = new BTreeIndex(100, new JsonComparator());
        index.add(mapper.readTree("\"A\""), "1");
        index.add(mapper.readTree("\"A\""), "2");
        index.add(mapper.readTree("\"A\""), "3");
        index.add(mapper.readTree("\"B\""), "dsafsadjfiods");

        index.add(mapper.readTree("null"), "hello");
        index.add(mapper.readTree("50"), "hello");
        index.add(mapper.readTree("5"), "hello");
        index.add(mapper.readTree("false"), "0");
        index.add(mapper.readTree("true"), "1");
        index.delete(mapper.readTree("null"), "hello");
        index.delete(mapper.readTree("\"A\""), "3");
        System.out.println(index.search(mapper.readTree("\"A\"")));
        index.traverse();
    }







}
