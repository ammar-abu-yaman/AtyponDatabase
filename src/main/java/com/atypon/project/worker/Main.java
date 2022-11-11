package com.atypon.project.worker;

import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.index.BTreeIndex;
import com.atypon.project.worker.index.JsonComparator;
import com.atypon.project.worker.query.QueryHandler;

import java.util.function.Function;

public class Main {
    public static void main(String[] args) throws Exception {
        DatabaseManager.initialize();
        testCache();
    }


    public static void testCache() throws JsonProcessingException {
        loadData();
        DatabaseManager manager = DatabaseManager.getInstance();
        QueryHandler chain = manager.getHandlersFactory().getHandler(null);

        Function<Integer, Query> view = (i) -> {
            try {
                Query request = Query.builder()
                        .queryType(QueryType.FindDocuments)
                        .databaseName("Books")
                        .filterKey(new Entry<>("price", new ObjectMapper().readTree("13.5")))
                        .build();
                chain.handle(request);
                System.out.println(request.getRequestOutput() + "\n---------");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };

        view.apply(1);
        view.apply(1);

        chain.handle(Query.builder()
                .queryType(QueryType.DeleteDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"Jamaica\"")))
                .build());

        view.apply(1);


        chain.handle(Query.builder()
                .queryType(QueryType.UpdateDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"C++\"")))
                .payload(new ObjectMapper().readTree("{\"price\": 13.5}"))
                .build());


        view.apply(1);

        chain.handle(Query.builder()
                .queryType(QueryType.DeleteDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

        chain.handle(Query.builder()
                .queryType(QueryType.CreateDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

    }


    public static void testDatabase() throws JsonProcessingException {
        loadData();
        DatabaseManager manager = DatabaseManager.getInstance();
        QueryHandler chain = manager.getHandlersFactory().getHandler(null);

        Function<Integer, Query> view = (i) -> {
            try {
                Query request = Query.builder()
                        .queryType(QueryType.FindDocuments)
                        .databaseName("Books")
                        .filterKey(new Entry<>("price", new ObjectMapper().readTree("13.5")))
                        .build();
                chain.handle(request);
                System.out.println(request.getRequestOutput() + "\n---------");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };

        view.apply(1);

        chain.handle(Query.builder()
                .queryType(QueryType.DeleteDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"Jamaica\"")))
                .build());

        view.apply(1);


        chain.handle(Query.builder()
                .queryType(QueryType.UpdateDocument)
                .databaseName("Books")
                .filterKey(new Entry<>("title", new ObjectMapper().readTree("\"C++\"")))
                .payload(new ObjectMapper().readTree("{\"price\": 13.5}"))
                .build());


        view.apply(1);

        chain.handle(Query.builder()
                .queryType(QueryType.DeleteDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

        chain.handle(Query.builder()
                .queryType(QueryType.CreateDatabase)
                .databaseName("Books")
                .build()
        );

        view.apply(1);

    }

    static void loadData() throws JsonProcessingException {
        DatabaseManager manager = DatabaseManager.getInstance();

        Query request = Query.builder()
                .queryType(QueryType.CreateIndex)
                .databaseName("Books")
                .indexFieldName("price")
                .build();

        QueryHandler chain = manager.getHandlersFactory().getHandler(request);
        chain.handle(request);

        for(String content: new String[] {
                "{\"title\": \"Harry Potter\", \"price\": 13.5}",
                "{\"title\": \"Jamaica\", \"price\": 13.5}",
                "{\"title\": \"C++\", \"price\": 200}",
                "{\"title\": \"Java\", \"price\": 200}",
        }) {
            chain.handle(Query
                    .builder()
                    .databaseName("Books")
                    .queryType(QueryType.AddDocument)
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
