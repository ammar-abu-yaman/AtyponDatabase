package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.bind.annotation.*;
import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class InternalController {

    DatabaseManager manager = DatabaseManager.getInstance();
    ObjectMapper mapper = new ObjectMapper();

    @PostMapping("/_internal/add_document/{database}")
    public String addDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(QueryType.AddDocument)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    @PostMapping("/_internal/delete_document/{database}")
    public String deleteDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(QueryType.DeleteDocument)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    @PostMapping("/_internal/update_document/{database}")
    public String updateDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .queryType(QueryType.UpdateDocument)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }
    @PostMapping("/_internal/defer_update/{database}")
    public String updateDefer(HttpServletResponse resp, @RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        Query query = Query.builder()
                .originator(Query.Originator.Deferrer)
                .databaseName(databaseName)
                .payload(mapper.valueToTree(requestBody))
                .queryType(QueryType.UpdateDocument)
                .build();
        manager.getHandlersFactory().getHandler(query).handle(query);
        if(query.getStatus() == Query.Status.Accepted) {
            return "";
        } else {
            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            return query.getOldData().toString();
        }
    }


    @PostMapping("/_internal/create_database/{database}")
    public String createDatabase(@PathVariable("database") String databaseName) {
        return databaseHelper(databaseName, QueryType.CreateDatabase);
    }

    @PostMapping("/_internal/delete_database/{database}")
    public String deleteDatabase(@PathVariable("database") String databaseName) {
        return databaseHelper(databaseName, QueryType.DeleteDatabase);
    }

    @PostMapping("/_internal/create_index/{database}/{index}")
    public String createIndex(@PathVariable("database") String database, @PathVariable("index") String index) {
        return indexHelper(database, index, QueryType.CreateIndex);
    }

    @PostMapping("/_internal/delete_index/{database}/{index}")
    public String deleteIndex(@PathVariable("database") String database,
            @PathVariable("index") String index) {
        return indexHelper(database, index, QueryType.DeleteIndex);
    }

    @PostMapping("/_internal/add_user")
    public String addUser(@RequestBody Map<String, Object> requestBody) {
        Query query = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .databaseName("_Users")
                .queryType(QueryType.AddDocument)
                .payload(mapper.valueToTree(requestBody))
                .build();
        manager.getHandlersFactory().getHandler(query).handle(query);
        return query.getStatus().toString();
    }

    @GetMapping("/_internal/get_users")
    public String getUsers() {
        DatabaseService service = manager.getDatabaseService();
        Database usersDatabase = service.getDatabase("_Users");
        List<JsonNode> users = usersDatabase.getAllDocuments().collect(Collectors.toList());
        return mapper.valueToTree(users).toString();
    }

    @GetMapping("/_internal/get_nodes")
    public String getNodes() {
        List<Node> nodes = manager.getConfiguration().getNodes();
        return mapper.valueToTree(nodes).toString();
    }



    private String databaseHelper(String databaseName, QueryType type) {
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(type)
                .databaseName(databaseName)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

    private String indexHelper(String database, String index, QueryType type) {
        Query request = Query.builder()
                .originator(Query.Originator.Broadcaster)
                .queryType(type)
                .databaseName(database)
                .indexFieldName(index)
                .build();
        manager.getHandlersFactory().getHandler(request).handle(request);
        return "";
    }

}
