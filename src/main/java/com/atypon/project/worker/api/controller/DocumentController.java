package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.atypon.project.worker.handler.QueryHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;

@RestController
public class DocumentController {

    ObjectMapper mapper = new ObjectMapper();
    DatabaseManager manager = DatabaseManager.getInstance();

    @GetMapping(value="/document/find/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public ResponseEntity<String> findDocument(@RequestBody(required = false) Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        return findDocumentHelper(requestBody, databaseName, QueryType.FindDocument);
    }

    @GetMapping(value="/document/finds/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public ResponseEntity<String> findDocuments(@RequestBody(required = false) Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        return findDocumentHelper(requestBody, databaseName, QueryType.FindDocuments);
    }


    private ResponseEntity<String> findDocumentHelper(Map<String, Object> requestBody, String databaseName, QueryType type) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        Query.QueryBuilder builder = Query
                .builder()
                .originator(Query.Originator.User)
                .databaseName(databaseName)
                .queryType(type);

        // redirect this request to another node in the system
        if(manager.getNumRequests() >= manager.getCongestionThreshold()) {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> entity = new HttpEntity<>(mapper.valueToTree(requestBody).toString(), headers);

            // send request to other node
            ResponseEntity<String> response =  new RestTemplate().postForEntity(URI.create(String.format("http://%s:8080/document/%s/%s",
                    getNextNodeAddress(),
                    type == QueryType.FindDocument ? "find" : "finds",
                    databaseName
            )), entity, String.class);
            // return node's request
            return response;
        }

        // check filter for errors
        if(json.has("filter")) {
            Optional<String> err = getFilterErrorMessage(json);
            if(err.isPresent())
                return ResponseEntity
                        .status(HttpStatus.BAD_REQUEST)
                        .body(err.get());
            builder.filterKey(getFilter(json));
        }

        // check required properties for errors
        if(json.has("requiredProperties")) {
            Optional<String> err = getRequiredPropertiesError(json);
            if(err.isPresent())
                return ResponseEntity
                        .status(HttpStatus.BAD_REQUEST)
                        .body(err.get());
            builder.requiredProperties(getRequiredProperties(json));
        }

        Query request = builder.build();
        QueryHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handle(request);
        return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .body(request.getRequestOutput().toString());
    }

    @PostMapping(value = "/document/add/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String addDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        Query.QueryBuilder builder = Query
                .builder()
                .originator(Query.Originator.User)
                .databaseName(databaseName)
                .queryType(QueryType.AddDocument);

        if(!json.has("payload") || !json.get("payload").isObject()) {
            return "Payload field is invalid";
        }

        Query request = builder.payload(json.get("payload")).build();
        QueryHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handle(request);
        return request.getRequestOutput().toString();
    }

    @PostMapping(value = "/document/update/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String updateDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        Query.QueryBuilder builder = Query
                .builder()
                .originator(Query.Originator.User)
                .databaseName(databaseName)
                .queryType(QueryType.UpdateDocument);

        if(!json.has("payload") || !json.get("payload").isObject()) {
            return "\"Payload field is invalid\"";
        }

        Optional<String> filterError = getFilterErrorMessage(json);
        if(filterError.isPresent()) {
            return filterError.get();
        }
        Query request = builder
                .payload(json.get("payload"))
                .filterKey(getFilter(json))
                .build();

        QueryHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handle(request);
        return request.getRequestOutput().toString();
    }

    @PostMapping(value = "/document/delete/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String deleteDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        Query.QueryBuilder builder = Query
                .builder()
                .originator(Query.Originator.User)
                .databaseName(databaseName)
                .queryType(QueryType.DeleteDocument);

        Optional<String> filterError = getFilterErrorMessage(json);
        if(filterError.isPresent()) {
            return filterError.get();
        }
        Query request = builder
                .filterKey(getFilter(json))
                .build();

        QueryHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handle(request);
        return request.getRequestOutput().toString();
    }

    private String getNextNodeAddress() {
        List<Node> nodes = manager.getConfiguration().getNodes();
        int id = manager.getRedirectNodeIndexAndIncrement();
        return nodes.get(id).getAddress();
    }

    private Entry<String, JsonNode> getFilter(JsonNode json) {
        Map.Entry<String, JsonNode> filter = json.get("filter").fields().next();
        return new Entry<String, JsonNode>(filter.getKey(), filter.getValue());
    }

    private Optional<String> getFilterErrorMessage(JsonNode json) {
        if(!json.has("filter"))
            return Optional.of("No filter is Provided");
        JsonNode filter = json.get("filter");
        if(!filter.isObject())
            return Optional.of("Filter Doesn't Have Key");
        if(filter.size() != 1)
            return Optional.of("Filer Must Have One Filter Field");
        return Optional.empty();
    }

    private Optional<String> getRequiredPropertiesError(JsonNode json) {
        if(!json.has("requiredProperties"))
            return Optional.of("No Required Properties Provided");
        JsonNode required = json.get("requiredProperties");
        ObjectMapper mapper = new ObjectMapper();
        try {
            List<String> list = mapper.readValue(required.toString(), TypeFactory.defaultInstance().constructCollectionType(List.class, String.class));
        } catch (JsonProcessingException e) {
            return Optional.of("Required Properties should be a list of strings");
        }
        return Optional.empty();
    }

    private List<String> getRequiredProperties(JsonNode json) {
        ObjectMapper mapper = new ObjectMapper();
        List<String> list = new ArrayList<>();
        try {
             list = mapper.readValue(json.get("requiredProperties").toString(), TypeFactory.defaultInstance().constructCollectionType(List.class, String.class));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return list;
    }

}
