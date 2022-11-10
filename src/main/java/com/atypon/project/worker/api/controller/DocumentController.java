package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.request.DatabaseRequest;
import com.atypon.project.worker.request.RequestHandler;
import com.atypon.project.worker.request.RequestType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@RestController
public class DocumentController {

    DatabaseManager manager = DatabaseManager.getInstance();

    @GetMapping(value="/document/find/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String findDocument(@RequestBody(required = false) Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        return findDocumentHelper(requestBody, databaseName, RequestType.FindDocument);
    }

    @GetMapping(value="/document/finds/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String findDocuments(@RequestBody(required = false) Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        return findDocumentHelper(requestBody, databaseName, RequestType.FindDocuments);
    }


    private String findDocumentHelper( Map<String, Object> requestBody, String databaseName, RequestType type) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        DatabaseRequest.DatabaseRequestBuilder builder = DatabaseRequest
                .builder()
                .originator(DatabaseRequest.Originator.User)
                .databaseName(databaseName)
                .requestType(type);

        if(json.has("filter")) {
            Optional<String> err = getFilterErrorMessage(json);
            if(err.isPresent())
                return err.get();
            builder.filterKey(getFilter(json));
        }

        if(json.has("requiredProperties")) {
            Optional<String> err = getRequiredPropertiesError(json);
            if(err.isPresent())
                return err.get();
            builder.requiredProperties(getRequiredProperties(json));
        }

        DatabaseRequest request = builder.build();
        RequestHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

    @PostMapping(value = "/document/add/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String addDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        DatabaseRequest.DatabaseRequestBuilder builder = DatabaseRequest
                .builder()
                .originator(DatabaseRequest.Originator.User)
                .databaseName(databaseName)
                .requestType(RequestType.AddDocument);

        if(!json.has("payload") || !json.get("payload").isObject()) {
            return "Payload field is invalid";
        }

        DatabaseRequest request = builder.payload(json.get("payload")).build();
        RequestHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

    @PostMapping(value = "/document/update/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String updateDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        DatabaseRequest.DatabaseRequestBuilder builder = DatabaseRequest
                .builder()
                .originator(DatabaseRequest.Originator.User)
                .databaseName(databaseName)
                .requestType(RequestType.UpdateDocument);

        if(!json.has("payload") || !json.get("payload").isObject() || json.get("payload").size() != 1) {
            return "Payload field is invalid";
        }

        Optional<String> filterError = getFilterErrorMessage(json);
        if(filterError.isPresent()) {
            return filterError.get();
        }
        DatabaseRequest request = builder
                .payload(json.get("payload"))
                .filterKey(getFilter(json))
                .build();

        RequestHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
    }

    @PostMapping(value = "/document/delete/{database}", consumes = { MediaType.APPLICATION_JSON_VALUE })
    public String deleteDocument(@RequestBody Map<String, Object> requestBody, @PathVariable("database") String databaseName) {
        JsonNode json = new ObjectMapper().valueToTree(requestBody);
        DatabaseRequest.DatabaseRequestBuilder builder = DatabaseRequest
                .builder()
                .originator(DatabaseRequest.Originator.User)
                .databaseName(databaseName)
                .requestType(RequestType.DeleteDocument);

        Optional<String> filterError = getFilterErrorMessage(json);
        if(filterError.isPresent()) {
            return filterError.get();
        }
        DatabaseRequest request = builder
                .filterKey(getFilter(json))
                .build();

        RequestHandler handler = manager.getHandlersFactory().getHandler(request);
        handler.handleRequest(request);
        return request.getStatus() + " => " + request.getRequestOutput();
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
            /* should never happen */
        }
        return list;
    }

}
