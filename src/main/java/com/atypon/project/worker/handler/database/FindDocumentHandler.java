package com.atypon.project.worker.handler.database;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FindDocumentHandler extends QueryHandler {

    ObjectMapper mapper = new ObjectMapper();
    DatabaseService databaseService = DatabaseManager.getInstance().getDatabaseService();

    @Override
    public void handle(Query query) {
        if(query.getQueryType() == QueryType.FindDocument)
            findDocument(query);
        else
            findDocuments(query);
    }

    private void findDocument(Query query) {
        Database database = databaseService.getDatabase(query.getDatabaseName());
        List<String> documentIndices = DatabaseUtil.indexRequest(query, database)
                .stream()
                .limit(1)
                .collect(Collectors.toList());

        Set<String> usedDocuments = documentIndices.stream().collect(Collectors.toSet());
        query.setUsedDocuments(usedDocuments);
        List<JsonNode> documents = database
                .getDocuments(documentIndices)
                .map(document -> DatabaseUtil.filterFields(document, query.getRequiredProperties()))
                .collect(Collectors.toList());
        if (!documents.isEmpty())
            query.getRequestOutput().append(documents.get(0).toString());
    }

    private void findDocuments(Query query) {
        Database database = databaseService.getDatabase(query.getDatabaseName());
        Set<String> documentIndices = DatabaseUtil.indexRequest(query, database).stream().collect(Collectors.toSet());
        Stream<JsonNode> documents = database.getDocuments(documentIndices);
        StringBuilder output = query.getRequestOutput();
        query.setUsedDocuments(documentIndices);

        List<JsonNode> list = documents
                .map(document -> DatabaseUtil.filterFields(document, query.getRequiredProperties()))
                .collect(Collectors.toList());
        query.getRequestOutput().append(mapper.valueToTree(list).toString());
    }
}
