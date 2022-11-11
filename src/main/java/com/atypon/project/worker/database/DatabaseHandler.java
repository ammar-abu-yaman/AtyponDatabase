package com.atypon.project.worker.database;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.query.QueryHandler;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DatabaseHandler extends QueryHandler {

    private DatabaseManager manager = DatabaseManager.getInstance();
    private DatabaseService databaseService;
    private IdCreator idCreator;

    public DatabaseHandler(DatabaseService databaseService, IdCreator idCreator) {
        this.databaseService = databaseService;
        this.idCreator = idCreator;
    }

    @Override
    public void handle(Query request) {
        if (request.getQueryType() != QueryType.CreateDatabase
                && !databaseService.containsDatabase(request.getDatabaseName())) {
            request.getRequestOutput().append("No Such Database Exists");
            request.setStatus(Query.Status.Rejected);
            return;
        }

        switch (request.getQueryType()) {
            case CreateDatabase:
                createDatabase(request);
                break;
            case DeleteDatabase:
                deleteDatabase(request);
                break;
            case AddDocument:
                addDocument(request);
                break;
            case DeleteDocument:
                deleteDocument(request);
                break;
            case UpdateDocument:
                updateDocument(request);
                break;
            case FindDocument:
                findDocument(request);
                break;
            case FindDocuments:
                findDocuments(request);
                break;
        }
    }

    private void createDatabase(Query request) {
        if (databaseService.containsDatabase(request.getDatabaseName())) {
            request.getRequestOutput().append("Database Already Exists");
            request.setStatus(Query.Status.Rejected);
            return;
        }
        databaseService.createDatabase(request.getDatabaseName());
        pass(request);
    }

    private void deleteDatabase(Query request) {
        Database database = databaseService.getDatabase(request.getDatabaseName());
        List<String> ids = database.getAllDocuments()
                .map(document -> document.get("_affinity").asText())
                .collect(Collectors.toList());
        decrementAffinity(database, ids);
        databaseService.deleteDatabase(request.getDatabaseName());
        pass(request);
    }

    private void addDocument(Query request) {
        JsonNode payload = request.getPayload();
        String documentIndex;
        String affinity;
        // user added a document
        if (request.getOriginator() == Query.Originator.User) {
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> result = mapper.convertValue(payload, new TypeReference<Map<String, Object>>() {
            });
            documentIndex = idCreator.createId(request);
            affinity = calculateAffinity();
            result.put("_id", documentIndex);
            result.put("_affinity", affinity);
            payload = mapper.valueToTree(result);
            request.setPayload(payload);
            // broadcaster is accessing the system
        } else {
            documentIndex = payload.get("_id").asText();
            affinity = payload.get("_affinity").asText();
            databaseService.getDatabase(request.getDatabaseName()).addDocument(documentIndex, payload);

        }
        request.setUsedDocuments(new HashSet<>(Arrays.asList(documentIndex)));
        if (!request.getDatabaseName().equals("_Users"))
            incrementAffinity(affinity);
        databaseService.getDatabase(request.getDatabaseName()).addDocument(documentIndex, payload);
        request.setUsedDocuments(new HashSet<>(Arrays.asList(documentIndex)));
        pass(request);
    }

    private void findDocument(Query request) {
        Database database = databaseService.getDatabase(request.getDatabaseName());
        List<String> documentIndices = indexRequest(request, database)
                .stream()
                .limit(1)
                .collect(Collectors.toList());

        Set<String> usedDocuments = documentIndices.stream().collect(Collectors.toSet());
        request.setUsedDocuments(usedDocuments);
        List<JsonNode> documents = database
                .getDocuments(documentIndices)
                .map(document -> filterFields(document, request.getRequiredProperties()))
                .collect(Collectors.toList());
        if (!documents.isEmpty())
            request.getRequestOutput().append(documents.get(0).toPrettyString());
    }

    private void findDocuments(Query request) {
        Database database = databaseService.getDatabase(request.getDatabaseName());
        Set<String> documentIndices = indexRequest(request, database).stream().collect(Collectors.toSet());
        Stream<JsonNode> documents = database.getDocuments(documentIndices);
        StringBuilder output = request.getRequestOutput();
        request.setUsedDocuments(documentIndices);
        output.append("[\n");
        documents
                .map(document -> filterFields(document, request.getRequiredProperties()))
                .forEach(json -> output.append(json.toPrettyString() + ",\n"));
        if (output.length() > 1)
            output.delete(output.length() - 2, output.length()).append("\n");
        output.append("]");
    }

    private void deleteDocument(Query request) {
        Database database = databaseService.getDatabase(request.getDatabaseName());
        Set<String> usedDocuments = new HashSet<>();

        manager.lockMetaData();
        try {
            MetaData metaData = manager.getConfiguration();
            List<Node> nodes = metaData.getNodes();
            List<Entry<String, String>> info = indexRequest(request, database)
                    .stream()
                    .limit(1)
                    .map(id -> database.getDocument(id))
                    .map(document -> new Entry<>(document.get("_id").asText(), document.get("_affinity").asText()))
                    .collect(Collectors.toList());
            for(Entry<String, String> entry: info) {
                String documentIndex = entry.getKey();
                String affinity = entry.getValue();
                database.deleteDocument(documentIndex);
                usedDocuments.add(documentIndex);
                nodes.stream()
                        .filter(node -> node.getId().equals(affinity))
                        .findFirst().ifPresent(node -> node.decNumDocuments());
            }
        } finally {
            manager.saveMetaData();
            manager.unlockMetaData();
        }

        request.setUsedDocuments(usedDocuments);
        pass(request);
    }

    private void updateDocument(Query request) {
        Database database = databaseService.getDatabase(request.getDatabaseName());

        if(request.getOriginator() == Query.Originator.SelfUpdate) {  // self update case
            JsonNode payload = request.getPayload();
            String documentId = payload.get("_id").asText();
            database.updateDocument(payload, documentId);
            request.setUsedDocuments(Stream.of(documentId).collect(Collectors.toSet()));
            request.setStatus(Query.Status.Accepted);
            return;
        } else if(request.getOriginator() == Query.Originator.User) { // user query case
            Optional<JsonNode> optional = indexRequest(request, database)
                    .stream()
                    .limit(1)
                    .map(documentId -> database.getDocument(documentId))
                    .findFirst();
            // no document to update
            if(!optional.isPresent()) {
                request.setStatus(Query.Status.Rejected);
                return;
            }
            JsonNode oldData = optional.get();
            String documentId = oldData.get("_id").asText();
            String affinity = oldData.get("_affinity").asText();
            String thisNodeId = manager.getConfiguration().getNodeId();
            request.setOldData(oldData);
            request.setUsedDocuments(Stream.of(documentId).collect(Collectors.toSet()));

            if(thisNodeId.equals(affinity)) {
                request.setHasAffinity(true); // declare that affinity is set
                database.updateDocument(request.getPayload(), documentId); // update database
            } else {
                request.setStatus(Query.Status.Rejected); // reject to update current node
            }

            pass(request); // broadcast
        } else if(request.getOriginator() == Query.Originator.Broadcaster) {
            JsonNode broadcasterOldData = request.getPayload().get("old");
            JsonNode payload = request.getPayload().get("payload");
            JsonNode myOldData = database.getDocument(broadcasterOldData.get("_id").asText());
            String documentId = myOldData.get("_id").asText();

            request.setOldData(myOldData); // set old data field
            request.setUsedDocuments(Stream.of(documentId).collect(Collectors.toSet())); // set used documents field
            if(broadcasterOldData.equals(myOldData)) {
                database.updateDocument(payload, documentId);
            } else {
                request.setStatus(Query.Status.Rejected);
            }
        } else if(request.getOriginator() == Query.Originator.Deferrer) {
            JsonNode broadcasterOldData = request.getPayload().get("old");
            JsonNode payload = request.getPayload().get("payload");
            JsonNode myOldData = database.getDocument(broadcasterOldData.get("_id").asText());
            String documentId = myOldData.get("_id").asText();

            request.setHasAffinity(true); // set affinity to true
            request.setOldData(myOldData); // set old data field
            request.setUsedDocuments(Stream.of(documentId).collect(Collectors.toSet())); // set used documents field
            if(broadcasterOldData.equals(myOldData)) {
                database.updateDocument(payload, documentId); // update
                pass(request); // broadcast
            } else {
                request.setStatus(Query.Status.Rejected);
            }
        }
    }

    private List<String> indexRequest(Query request, Database database) {
        // broadcaster provides the ids of the documents to be deleted
        if (request.getOriginator() == Query.Originator.Broadcaster && request.getQueryType() == QueryType.DeleteDocument) {
            List<String> ids = null;
            try {
                ids = new ObjectMapper().treeToValue(request.getPayload().get("_ids"),
                        TypeFactory.defaultInstance().constructCollectionType(List.class, String.class));
            } catch (JsonMappingException e) {
                e.printStackTrace();
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return ids.stream().filter(id -> database.contains(id)).collect(Collectors.toList());
        }

        if (request.getIndex() != null) {
            {
                System.out.println("using index for " + request.getFilterKey());
            }
            return request.getIndex().search(request.getFilterKey().getValue());
        }

        Stream<JsonNode> stream = database.getAllDocuments();
        Entry<String, JsonNode> filterKey = request.getFilterKey();
        if (request.getFilterKey() != null)
            stream = stream
                    .filter(document -> document.has(filterKey.getKey())
                            && document.get(filterKey.getKey()).equals(filterKey.getValue()));
        return stream
                .map(document -> document.get("_id").asText())
                .collect(Collectors.toList());
    }

    private static JsonNode filterFields(JsonNode node, List<String> requiredProperties) {
        if (requiredProperties == null || requiredProperties.isEmpty())
            return node;
        Map<String, Object> properties = new HashMap<>();
        for (String field : requiredProperties) {
            if (!node.has(field))
                continue;
            properties.put(field, node.get(field));
        }

        ObjectMapper mapper = new ObjectMapper();
        return mapper.valueToTree(properties);
    }

    /*
     * a simple affinity calculation scheme
     * that assign the newly created document's affinity to the
     * node with the least number of assigned documents
     */
    private String calculateAffinity() {
        String nodeId = manager.getConfiguration().getNodeId();
        try {
            MetaData metaData = manager.getConfiguration();
            manager.lockMetaData();
            List<Node> nodes = metaData.getNodes();
            int numDocuments = manager.getConfiguration().getNumDocuments();
            for (Node node : nodes) {
                if (node.getNumDocuments() < numDocuments) {
                    nodeId = node.getId();
                    numDocuments = node.getNumDocuments();
                }
            }
        } finally {
            manager.unlockMetaData();
        }
        return nodeId;
    }

    private void incrementAffinity(String nodeId) {
        manager.lockMetaData();
        try {
            MetaData metaData = manager.getConfiguration();
            List<Node> nodes = metaData.getNodes();
            if (nodeId.equals(metaData.getNodeId())) {
                manager.getConfiguration().incNumDocuments();
            } else {
                final String nid = nodeId;
                nodes.stream().filter(node -> node.getId().equals(nid)).forEach(node -> node.incNumDocuments());
            }
        } finally {
            manager.saveMetaData();
            manager.unlockMetaData();
        }

    }

    private void decrementAffinity(Database database, Collection<String> documentIndexes) {
        manager.lockMetaData();
        try {
            MetaData metaData = manager.getConfiguration();
            List<Node> nodes = metaData.getNodes();
            database
                    .getDocuments(documentIndexes)
                    .map(document -> document.get("_affinity").asText())
                    .forEach(affinity -> nodes.stream()
                            .filter(node -> node.getId().equals(affinity))
                            .findFirst().ifPresent(node -> node.decNumDocuments()));
        } finally {
            manager.saveMetaData();
            manager.unlockMetaData();
        }
    }

}
