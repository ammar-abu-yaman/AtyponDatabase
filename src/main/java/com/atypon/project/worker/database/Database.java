package com.atypon.project.worker.database;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.File;
import java.util.Collection;
import java.util.stream.Stream;

public abstract class Database {

    private String name;
    private File dataDirectory;

    public Database(String name, File dataDirectory) {
        this.name = name;
        this.dataDirectory = dataDirectory;
    }

    public String getName() {
        return name;
    }

    public File getDataDirectory() {
        return dataDirectory;
    }

    public abstract void drop();
    public abstract  void addDocument(String docIdx, JsonNode document);
    public abstract void deleteDocument(String docId);
    public abstract void updateDocument(JsonNode fieldsToUpdate, String docId);
    public abstract JsonNode getDocument(String docId);
    public abstract Stream<JsonNode> getDocuments(Collection<String> docIds);
    public abstract Stream<JsonNode> getDocuments(Stream<String> documentIndices);
    public abstract Stream<JsonNode> getAllDocuments();
}
