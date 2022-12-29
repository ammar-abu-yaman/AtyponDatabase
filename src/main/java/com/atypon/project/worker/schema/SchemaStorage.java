package com.atypon.project.worker.schema;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Optional;

public interface SchemaStorage {
    // load schema from disk
    Optional<JsonNode> loadSchema(String databaseName);
    // save schema to disk
    void saveSchema(JsonNode schema, String databaseName) ;
    // delete schema from disk
    void deleteSchema(String databaseName);
}
