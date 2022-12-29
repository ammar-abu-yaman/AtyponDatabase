package com.atypon.project.worker.schema;

import com.fasterxml.jackson.databind.JsonNode;

public interface SchemaValidator {
    // ensure the schema is valid
    boolean validateSchema(JsonNode schema);
    // ensure the document exactly matches the schema and all the fields
    // in the schema exist in the document no more no less
    boolean validateDocument(JsonNode schema, JsonNode document);

    // validate that the fields in the partial documents matches the schema
    // but doesn't require that all fields in the schema to be present in the document
    boolean validatePartialDocument(JsonNode schema, JsonNode partialDocument);
}
