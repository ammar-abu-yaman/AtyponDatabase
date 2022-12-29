package com.atypon.project.worker.schema;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.Iterator;
import java.util.Map;

public class StaticSchemaValidator implements SchemaValidator {

    @Override
    public boolean validateSchema(JsonNode schema) {
        if(!schema.isObject())
            return false;

        for (Iterator<Map.Entry<String, JsonNode>> it = schema.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> field = it.next();
            if(!isValidateField(field))
                return false;
        }

        return true;
    }

    public boolean isValidateField(Map.Entry<String, JsonNode> field) {
        //reserved fields
        if(field.getKey().equals("_id") || field.getKey().equals("_affinity"))
            return false;
        // must be one of the allowed types
        switch (field.getValue().asText("").toLowerCase()) {
            case "string":
            case "number":
            case "boolean":
            case "array":
            case "object":
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean validateDocument(JsonNode schema, JsonNode document) {
        if(!document.isObject() || document.size() != schema.size())
            return false;
        return validateFields(schema, document);
    }

    @Override
    public boolean validatePartialDocument(JsonNode schema, JsonNode partialDocument) {
        if(!partialDocument.isObject())
            return false;
        return validateFields(schema, partialDocument);
    }


    private boolean validateFields(JsonNode schema, JsonNode document) {
        for (Iterator<Map.Entry<String, JsonNode>> it = document.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> field = it.next();
            if(!schema.has(field.getKey()))
                return false;
            String type = field.getValue().getNodeType().name();
            String required = schema.get(field.getKey()).asText();

            if(!type.equalsIgnoreCase(required))
                return false;
        }
        return true;
    }
}
