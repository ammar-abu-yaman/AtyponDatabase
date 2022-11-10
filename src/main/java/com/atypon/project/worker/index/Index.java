package com.atypon.project.worker.index;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.request.DatabaseRequest;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

public interface Index extends Serializable {
    List<String> search(JsonNode key);
    void add(JsonNode key, String documentIndex);
    void delete(JsonNode key, String documentIndex);
    boolean contains(JsonNode key);
    void clear();
}
