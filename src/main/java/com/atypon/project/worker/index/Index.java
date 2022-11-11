package com.atypon.project.worker.index;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.List;

public interface Index extends Serializable {
    List<String> search(JsonNode key);
    void add(JsonNode key, String documentIndex);
    void delete(JsonNode key, String documentIndex);
    boolean contains(JsonNode key);
    void clear();
}
