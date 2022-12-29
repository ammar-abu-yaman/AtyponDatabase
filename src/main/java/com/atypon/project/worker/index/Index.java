package com.atypon.project.worker.index;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.List;

/*
* Interface describing the operations that must be
* implemented in the index
* */
public interface Index extends Serializable {
    // search the index for documents with a specific values for the index's field
    List<String> search(JsonNode key);
    // add a mapping from a value to the document id
    void add(JsonNode key, String documentId);
    // delete a mapping from a value to a specific document id
    void delete(JsonNode key, String documentId);
    // check if the index contain the
    boolean contains(JsonNode key);
    // clear the index of all values
    void clear();
}
