package com.atypon.project.worker.request;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.index.Index;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Builder
public class DatabaseRequest implements Serializable {

    public enum Originator { User, Broadcaster, Deferrer}
    public enum Status { Accepted, Rejected }

    @Builder.Default
    private Status status = Status.Accepted;
    private String databaseName;
    private String requestInput;
    @Builder.Default
    private StringBuilder requestOutput = new StringBuilder();
    private Originator originator;
    private RequestType requestType;
    private Index index;
    private Entry<String, JsonNode> filterKey;
    private JsonNode payload;
    private String indexFieldName;
    private List<String> requiredProperties;
    private Set<String> usedDocuments;

    public String getDatabaseName() {
        return databaseName;
    }

    public String getRequestInput() {
        return requestInput;
    }

    public Originator getOriginator() {
        return originator;
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public StringBuilder getRequestOutput() {
        return requestOutput;
    }

    public Index getIndex() {
        return index;
    }

    public Entry<String, JsonNode> getFilterKey() {
        return filterKey;
    }

    public JsonNode getPayload() {
        return payload;
    }

    public String getIndexFieldName() {
        return indexFieldName;
    }

    public Status getStatus() {
        return status;
    }

    public List<String> getRequiredProperties() {
        return requiredProperties;
    }

    public void setIndex(Index index) {
        this.index = index;
    }

    public Set<String> getUsedDocuments() {
        return usedDocuments;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setUsedDocuments(Set<String> usedDocuments) {
        this.usedDocuments = usedDocuments;
    }
}
