package com.atypon.project.worker.query;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import com.atypon.project.worker.core.Entry;
import com.atypon.project.worker.index.Index;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

@Getter
@Setter
@Builder
public class Query implements Serializable {

    private static final long serialVersionUID = 6L;

    private String databaseName;
    private JsonNode oldData;
    @Builder.Default
    private boolean hasAffinity = false;
    @Builder.Default
    private StringBuilder requestOutput = new StringBuilder();
    @Builder.Default
    private Status status = Status.Accepted;
    private Originator originator;
    private QueryType queryType;
    private Index index;
    private Entry<String, JsonNode> filterKey;
    private JsonNode payload;
    private String indexFieldName;
    private List<String> requiredProperties;
    private Set<String> usedDocuments;

    public enum Originator { User, Broadcaster, Deferrer, SelfUpdate }
    public enum Status { Accepted, Rejected, Deferred }
}
