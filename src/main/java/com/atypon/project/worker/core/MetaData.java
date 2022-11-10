package com.atypon.project.worker.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Builder
@Getter
@Setter
public class MetaData implements Serializable {

    private String nodeId;
    private String address;
    private int numDocuments;
    private String savePath;
    private String dataDirectory;
    private String indexesDirectory;
    private List<String> indexesIdentifiers;
    private List<String> databasesNames;
    @Builder.Default
    private List<Node> nodes = new ArrayList<>();

    public void incNumDocuments() {
        numDocuments++;
    }
    public void decNumDocuments() {
        numDocuments--;
    }
}
