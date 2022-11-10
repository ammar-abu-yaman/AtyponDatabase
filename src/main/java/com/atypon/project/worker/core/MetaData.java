package com.atypon.project.worker.core;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Builder
@Getter
@Setter
public class MetaData implements Serializable {

    private String nodeId;
    private String address;
    private String savePath;
    private String dataDirectory;
    private String indexesDirectory;
    private List<String> indexesIdentifiers;
    private List<String> databasesNames;
    private List<Node> nodes;

}
