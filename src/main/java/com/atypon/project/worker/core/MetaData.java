package com.atypon.project.worker.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Builder(access = AccessLevel.PRIVATE)
@Getter
@Setter
public class MetaData implements Serializable {

    private static final long serialVersionUID = 1L;

    private static MetaData INSTANCE;

    private String nodeId;
    private String address;
    @Builder.Default
    private String bootstrapAddress = System.getenv("BOOTSTRAP_ADDRESS") != null
            ? System.getenv("BOOTSTRAP_ADDRESS")
            : "10.1.4.0";
    private int numDocuments;
    private String savePath;
    private String dataDirectory;
    private String indexesDirectory;
    private List<String> indexesIdentifiers;
    private List<String> databasesNames;
    @Builder.Default
    private boolean isBootstrap = false;
    @Builder.Default
    private List<Node> nodes = new ArrayList<>();

    public void incNumDocuments() {
        numDocuments++;
    }
    public void decNumDocuments() {
        numDocuments--;
    }

    public static MetaData getInstance() throws IOException, ClassNotFoundException {
        if(INSTANCE != null)
            return INSTANCE;
        File file = Paths.get("db", "config.dat").toFile();
        if(file.exists()) {
            ObjectInputStream stream = new ObjectInputStream(new FileInputStream(file));
            return INSTANCE = (MetaData) stream.readObject();
        }

        return INSTANCE = MetaData
                .builder()
                .isBootstrap(System.getenv("BOOTSTRAP") != null)
                .bootstrapAddress("10.1.4.0")
                .databasesNames(Stream.of("_Users").collect(Collectors.toList()))
                .indexesIdentifiers(Stream.of("_Users:username").collect(Collectors.toList()))
                .nodeId(System.getenv("NODE_ID") != null ? System.getenv("NODE_ID") : "node_1")
                .savePath("db/")
                .dataDirectory("db/data")
                .indexesDirectory("db/index")
                .build();
    }

}
