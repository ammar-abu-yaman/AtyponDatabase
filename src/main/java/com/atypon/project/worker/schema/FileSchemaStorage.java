package com.atypon.project.worker.schema;

import com.atypon.project.worker.core.DatabaseManager;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.Optional;

public class FileSchemaStorage implements SchemaStorage {

    File schemasFile;

    public FileSchemaStorage() {
        DatabaseManager manager = DatabaseManager.getInstance();
        schemasFile = Paths.get(manager.getConfiguration().getSavePath()).resolve("schema").toFile();
        if(!schemasFile.exists())
            schemasFile.mkdirs();
    }

    @Override
    public Optional<JsonNode> loadSchema(String databaseName) {
        File saveFile = schemasFile.toPath().resolve(databaseName + ".json").toFile();
        if(!saveFile.exists())
            return Optional.empty();
        JsonNode schema = null;
        try {
            schema = new ObjectMapper().readTree(saveFile);
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
        return Optional.of(schema);
    }

    @Override
    public void saveSchema(JsonNode schema, String databaseName)  {
        File saveFile = schemasFile.toPath().resolve(databaseName + ".json").toFile();
        try {
            if(!saveFile.exists())
                saveFile.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to save schema");
        }
        try (PrintWriter writer = new PrintWriter(saveFile)) {
            writer.write(schema.toString());
        } catch (FileNotFoundException e) {
            /*shouldn't happen*/
            e.printStackTrace();
            return;
        }
    }

    @Override
    public void deleteSchema(String databaseName) {
        File saveFile = schemasFile.toPath().resolve(databaseName + ".json").toFile();
        if(saveFile.exists())
            saveFile.delete();
    }
}
