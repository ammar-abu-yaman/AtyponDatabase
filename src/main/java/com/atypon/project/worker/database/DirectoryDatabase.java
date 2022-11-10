package com.atypon.project.worker.database;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

public class DirectoryDatabase extends Database {

    File databaseDirectory;

    public DirectoryDatabase(String name, File dataDirectory) {
        super(name, dataDirectory);
        this.databaseDirectory = dataDirectory.toPath().resolve(name).toFile();
        if(!databaseDirectory.exists()) { // create a directory if not exists
            databaseDirectory.mkdirs();
        }
    }

    @Override
    public void drop() {
        try {
            Files.walk(databaseDirectory.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
        databaseDirectory.delete();
    }

    @Override
    public void addDocument(String docIdx, JsonNode document) {
        Path filePath = databaseDirectory.toPath().resolve(docIdx + ".json");
        File documentFile = filePath.toFile();
        try {
            documentFile.createNewFile();
        } catch (IOException e) {
            /*this should never happen*/
        }
        try(PrintWriter writer = new PrintWriter(documentFile);) {
            writer.write(document.toString());
        } catch (FileNotFoundException e) { /*this error should never happen*/ }
    }

    @Override
    public void deleteDocument(String docId) {
        Path filePath = databaseDirectory.toPath().resolve(docId + ".json");
        File documentFile = filePath.toFile();
        if(documentFile.exists() && documentFile.isFile())
            documentFile.delete();
    }

    @Override
    public void updateDocument(JsonNode fieldsToUpdate, String docId) {
        Path filePath = databaseDirectory.toPath().resolve(docId + ".json");
        File documentFile = filePath.toFile();
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> document = mapper.convertValue(
                    mapper.readTree(documentFile),
                    new TypeReference<Map<String, Object>>(){});

            for (Iterator<Map.Entry<String, JsonNode>> it = fieldsToUpdate.fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> entry = it.next();
                document.put(entry.getKey(), entry.getValue());
            }

            JsonNode updatedDocument = mapper.valueToTree(document);
           try(PrintWriter writer = new PrintWriter(new FileOutputStream(documentFile, false))) {
               writer.write(updatedDocument.toString());
           }
        } catch (IOException e) { }
    }

    @Override
    public JsonNode getDocument(String docId) {
        File documentFile = databaseDirectory.toPath().resolve(docId + ".json").toFile();
        try {
            return new ObjectMapper().readTree(documentFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Stream<JsonNode> getDocuments(Collection<String> documentIndices) {
        return documentIndices
                .stream()
                .map(docId -> getDocument(docId));
    }

    @Override
    public Stream<JsonNode> getDocuments(Stream<String> documentIndices) {
        return documentIndices
                .map(documentIndex -> getDocument(documentIndex));
    }

    @Override
    public Stream<JsonNode> getAllDocuments() {
        try {
            return Files.walk(databaseDirectory.toPath())
                    .skip(1)
                    .map(path -> path.getFileName().toString().split("\\.")[0])
                    .map(documentIndex -> getDocument(documentIndex));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
