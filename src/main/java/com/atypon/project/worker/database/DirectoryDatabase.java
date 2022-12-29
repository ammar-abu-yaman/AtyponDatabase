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
        if (!databaseDirectory.exists()) { // create a directory if not exists
            databaseDirectory.mkdirs();
        }
    }

    @Override
    public void drop()  {
        try {
            Files.walk(databaseDirectory.toPath())
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        } catch (Exception e) {
            System.out.println("Unable to drop database");
            e.printStackTrace();
            throw new RuntimeException("Unable to drop database");
        }
        databaseDirectory.delete();
    }

    @Override
    public void addDocument(String docIdx, JsonNode document)  {
        Path filePath = databaseDirectory.toPath().resolve(docIdx + ".json");
        File documentFile = filePath.toFile();
        try {
            documentFile.createNewFile();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("Can't create new file to store document");
            throw new RuntimeException("Can't create new file to store document");
        }
        try (PrintWriter writer = new PrintWriter(documentFile);) {
            writer.write(document.toString());
        } catch (FileNotFoundException e) {
            /*Shouldn't happen as the file is created up*/
            e.printStackTrace();
            return;
        }
    }

    @Override
    public void deleteDocument(String docId) {
        Path filePath = databaseDirectory.toPath().resolve(docId + ".json");
        File documentFile = filePath.toFile();
        if (documentFile.exists() && documentFile.isFile())
            documentFile.delete();
    }

    @Override
    public void updateDocument(JsonNode fieldsToUpdate, String docId)  {
        Path filePath = databaseDirectory.toPath().resolve(docId + ".json");
        File documentFile = filePath.toFile();
        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> document = mapper.convertValue(
                    mapper.readTree(documentFile),
                    new TypeReference<Map<String, Object>>() {
                    });

            for (Iterator<Map.Entry<String, JsonNode>> it = fieldsToUpdate.fields(); it.hasNext();) {
                Map.Entry<String, JsonNode> entry = it.next();
                document.put(entry.getKey(), entry.getValue());
            }

            JsonNode updatedDocument = mapper.valueToTree(document);
            try (PrintWriter writer = new PrintWriter(new FileOutputStream(documentFile, false))) {
                writer.write(updatedDocument.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to update document");
        }
    }

    @Override
    public JsonNode getDocument(String docId) {
        File documentFile = databaseDirectory.toPath().resolve(docId + ".json").toFile();
        try {
            return new ObjectMapper().readTree(documentFile);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to read document");
        }
    }

    @Override
    public Stream<JsonNode> getDocuments(Collection<String> documentIndices) {
        return documentIndices
                .stream()
                .map(docId -> getDocument(docId));
    }


    @Override
    public Stream<JsonNode> getAllDocuments()  {
        try {
            return Files.walk(databaseDirectory.toPath())
                    .skip(1)
                    .map(path -> path.getFileName().toString().split("\\.")[0])
                    .map(documentIndex -> getDocument(documentIndex));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to read documents");
        }
    }

    @Override
    public boolean contains(String documentIndex) {
        File documentFile = databaseDirectory.toPath().resolve(documentIndex + ".json").toFile();
        return documentFile.exists();
    }

}
