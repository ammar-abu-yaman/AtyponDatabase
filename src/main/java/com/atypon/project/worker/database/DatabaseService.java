package com.atypon.project.worker.database;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.request.RequestHandler;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DatabaseService {

    private File dataDirectory;
    private Map<String, Database> databases;

    public DatabaseService(MetaData metaData) {
        this.dataDirectory = Paths.get(metaData.getDataDirectory()).toFile();
        if(!dataDirectory.exists())
            dataDirectory.mkdirs();
        databases = new HashMap<>();
        createDatabaseDirs(metaData);
    }


    public void createDatabase(String databaseName) {
        Database database = new DirectoryDatabase(databaseName, dataDirectory);
        databases.put(databaseName, database);

        DatabaseManager manager = DatabaseManager.getInstance();

        manager.lockMetaData();
        try {
            manager.getConfiguration().getDatabasesNames().add(databaseName);
            manager.saveMetaData();
        } finally {
            manager.unlockMetaData();
        }
    }

    public void deleteDatabase(String databaseName) {
        databases.get(databaseName).drop();
        databases.remove(databaseName);

        DatabaseManager manager = DatabaseManager.getInstance();

        manager.lockMetaData();
        try {
            manager.getConfiguration().getDatabasesNames().remove(databaseName);
            manager.saveMetaData();
        } finally {
            manager.unlockMetaData();
        }
    }


    public Database getDatabase(String databaseName) {
        return databases.get(databaseName);
    }

    public boolean containsDatabase(String databaseName) {
        return databases.containsKey(databaseName);
    }

    private void createDatabaseDirs(MetaData metaData) {
        metaData
                .getDatabasesNames()
                .stream()
                .forEach(name -> databases.put(name, new DirectoryDatabase(name, dataDirectory)));
    }


    public File getDataDirectory() {
        return dataDirectory;
    }

    public RequestHandler getHandler() {
        return new DatabaseHandler(this, new UUIDIdCreator());
    }

}
