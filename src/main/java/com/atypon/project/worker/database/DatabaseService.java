package com.atypon.project.worker.database;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.MetaData;
import com.atypon.project.worker.handler.DatabaseHandler;
import com.atypon.project.worker.handler.QueryHandler;

import java.io.*;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class DatabaseService {

    private static DatabaseService INSTANCE;

    public static DatabaseService getInstance() throws IOException, ClassNotFoundException {
        if(INSTANCE != null)
            return INSTANCE;
        return INSTANCE = new DatabaseService(MetaData.getInstance());
    }

    private File dataDirectory;
    private Map<String, Database> databases;

    private DatabaseService(MetaData metaData) {
        this.dataDirectory = Paths.get(metaData.getDataDirectory()).toFile();
        if(!dataDirectory.exists())
            dataDirectory.mkdirs();
        databases = new HashMap<>();
        createDatabaseDirs(metaData);
    }




    public Database getDatabase(String databaseName) {
        return databases.get(databaseName);
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

    public QueryHandler getHandler() {
        return new DatabaseHandler(this);
    }

}
