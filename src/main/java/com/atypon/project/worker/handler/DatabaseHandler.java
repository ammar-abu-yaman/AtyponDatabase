package com.atypon.project.worker.handler;

import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.database.DatabaseHandlers;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;

import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DatabaseHandler extends QueryHandler {

    private static final Logger logger = Logger.getLogger(DatabaseHandler.class.getName());

    static {
        try {
            FileHandler fileHandler = new FileHandler("db/database.log", true);
            logger.setLevel(Level.WARNING);
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private DatabaseService databaseService;

    public DatabaseHandler(DatabaseService databaseService) {
        this.databaseService = databaseService;
    }

    @Override
    public void handle(Query query) {
        try {
            if (query.getQueryType() != QueryType.CreateDatabase
                    && !databaseService.containsDatabase(query.getDatabaseName())) {
                query.getRequestOutput().append("No Such Database Exists");
                query.setStatus(Query.Status.Rejected);
                return;
            }

            QueryHandler handler = DatabaseHandlers.getHandler(query, nextHandler);
            handler.handle(query);
        } catch (Exception e) {
            e.printStackTrace();
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append(e.getMessage());
            logger.warning(e.getMessage());
        }
    }

}
