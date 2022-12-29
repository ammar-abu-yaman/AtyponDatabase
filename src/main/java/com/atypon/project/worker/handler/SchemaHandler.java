package com.atypon.project.worker.handler;

import com.atypon.project.worker.handler.schema.SchemaHandlers;
import com.atypon.project.worker.query.Query;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SchemaHandler extends QueryHandler {

    private static final Logger logger = Logger.getLogger(SchemaHandler.class.getName());

    static {
        try {
            FileHandler fileHandler = new FileHandler("db/database.log", true);
            logger.setLevel(Level.WARNING);
            logger.addHandler(fileHandler);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void handle(Query query) {
        try {
            Optional<QueryHandler> handler = SchemaHandlers.getHandlers(query, nextHandler);
            if(handler.isPresent())
                handler.get().handle(query);
            else
                pass(query);
        } catch (Exception e) {
            e.printStackTrace();
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append(e.getMessage());
            logger.warning(e.getMessage());
        }
    }

}
