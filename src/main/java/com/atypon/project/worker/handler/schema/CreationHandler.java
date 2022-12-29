package com.atypon.project.worker.handler.schema;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.database.DatabaseService;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryType;
import com.atypon.project.worker.schema.FileSchemaStorage;
import com.atypon.project.worker.schema.SchemaStorage;
import com.atypon.project.worker.schema.SchemaValidator;
import com.atypon.project.worker.schema.StaticSchemaValidator;

public class CreationHandler extends QueryHandler {

    SchemaStorage storage = new FileSchemaStorage();
    SchemaValidator validator = new StaticSchemaValidator();
    DatabaseService databaseService = DatabaseManager.getInstance().getDatabaseService();

    @Override
    public void handle(Query query) {
        if(query.getQueryType() == QueryType.CreateDatabase)
            createDatabase(query);
        else if(query.getQueryType() == QueryType.DeleteDatabase)
            deleteDatabase(query);
    }

    private void createDatabase(Query query) {
        // check if the system contains the required database
        if(databaseService.containsDatabase(query.getDatabaseName())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Database Already Exists");
            return;
        }

        // validate the schema provided by the user
        if(!validator.validateSchema(query.getPayload())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Database Schema is Invalid");
            return;
        }

        // saving schema
        storage.saveSchema(query.getPayload(), query.getDatabaseName());
        // pass query to next handler
        pass(query);
    }

    private void deleteDatabase(Query query) {
        if(!databaseService.containsDatabase(query.getDatabaseName())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Database doesn't exist");
            return ;
        }
        // delete the schema for the database
        storage.deleteSchema(query.getDatabaseName());
        // pass query to next handler
        pass(query);
    }


}
