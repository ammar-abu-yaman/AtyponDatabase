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
import com.fasterxml.jackson.databind.JsonNode;

public class DocumentHandler extends QueryHandler {

    SchemaStorage storage = new FileSchemaStorage();
    SchemaValidator validator = new StaticSchemaValidator();
    DatabaseService databaseService = DatabaseManager.getInstance().getDatabaseService();

    @Override
    public void handle(Query query) {
        if(query.getQueryType() == QueryType.AddDocument)
            addDocument(query);
        else if(query.getQueryType() == QueryType.UpdateDocument)
            updateDocument(query);
    }

    private void addDocument(Query query) {
        // no database exist for this query
        if(!databaseService.containsDatabase(query.getDatabaseName())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Database doesn't exist");
            return;
        }

        // a broadcaster node is broadcasting the request
        if(query.getOriginator() != Query.Originator.User) {
            pass(query);
            return;
        }

        // validate document to ensure it conforms to the schema
        JsonNode schema = storage.loadSchema(query.getDatabaseName()).get();
        if(!validator.validateDocument(schema, query.getPayload())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Document added doesn't conform to the database schema");
            return;
        }
        // pass the query to the next handler
        pass(query);
    }

    private void updateDocument(Query query) {
        // no database exist for this query
        if(!databaseService.containsDatabase(query.getDatabaseName())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Database doesn't exist");
            return;
        }

        // a broadcaster or a deferrer node is broadcasting the request
        if(query.getOriginator() != Query.Originator.User) {
            pass(query);
            return;
        }

        // validate schema
        JsonNode schema = storage.loadSchema(query.getDatabaseName()).get();
        if(!validator.validatePartialDocument(schema, query.getPayload())) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("Document updates doesn't confirm to the database schema");
            return;
        }
        pass(query);
    }


}
