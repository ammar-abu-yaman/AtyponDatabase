package com.atypon.project.worker.user;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.index.BTreeIndex;
import com.atypon.project.worker.index.Index;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.query.QueryHandler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.mindrot.jbcrypt.BCrypt;

import java.util.Optional;


public class LoginHandler extends QueryHandler {

    @Override
    public void handle(Query request) {
        JsonNode credentials = request.getPayload();
        Optional<User> user = validateUser(credentials);
        if(user.isPresent()) {
            request.setStatus(Query.Status.Accepted);
            request.getRequestOutput().append(new ObjectMapper().valueToTree(user.get()).toString());
        } else {
            request.setStatus(Query.Status.Rejected);
            request.getRequestOutput().append("Incorrect username or password or incorrect node accessed by user");
        }
    }

    private Optional<User> validateUser(JsonNode credentials) {
        DatabaseManager manager = DatabaseManager.getInstance();
        Database usersDatabase = manager.getDatabaseService().getDatabase("_Users");
        Index usernameIndex = manager.getIndexService().getIndex(new IndexKey("_Users", "username")).get();

        JsonNode username = credentials.get("username");
        String password = credentials.get("password").asText();
        System.out.println(username + " " + password);
        ((BTreeIndex)usernameIndex).traverse();
        // no user in the database
        if(!usernameIndex.contains(username))
            return Optional.empty();

        String userDocumentId = usernameIndex.search(username).get(0);
        JsonNode json = usersDatabase.getDocument(userDocumentId);
        User user = null;

        user = new User(
                json.get("username").asText(),
                json.get("passwordHash").asText(),
                User.getRole(json.get("role").asText()),
                json.get("nodeId").asText());

        // user is assigned to this node
        if(user.getNodeId().equals(manager.getConfiguration().getNodeId())
                // password matches
                && BCrypt.checkpw(password, user.getPasswordHash()))
            return Optional.of(user);

        return Optional.empty();
    }



}
