package com.atypon.project.worker.handler;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.User;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.index.Index;
import com.atypon.project.worker.index.IndexKey;
import com.atypon.project.worker.query.Query;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.mindrot.jbcrypt.BCrypt;

import java.io.IOException;
import java.util.Optional;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;


public class LoginHandler extends QueryHandler {

    private static final Logger logger = Logger.getLogger(LoginHandler.class.getName());

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
            JsonNode credentials = query.getPayload();
            Optional<User> user = validateUser(credentials);
            if(user.isPresent()) {
                query.setStatus(Query.Status.Accepted);
                query.getRequestOutput().append(new ObjectMapper().valueToTree(user.get()).toString());
            } else {
                query.setStatus(Query.Status.Rejected);
                query.getRequestOutput().append("Incorrect username or password or incorrect node accessed by user");
            }
        } catch (Exception e) {
            e.printStackTrace();
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append(e.getMessage());
            logger.warning(e.getMessage());
        }

    }

    private Optional<User> validateUser(JsonNode credentials)  {
        DatabaseManager manager = DatabaseManager.getInstance();
        Database usersDatabase = manager.getDatabaseService().getDatabase("_Users");
        Index usernameIndex = manager.getIndexService().getIndex(new IndexKey("_Users", "username")).get();

        JsonNode username = credentials.get("username");
        String password = credentials.get("password").asText();

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
