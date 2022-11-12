package com.atypon.project.worker.handler;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.database.Database;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.mindrot.jbcrypt.BCrypt;

import java.util.List;

public class RegisterHandler extends QueryHandler {

    DatabaseManager manager = DatabaseManager.getInstance();
    List<Node> nodes = manager.getConfiguration().getNodes();
    ObjectMapper mapper = new ObjectMapper();

    @Override
    public void handle(Query query) {
        Database users = manager.getDatabaseService().getDatabase("_Users");
        JsonNode payload = query.getPayload();
        if(doesUserExist(users, payload)) {
            query.setStatus(Query.Status.Rejected);
            query.getRequestOutput().append("User with this name already exists");
            return;
        }

        ObjectNode user = mapper.createObjectNode();
        user.put("username", payload.get("username").asText());
        user.put("passwordHash", BCrypt.hashpw(payload.get("password").asText(), BCrypt.gensalt()));
        user.put("role", payload.get("role").asText());
        user.put("_id", payload.get("username").asText());

        Node assignedNode = assignAffinity();
        user.put("nodeId", assignedNode.getId());
        user.put("_affinity", assignedNode.getId());

        query.setPayload(user);
        pass(query);

        query.getRequestOutput().append("You have been assigned to node " + assignedNode.getId());
    }

    private Node assignAffinity() {
        manager.lockMetaData();
        try {
            Node affinityNode = nodes.get(0);
            for (Node node : nodes) {
                if (node.getNumUsers() < affinityNode.getNumUsers())
                    affinityNode = node;
            }
            affinityNode.incNumUsers();
            return affinityNode;
        } finally {
            manager.saveMetaData();
            manager.unlockMetaData();
        }
    }

    private boolean doesUserExist(Database users, JsonNode payload) {
        return users.contains(payload.get("username").asText());
    }

}
