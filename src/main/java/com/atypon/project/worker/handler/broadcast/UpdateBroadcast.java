package com.atypon.project.worker.handler.broadcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.atypon.project.worker.handler.QueryHandler;
import com.atypon.project.worker.query.Query;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.*;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.util.List;

import static java.lang.String.format;

public class UpdateBroadcast extends QueryHandler {

    ObjectMapper mapper = new ObjectMapper();
    DatabaseManager manager = DatabaseManager.getInstance();
    List<Node> nodes = manager.getConfiguration().getNodes();

    @Override
    public void handle(Query query)  {
        JsonNode oldData = query.getOldData();
        JsonNode payload = query.getOriginator() == Query.Originator.User ? query.getPayload() : query.getPayload().get("payload");
        ObjectNode body = mapper.createObjectNode();
        body.set("old", oldData);
        body.set("payload", payload);

        // node have affinity then do a regular broadcast to all other nodes
        if(query.isHasAffinity()) {
            BroadcastUtils.broadcast("update_document", query.getDatabaseName(), body.toString());
            query.setStatus(Query.Status.Accepted);
            return;
        }
        // node doesn't have affinity, defer the write to the node with the affinity for the document
        String affinity = oldData.get("_affinity").asText();
        Node node = nodes.stream().filter(n -> n.getId().equals(affinity)).findFirst().get();
        try {
            ResponseEntity<String> response = defer(query.getDatabaseName(), body.toString(), node);
            while(response.getStatusCode() != HttpStatus.OK) {
                JsonNode newData = mapper.readTree(response.getBody());
                Query rewrite = Query.builder()
                        .originator(Query.Originator.SelfUpdate)
                        .databaseName(query.getDatabaseName())
                        .payload(newData)
                        .build();
                // update self with new value
                manager.getHandlersFactory().getHandler(rewrite).handle(rewrite);
                // replace old data with the rewritten data
                body.replace("old", newData);
                // try to defer again
                response = defer(query.getDatabaseName(), body.toString(), node);
            }

            query.setStatus(Query.Status.Deferred);
            return;
        }
        catch (Exception e) { // network error or json parse error
            query.setStatus(Query.Status.Rejected);
            e.printStackTrace();
            throw new RuntimeException("Unable to broadcast update broadcast");
        }

    }

    private ResponseEntity<String> defer(String database, String body, Node node) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        return new RestTemplate().postForEntity(format(BroadcastUtils.URL, node.getAddress(), "defer_update", database), entity, String.class);
    }

}
