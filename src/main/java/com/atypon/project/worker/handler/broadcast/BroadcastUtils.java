package com.atypon.project.worker.handler.broadcast;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.String.format;

public class BroadcastUtils {

    // URL string template used in broadcasting
    public final static String URL = "http://%s:8000/_internal/%s/%s";

    // ThreadPool to be used to broadcast updates asynchronously
    private static ExecutorService executor = Executors.newCachedThreadPool();

    private static List<Node> nodes = DatabaseManager.getInstance().getConfiguration().getNodes();

    // async broadcasting
    public static void broadcast(String action, String info, String body) {
        executor.submit(() -> sendToNodes(action, info, body));
    }

    // send message to all nodes
    public static void sendToNodes(String action, String info, String body) {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        for (Node node : nodes) {
            try {
                new RestTemplate().postForEntity(format(URL, node.getAddress(), action, info), entity, String.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
