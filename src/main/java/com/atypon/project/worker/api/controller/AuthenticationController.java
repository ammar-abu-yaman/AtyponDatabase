package com.atypon.project.worker.api.controller;

import com.atypon.project.worker.query.Query;
import com.atypon.project.worker.user.User;
import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.query.QueryType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import javax.servlet.http.HttpSession;

@RestController
public class AuthenticationController {

    ObjectMapper mapper = new ObjectMapper();
    DatabaseManager manager = DatabaseManager.getInstance();

    @PostMapping("/login")
    public String login(HttpSession session, @RequestBody Credentials credentials) throws JsonProcessingException {
        Query request = Query.builder()
                .originator(Query.Originator.User)
                .queryType(QueryType.Login)
                .payload(mapper.valueToTree(credentials))
                .build();
        DatabaseManager.getInstance().getHandlersFactory().getHandler(request).handle(request);
        if(request.getStatus() == Query.Status.Accepted) {
            JsonNode json = mapper.readTree(request.getRequestOutput().toString());
            User user = new User(
                    json.get("username").asText(),
                    json.get("passwordHash").asText(),
                    User.getRole(json.get("role").asText()),
                    json.get("nodeId").asText());
            System.out.println("Correctly logged in");
            session.setAttribute("user", user);
        } else {
            System.out.println("Something went wrong");
        }
        return request.getRequestOutput().toString();
    }

    @PostMapping("/register")
    public String register(@RequestBody Credentials credentials) {
        Query query = Query.builder()
                .databaseName("_Users")
                .originator(Query.Originator.User)
                .queryType(QueryType.RegisterUser)
                .payload(mapper.valueToTree(credentials))
                .build();

        manager.getHandlersFactory().getHandler(query).handle(query);
        return query.getRequestOutput().toString();
    }


    @PostMapping("/logout")
    public void logout(HttpSession session) {
        session.removeAttribute("user");
    }

    @Getter
    @NoArgsConstructor
    private static class Credentials {
        private String username;
        private String password;

        public Credentials(String username, String password) {
            this.username = username;
            this.password = password;
        }
    }
}
