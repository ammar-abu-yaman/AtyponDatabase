package com.atypon.project.worker.api.filter;


import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.util.List;

/*
* Filter that guarantee access to logged in users or for other nodes in the clusters only
* */

@Component
@Order(2)
public class AuthFilter implements Filter {

    @Value("${server.trustedPort:8000}")
    private String trustedPort;

    @Value("${server.trustedPathPrefix:/_internal}")
    private String trustedInternalPathPrefix;

    private List<Node> nodes = DatabaseManager.getInstance().getConfiguration().getNodes();
    private String bootstrapAddress = DatabaseManager.getInstance().getConfiguration().getBootstrapAddress();


    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) servletRequest;
        HttpServletResponse resp = (HttpServletResponse) servletResponse;
        String url = req.getRequestURI();

        // trying to access auth apis or a node is trying to access the system
        if(url.startsWith("/login")
                || url.startsWith("/logout")
                || url.startsWith("/register")
                || isNodeAccess(req)
        ) {
            filterChain.doFilter(servletRequest, servletResponse);
            return;
        }

        // user trying to access internal api
        if(url.startsWith("/_internal")) {
            resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        HttpSession session = req.getSession(false);

        // user is not logged in
        if(session == null || session.getAttribute("user") == null) {
            resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return;
        }
        filterChain.doFilter(servletRequest, servletResponse);
    }

    boolean isNodeAccess(HttpServletRequest request) {
        String address = request.getRemoteAddr();
        int port = request.getLocalPort();
        return port == Integer.parseInt(trustedPort) // node is accessing through port 8000
                && (nodes.stream().anyMatch(node -> node.getAddress().equals(address)) // worker node is accessing
                        || address.equals(bootstrapAddress) // is the bootstrap node
        );
    }
}
