package com.atypon.project.worker.api.filter;

import com.atypon.project.worker.core.DatabaseManager;
import com.atypon.project.worker.core.Node;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;

//@Component
//@Order(2)
public class InternalFilter implements Filter {

    List<Node> nodes = DatabaseManager.getInstance().getConfiguration().getNodes();

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        String url = req.getRequestURI();

        // A non node user trying to access internal communication api
        if(url.startsWith("/_internal") && !validateNode(req)) {
            resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        chain.doFilter(request, response);
    }

    boolean validateNode(HttpServletRequest httpRequest) {
        String address = httpRequest.getRemoteAddr();
        return nodes.stream().anyMatch(node -> node.getAddress().equals(address));
    }
}
