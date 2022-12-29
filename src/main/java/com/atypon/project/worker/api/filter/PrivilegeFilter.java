package com.atypon.project.worker.api.filter;

import com.atypon.project.worker.core.User;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@Component
@Order(3)
public class PrivilegeFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        String url = req.getRequestURI();

        // user trying to create or delete databases and indexes must be admin
        if(url.startsWith("/database") || url.startsWith("/index")) {
            User user = (User) req.getSession(false).getAttribute("user");
            if(user == null || user.getRole() != User.Role.Admin) {
                resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                return;
            }
        }

        // user trying to modify data must be admin or editor
        if(url.startsWith("/document/add") || url.startsWith("/document/delete") || url.startsWith("/document/update")) {
            User user = (User) req.getSession(false).getAttribute("user");
            if(user == null || user.getRole() == User.Role.Viewer) {
                resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                return;
            }
        }

        // user with viewer access level or higher or a worker node
        chain.doFilter(request, response);
    }
}
