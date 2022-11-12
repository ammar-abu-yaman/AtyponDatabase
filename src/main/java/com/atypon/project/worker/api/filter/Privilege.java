package com.atypon.project.worker.api.filter;

import com.atypon.project.worker.user.User;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class Privilege implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        String url = req.getRequestURI();

        if(url.startsWith("/database") || url.startsWith("/index")) {
            User user = (User) req.getSession(false).getAttribute("user");
            if(user == null || user.getRole() != User.Role.Admin) {
                resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
                return;
            }
        }

        chain.doFilter(request, response);
    }
}
