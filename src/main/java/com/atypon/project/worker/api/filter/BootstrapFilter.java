package com.atypon.project.worker.api.filter;

import com.atypon.project.worker.core.DatabaseManager;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


//@Component
//@Order(1)
public class BootstrapFilter implements Filter {

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        String url = req.getRequestURI();

        if(url.startsWith("/register") && !DatabaseManager.getInstance().getConfiguration().isBootstrap()) {
            resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        // trying to access worker resources on a bootstrap node
        if(DatabaseManager.getInstance().getConfiguration().isBootstrap()
                && (url.startsWith("/document")
                || url.startsWith("/database")
                || url.startsWith("/index")
                || url.startsWith("/login")
                || url.startsWith("/logout"))) {
            resp.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return;
        }

        chain.doFilter(request, response);
    }
}
