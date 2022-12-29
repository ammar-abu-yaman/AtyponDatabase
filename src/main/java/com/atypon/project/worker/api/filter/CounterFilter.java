package com.atypon.project.worker.api.filter;

import com.atypon.project.worker.core.DatabaseManager;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import java.io.IOException;

/*
* A Count Filter that count incoming requests to the node
* And decrement the count when requests leave the node
* to keep track of node congestion
*/

@Component
@Order(4)
public class CounterFilter implements Filter {

    DatabaseManager manager = DatabaseManager.getInstance();

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        manager.incrementNumRequests();
        chain.doFilter(request, response);
        manager.decrementNumRequests();
    }
}
