package com.atypon.project.worker.database;

import com.atypon.project.worker.request.Query;

public interface IdCreator {

    public String createId(Query request);

}
