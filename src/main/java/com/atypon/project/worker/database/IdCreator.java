package com.atypon.project.worker.database;

import com.atypon.project.worker.query.Query;

public interface IdCreator {

    public String createId(Query request);

}
