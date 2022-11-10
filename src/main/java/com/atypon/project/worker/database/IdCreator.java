package com.atypon.project.worker.database;

import com.atypon.project.worker.request.DatabaseRequest;

public interface IdCreator {

    public String createId(DatabaseRequest request);

}
