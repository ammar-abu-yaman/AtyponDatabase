package com.atypon.project.worker.database;

import com.atypon.project.worker.query.Query;

import java.util.UUID;

public class UUIDIdCreator implements IdCreator {
    @Override
    public String createId(Query request) {
        return UUID.randomUUID().toString();
    }
}
