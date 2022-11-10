package com.atypon.project.worker.database;

import com.atypon.project.worker.request.DatabaseRequest;

import java.util.UUID;

public class UUIDIdCreator implements IdCreator {
    @Override
    public String createId(DatabaseRequest request) {
        return UUID.randomUUID().toString();
    }
}
