package com.atypon.project.worker.index;

import java.util.Objects;

public class IndexKey {
    private String databaseName;
    private String field;

    public IndexKey(String databaseName, String field) {
        this.databaseName = databaseName;
        this.field = field;
    }

    public String getDatabaseName() {
        return databaseName;
    }

    public String getField() {
        return field;
    }

    public String getName() {
        return String.format("%s_%s", databaseName, field);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexKey key = (IndexKey) o;
        return databaseName.equals(key.databaseName) && field.equals(key.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, field);
    }

    @Override
    public String toString() {
        return "IndexKey{" +
                "databaseName='" + databaseName + '\'' +
                ", field='" + field + '\'' +
                '}';
    }
}
