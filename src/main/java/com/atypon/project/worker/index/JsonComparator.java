package com.atypon.project.worker.index;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.Comparator;

public class JsonComparator implements Comparator<JsonNode>, Serializable {
    @Override
    public int compare(JsonNode o1, JsonNode o2) {
        int rank1 = getTypeRank(o1);
        int rank2 = getTypeRank(o2);
        if(rank1 != rank2)
            return Integer.compare(rank1, rank2);
        return compareSameType(o1, o2);
    }

    private int getTypeRank(JsonNode o) {
        switch (o.getNodeType()) {
            case NULL:
                return 0;
            case BOOLEAN:
                return 1;
            case NUMBER:
                return 2;
            case STRING:
                return 3;
            case ARRAY:
                return 4;
            case OBJECT:
                return 5;
        }
        return 0;
    }

    private int compareSameType(JsonNode o1, JsonNode o2) {
        if(o1.isTextual() || o1.isArray() || o1.isObject())
            return o1.asText().compareTo(o2.asText());
        if(o1.isNumber())
            return Double.compare(o1.asDouble(), o2.asDouble());
        if(o1.isBoolean())
            return Boolean.compare(o1.asBoolean(), o2.asBoolean());
       return o1.asText().compareTo(o2.asText());
    }



}
