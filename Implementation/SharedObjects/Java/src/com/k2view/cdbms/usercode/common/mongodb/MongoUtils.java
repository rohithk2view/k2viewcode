package com.k2view.cdbms.usercode.common.mongodb;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.BsonDateTime;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import com.k2view.broadway.model.Data;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorAbstract;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;

public class MongoUtils {
    // private static Log log = Log.a(MongoUtils.class);

    static void normalizeMongoDocument(Document doc, boolean binaryAsBytes) {
        for (var entry : doc.entrySet()) {
            if (entry.getKey().equals("_id") && entry.getValue() instanceof ObjectId oid) {
                entry.setValue(oid.toHexString());
            } else if (binaryAsBytes && entry.getValue() instanceof Binary b) {
                entry.setValue(b.getData());
            }
        }
    }

    static Document getParentRowsFilter(Iterator<Map<String, Object>> parentRowsItr, int parentRowsChunkSize) {
        int rowCnt = 0;
        List<Document> orConditions = new ArrayList<>();
        while (parentRowsItr.hasNext() && rowCnt < parentRowsChunkSize) {
            Map<String, Object> conditionMap = parentRowsItr.next();
            Document condition = new Document();
            for (Map.Entry<String, Object> entry : conditionMap.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (key.toLowerCase().equals("_id")) {
                    value = getObjectId(value);
                }
                condition.append(key, value);
            }
            orConditions.add(condition);
            rowCnt++;
        }
        return orConditions.isEmpty() ? new Document() : new Document("$or", orConditions);
    }

    @SuppressWarnings("unchecked")
    static Document dataAsDoc(Object data) {
        // Build document either from a map, or from extended-json string
        Document parsedData;
        if (data instanceof Map m) {
            replaceObjectId(m);
            parsedData = new Document(m);
        } else if (data instanceof String s && !s.isEmpty()) {
            parsedData = Document.parse(s);
        } else {
            parsedData = new Document();
        }
        return parsedData;
    }

    @SuppressWarnings("unchecked")
    static String parseUserFilter(Data input) {
        String filter = input.string(MongoDbActorAbstract.INPUT_FILTER);
        return Util.dynamicString(filter, true, k -> {
            String fieldName;
            if (k.charAt(0) == '@') {
                fieldName = k.substring(1).toLowerCase();
            } else {
                fieldName = k.toLowerCase();
            }
            Object v = null;
            AtomicBoolean found = new AtomicBoolean(false);
            v = getValueCaseInsensitive(fieldName, input.fields(), found);
            if (!found.get()) {
                v = getValueCaseInsensitive(fieldName,
                        (Map<String, Object>) input.map(MongoDbActorAbstract.INPUT_FILTER_PARAMS), found);
            }
            if (found.get())
                return String.format("%s", filterParamValueToJson(v));
            throw new IllegalArgumentException(String.format("Please provide value for field '%s'", fieldName));
        });
    }

    static void flattenDocument(Document next) {
        next.replaceAll((k, v) -> {
            Json json = Json.get();
            if (v instanceof List || v instanceof Map) {
                return json.toJson(v);
            } else if (v instanceof Binary) {
                // result.put(k, b.getData());
                return json.toJson(v);
            } else if (v instanceof BsonTimestamp) {
                // result.put(k, t.getTime());
                return json.toJson(v);
            } else if (v instanceof Decimal128) {
                // result.put(k, d);
                return json.toJson(v);
            } else if (v instanceof ObjectId) {
                return json.toJson(v);
            }
            return v;
        });
    }

    private static Object getObjectId(Object id) {
        if (id instanceof String s && ObjectId.isValid(s)) {
            return new ObjectId(s); // Convert valid 24-char hex string to ObjectId
        }
        return id;
    }

    private static void replaceObjectId(Map<String, Object> m) {
        AtomicBoolean objectIdFound = new AtomicBoolean();
        Object objectId = getValueCaseInsensitive("_id", m, objectIdFound);
        if (objectIdFound.get()) {
            replaceValueCaseInsensitive("_id", m, getObjectId(objectId));
        }
    }

    private static String filterParamValueToJson(Object value) {
        if (value instanceof String s) {
            // escape json string
            return "\"" + s.replace("\"", "\\\"") + "\"";
        } else if (value instanceof Decimal128) {
            return "{ \"$numberDecimal\": \"" + value.toString() + "\" }";
        } else if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        } else if (value instanceof Date) {
            return "{ \"$date\": \"" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").format((Date) value)
                    + "\" }";
        } else if (value instanceof ObjectId) {
            return "{ \"$oid\": \"" + value.toString() + "\" }";
        } else if (value instanceof BsonDateTime) {
            return "{ \"$date\": \"" + Instant.ofEpochMilli(((BsonDateTime) value).getValue()).toString() + "\" }";
        } else {
            throw new IllegalArgumentException("Unsupported type for parameter: " + value.getClass().getName());
        }
    }

    private static Object getValueCaseInsensitive(String key, Map<String, Object> map, AtomicBoolean found) {
        for (String in : map.keySet()) {
            if (in.toLowerCase().equals(key)) {
                found.set(true);
                return map.get(key);
            }
        }
        found.set(false);
        return null;
    }

    private static void replaceValueCaseInsensitive(String key, Map<String, Object> map, Object newValue) {
        for (String in : map.keySet()) {
            if (in.toLowerCase().equals(key)) {
                map.put(in, newValue);
                return;
            }
        }
    }
}
