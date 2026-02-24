package com.k2view.cdbms.usercode.common.dataverse;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

class DataverseMetadataCache implements AutoCloseable{
    private final Map<String, Map<String, Object>> logicalToMetadata = new HashMap<>();
    private final Map<String, String> entitySetToLogical = new HashMap<>();

    public synchronized void put(Map<String, Object> metadata) {
        String logicalName = (String) metadata.get("LogicalName");
        String entitySetName = (String) metadata.get("EntitySetName");

        if (logicalName == null || entitySetName == null) {
            throw new IllegalArgumentException("Metadata must contain 'LogicalName' and 'EntitySetName'");
        }

        // Remove old reverse mapping
        if (logicalToMetadata.containsKey(logicalName)) {
            String oldEntitySet = (String) logicalToMetadata.get(logicalName).get("EntitySetName");
            entitySetToLogical.remove(oldEntitySet);
        }

        if (entitySetToLogical.containsKey(entitySetName)) {
            String oldLogical = entitySetToLogical.get(entitySetName);
            logicalToMetadata.remove(oldLogical);
        }

        logicalToMetadata.put(logicalName, new HashMap<>(metadata));
        entitySetToLogical.put(entitySetName, logicalName);
    }

    public synchronized Map<String, Object> getByLogicalName(String logicalName) {
        return logicalToMetadata.get(logicalName);
    }

    public synchronized Map<String, Object> tableMetadata(String entitySetName) {
        String logicalName = entitySetToLogical.get(entitySetName);
        return logicalName != null ? logicalToMetadata.get(logicalName) : null;
    }

    public synchronized String getEntitySetName(String logicalName) {
        Map<String, Object> metadata = logicalToMetadata.get(logicalName);
        return metadata != null ? (String) metadata.get("EntitySetName") : null;
    }

    public synchronized String getLogicalName(String entitySetName) {
        return entitySetToLogical.get(entitySetName);
    }

    public synchronized Map<String, Map<String, Object>> getAll() {
        return Collections.unmodifiableMap(logicalToMetadata);
    }

    public synchronized boolean containsLogicalName(String logicalName) {
        return logicalToMetadata.containsKey(logicalName);
    }

    public synchronized boolean containsEntitySetName(String entitySetName) {
        return entitySetToLogical.containsKey(entitySetName);
    }

    @Override
    public void close() throws Exception {
        this.logicalToMetadata.clear();
        this.entitySetToLogical.clear();
    }
}