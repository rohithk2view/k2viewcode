package com.k2view.cdbms.usercode.common.dataverse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import com.k2view.broadway.actors.builtin.JsonStringify;
import com.k2view.broadway.exception.AbortException;
import com.k2view.broadway.util.InputStreamIterator;
import com.k2view.cdbms.interfaces.jobs.custom.CustomConnection;
import com.k2view.cdbms.interfaces.jobs.http.HTTPConnection;
import com.k2view.cdbms.usercode.common.dataverse.actors.DataverseTableQueryActor;
import com.k2view.cdbms.usercode.common.dataverse.actors.DataverseUpdateLookupsActor;
import com.k2view.cdbms.usercode.common.dataverse.actors.DataverseBulkDelete;
import com.k2view.fabric.common.ByteStream;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.SupplierIterator;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;
import com.k2view.fabric.common.io.IoSession;
import com.k2view.fabric.common.io.basic.IoSimpleResultSet;
import com.k2view.fabric.common.io.basic.IoSimpleRow;
import com.k2view.fabric.common.threadPool.K2ThreadPool;
import com.k2view.cdbms.usercode.common.dataverse.actors.DataverseBulkDeleteAwait;

public class DataverseIoSession extends AbstractIoSession {
    private static enum LoadOperation {
        create("POST"),
        update("PATCH"),
        upsert("PATCH");

        private final String method;

        LoadOperation(String method) {
            this.method = method;
        }
    }

    private static final String BASE_PATH = "/api/data/v9.2/";
    private static final Log log = Log.a(DataverseIoSession.class);
    private static final String EOL = "\r\n"; // batch API requires CRLF line ending

    protected final boolean debug;
    protected final boolean addForeignKeys;
    protected final String discoveryMode;
    protected final String catalogAttributesFilter;
    protected final String catalogTablesFilter;
    protected final String catalogSchemasFilter;

    private final HTTPConnection connection;
    private final Map<String, Object> sessionProps;
    private final int batchSize;
    private final Queue<Future<Void>> batchQueue = new LinkedList<>();

    private IoSession ioStreamHttpSession; // for use via read() and write() only
    private boolean inTrx;
    private String currBatchId;
    private StringBuilder batchData = new StringBuilder();
    private int currBatchSize = 0;
    private boolean inChangeset = false;
    private String currChangesetId;
    private boolean batchDebug;
    private DataverseMetadataCache metadataCache;
    private String callingActor;
    private int maxBatchQueueSize;
    private AtomicReference<Exception> error = new AtomicReference<>();
    private K2ThreadPool executors;
    private boolean isBulkDelete;
    private boolean isBulkDeleteAwait;
    private boolean isTableQuery;
    private boolean isUpdateLookups;

    public DataverseIoSession(Map<String, Object> sessionArgs) {
        this.connection = (HTTPConnection) sessionArgs.get(CustomConnection.DELEGATE);
        this.sessionProps = sessionArgs;
        this.debug = ParamConvertor.toBool(sessionArgs.get("DEBUG"));
        this.discoveryMode = ParamConvertor.toString(sessionArgs.get("DISCOVERY_MODE"));
        this.catalogTablesFilter = ParamConvertor.toString(sessionArgs.get("TABLES_FILTER"));
        this.catalogAttributesFilter = ParamConvertor.toString(sessionArgs.get("ATTRIBUTES_FILTER"));
        this.catalogSchemasFilter = ParamConvertor.toString(sessionArgs.get("SOLUTIONS_FILTER"));
        this.batchSize = (int) ParamConvertor.toInteger(sessionArgs.get("BATCH_SIZE"));
        this.addForeignKeys = ParamConvertor.toBool(sessionArgs.get("FOREIGN_KEYS"));
        this.metadataCache = new DataverseMetadataCache();
        this.callingActor = ParamConvertor.toString(sessionArgs.get("ActorClass"));
        this.isBulkDelete = callingActor.equals(DataverseBulkDelete.class.getSimpleName());
        this.isBulkDeleteAwait = callingActor.equals(DataverseBulkDeleteAwait.class.getSimpleName());
        this.isTableQuery = callingActor.equals(DataverseTableQueryActor.class.getSimpleName());
        this.isUpdateLookups = callingActor.equals(DataverseUpdateLookupsActor.class.getSimpleName());
        this.maxBatchQueueSize = (int) ParamConvertor.toInteger(sessionArgs.get("MAX_CONCURRENT_BATCHES"));
    }

    protected void resetIoStreamHttpSession() {
        Util.safeClose(ioStreamHttpSession);
        ioStreamHttpSession = null;
    }

    @Override
    public Statement statement() throws Exception {
        return new DataverseStatement();
    }

    private void log(String message, Object... args) {
        if (debug) {
            log.info(message, args);
        } else {
            log.debug(message, args);
        }
    }

    @Override
    public InputStream read(Map<String, Object> params) throws Exception {
        modifyParams(params);
        if (ioStreamHttpSession == null) {
            // in case it's a GET
            this.ioStreamHttpSession = connection.getIoSession(params);
        }
        return ioStreamHttpSession.read(params);
    }

    private void modifyParams(Map<String, Object> params) {
        params.put("path", path(params));
        params.put("debug", debug(params));
    }

    @Override
    public OutputStream write(Map<String, Object> params) throws Exception {
        modifyParams(params);
        // httpSession should always be null here
        this.ioStreamHttpSession = connection.getIoSession(params);
        return ioStreamHttpSession.write(params);
    }

    private String path(Map<String, Object> params) {
        return BASE_PATH + params.get("path");
    }

    @Override
    public void close() throws Exception {
        Util.safeClose(ioStreamHttpSession);
        ioStreamHttpSession = null;
        Util.safeClose(connection);
        Util.safeClose(metadataCache);
        Util.safeClose(executors);
        executors = null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getMetadata(Map<String, Object> params) throws Exception {
        return (T) new DataverseMetadata(this, params, (String) sessionProps.get("interface"));
    }

    @Override
    public boolean isTransactional() throws Exception {
        return true;
    }

    @Override
    public void beginTransaction() throws Exception {
        this.inTrx = true;
    }

    @Override
    public void commit() throws Exception {
        if (!this.batchData.isEmpty()) {
            log.debug("Committing last batch");
            batchQueue.add(executeBatch());
        }
        clearFutures(false);
        this.inTrx = false;
    }

    @Override
    public void rollback() throws Exception {
        this.clearFutures(true);
        this.inTrx = false;
    }

    @Override
    public void testConnection() {
        Map<String, Object> input = Util.map("path", BASE_PATH + "WhoAmI", "debug", debug);
        Util.rte(() -> {
            try (var s = connection.getIoSession(input)) {
                try (var res = s.read(input)) {
                }
            }
        });
    }

    private Future<Void> executeBatch() throws Exception, IOException {
        log("Executing batch");
        if (this.error.get() != null) {
            // Prevent a new dispatch in case of a previous failure
            throw error.get();
        }
        if (currBatchSize > 0) {
            String contentType = String.format("multipart/mixed; boundary=batch_%s", currBatchId);
            Map<String, Object> headers = Util.map(
                    "Content-Type", contentType);

            endBatchEntry(inChangeset);

            Map<String, Object> batchInput = Util.map(
                    "path", "$batch",
                    "headers", headers,
                    "stream", batchData.toString(),
                    "method", "POST",
                    "debug", batchDebug || debug);
            modifyParams(batchInput);
            try (var httpSession = connection.getIoSession(batchInput)) {
                try (OutputStream os = httpSession.write(batchInput)) {
                    writeToOutputStream(batchInput, os);
                }
                batchData = new StringBuilder();
                currBatchId = null;
                currChangesetId = null;
                currBatchSize = 0;
                inChangeset = false;
                return CompletableFuture.runAsync(() -> {
                    try (var is = httpSession.read(batchInput)) {
                    } catch (Exception e) {
                        this.error.set(e);
                    } finally {
                        log("Batch execution done");
                        // Util.safeClose(httpSession);
                        // httpSession = null;
                    }
                }, executors);
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    private void endBatchEntry(boolean isChangeset) {
        if (isChangeset) {
            batchData.append(EOL).append("--changeset_").append(currChangesetId).append("--").append(EOL);
        }
        batchData.append(EOL).append("--batch_").append(currBatchId).append("--").append(EOL);
    }

    private void writeToOutputStream(Map<String, Object> input, OutputStream os) throws IOException, Exception {
        Object stream = input.get("stream");
        if (stream != null) {
            for (byte[] buff : ByteStream.toIterableBuffer(stream)) {
                os.write(buff);
            }
        }
    }

    @Override
    public void abort() throws Exception {
        this.error.set(new AbortException("Dataverse session aborted!"));
    }

    private boolean debug(Map<String, Object> input) {
        return ParamConvertor.toBool(input.get("debug")) || debug;
    }

    private void clearFutures(boolean ignoreError) throws Exception {
        for (Future<Void> f : batchQueue) {
            f.get();
        }
        batchQueue.clear();
        if (!ignoreError && this.error.get() != null) {
            throw (Exception) error.get();
        }
    }

    private class DataverseStatement implements Statement {

        @SuppressWarnings("unchecked")
        @Override
        public void batch(Object... params) throws Exception {
            if (executors == null) {
                executors = new K2ThreadPool.Builder("Dataverse batch thread pool", maxBatchQueueSize).build();
            }
            Map<String, Object> input = (Map<String, Object>) params[0];
            boolean isAtomic = ParamConvertor.toBool(input.get("atomic"));
            boolean skipLookups = ParamConvertor.toBool(input.get("skip_lookups"));

            Object payloadObj = input.get("payload");
            if (!(payloadObj instanceof Map payloadMap)) {
                throw new IllegalArgumentException("Expected a not null Map payload.");
            }
            boolean isAsync = ParamConvertor.toBool(input.get("async"));
            String path = (String) input.get("path");
            String table = path.split("/")[0].split("\\(")[0];

            fetchAndCacheMetadata(table, true, false);

            boolean endOfChangeSet = inChangeset && !isAtomic;
            boolean startOfChangeSet = !inChangeset && isAtomic;
            inChangeset = isAtomic;
            batchDebug = ParamConvertor.toBool("debug");
            if (!inTrx) {
                throw new IllegalStateException("Batch mode is supported in transaction mode only.");
            }
            if (currBatchId == null) {
                currBatchId = Util.fastUUID().toString();
            }

            if (endOfChangeSet) {
                endBatchEntry(true);
                currChangesetId = null;
            }

            if (!isAtomic || startOfChangeSet) {
                startBatchEntry(isAtomic);
            }

            if (isAtomic) {
                startChangesetEntry();
            }

            batchData.append("Content-Type: application/http").append(EOL);

            batchData.append("Content-Transfer-Encoding: binary").append(EOL);

            if (isAtomic) {
                batchData.append("Content-ID: ").append(currBatchSize).append(EOL);
            }

            batchData.append(EOL);

            String method = batchEntryMethod(input);
            String entryPath = batchEntryPath(input, payloadMap, table, method);

            batchData.append(method).append(" ").append(entryPath).append(" HTTP/1.1").append(EOL);

            batchData.append("Content-Type: application/json; type=entry").append(EOL);

            if (ParamConvertor.toString(input.get("operation")).equals(LoadOperation.update.name())) {
                batchData.append("If-Match: *").append(EOL);
            }

            batchData.append(EOL);

            // now add user payload
            Map<String, Object> parsedPayload = parsePayload(payloadMap, table, skipLookups);

            String payload = Json.get().toJson(parsedPayload);
            batchData.append(payload);

            if (++currBatchSize >= batchSize) {
                if (batchQueue.size() >= maxBatchQueueSize) {
                    batchQueue.poll().get();
                }
                Future<Void> f = executeBatch();
                if (!isAsync) {
                    f.get();
                } else {
                    batchQueue.add(f);
                }
            }
        }

        private String batchEntryPath(Map<String, Object> input, Map<String, Object> payloadMap, String table,
                String method) {
            StringBuilder path = new StringBuilder();
            path.append(BASE_PATH).append(input.get("path"));
            if (method.equals("PATCH")) {
                path.append("(").append(guidOrAlternateKey(payloadMap, table)).append(")");
            }
            return path.toString();
        }

        private String batchEntryMethod(Map<String, Object> input) {
            if (input.containsKey("operation"))
                return LoadOperation.valueOf((String) input.get("operation")).method;
            return (String) input.get("method");
        }

        private String guidOrAlternateKey(Map<String, Object> payloadMap, String table) {
            Map<String, Object> tableMetadata = metadataCache.tableMetadata(table);
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> alternateKeys = (List<Map<String, Object>>) tableMetadata.get("Keys");

            // Use the primary ID fallback early if no alternate keys
            if (Util.isEmpty(alternateKeys)) {
                String idFieldName = (String) tableMetadata.get("PrimaryIdAttribute");
                return (String) payloadMap.get(idFieldName);
            }

            @SuppressWarnings("unchecked")
            List<String> alternateKeyFields = (List<String>) alternateKeys.get(0).get("KeyAttributes");

            // Early exit if empty
            if (Util.isEmpty(alternateKeyFields)) {
                String idFieldName = (String) tableMetadata.get("PrimaryIdAttribute");
                return (String) payloadMap.get(idFieldName);
            }

            Set<String> keys = payloadMap.keySet();
            boolean hasAllKeys = true;
            for (String k : alternateKeyFields) {
                if (!(keys.contains(k) || keys.contains("_" + k + "_value"))) {
                    hasAllKeys = false;
                    break;
                }
            }

            if (!hasAllKeys) {
                // Use primary key (guid)
                String idFieldName = (String) tableMetadata.get("PrimaryIdAttribute");
                return (String) payloadMap.get(idFieldName);
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < alternateKeyFields.size(); i++) {
                String k = alternateKeyFields.get(i);
                String lookupKey = "_" + k + "_value";
                boolean isLookup = !keys.contains(k);
                String actualKey = isLookup ? lookupKey : k;
                Object value = payloadMap.get(actualKey);
                sb.append(actualKey).append("=");
                if (!isLookup) {
                    sb.append("'").append(value).append("'");
                } else {
                    sb.append(value);
                }
                if (i < alternateKeyFields.size() - 1)
                    sb.append(",");
            }
            return sb.toString();
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> parsePayload(Map<String, Object> payload, String table, boolean skipLookups) {
            Map<String, Object> parsedPayload = new LinkedHashMap<>();
            Map<String, Object> tableMetadata = metadataCache.tableMetadata(table);
            List<Map<String, Object>> tableAttributes = (List<Map<String, Object>>) tableMetadata.get("Attributes");
            List<Map<String, Object>> manyToOneRelationships = (List<Map<String, Object>>) tableMetadata
                    .get("ManyToOneRelationships");
            HashSet<String> polymorphicLookups = new HashSet<>();
            HashSet<String> validForCreate = new HashSet<>();

            // add all attributes
            for (var attribute : tableAttributes) {
                if ((boolean) attribute.get("IsValidForCreate")) {
                    String attributeName = (String) attribute.get("LogicalName");
                    validForCreate.add(attributeName);
                    if (!isUpdateLookups) {
                        parsedPayload.put(attributeName, parseToDataverseValue(payload.get(attributeName), attribute));
                    }
                    Object targets = attribute.get("Targets");
                    if (targets instanceof List l && l.size() > 1 && !attributeName.equals("ownerid")) {
                        polymorphicLookups.add(attributeName);
                    }
                }
            }

            // recalculate lookup attributes
            for (var relation : manyToOneRelationships) {
                String referencingAttr = (String) relation.get("ReferencingAttribute");
                Object lookupVal = payload.get(referencingAttr);
                String referencedTable = (String) relation.get("ReferencedEntity");

                parsedPayload.remove(referencingAttr);

                if (skipLookups || !validForCreate.contains(referencingAttr)) {
                    continue;
                }

                String entitySetName = metadataCache.getEntitySetName(referencedTable);
                if (lookupVal instanceof Map) {
                    // meaning it's an expanded alternate key lookup
                    // can we have polymorphic lookups here?
                    Map<String, Object> alternateKey = new LinkedHashMap<>((Map<String, Object>) lookupVal);
                    alternateKey.remove(referencingAttr);

                    String binding = String.format("/%s(%s)", entitySetName,
                            buildKeyQueryFromMap(entitySetName, alternateKey));
                    parsedPayload.put(String.format("%s@odata.bind", referencingAttr), binding);
                } else {
                    String key = "_" + referencingAttr + "_value";
                    lookupVal = payload.get(key);
                    if (lookupVal != null) {
                        // parsedPayload.put(key, lookupVal);
                        String lookupLogicalNameKey = String.format("%s@Microsoft.Dynamics.CRM.lookuplogicalname",
                                key);
                        if (polymorphicLookups.contains(referencingAttr)) {
                            Object targetLookupLogicalName = payload.get(lookupLogicalNameKey);
                            if (targetLookupLogicalName == null) {
                                throw new IllegalArgumentException(String.format(
                                        "Attribute '%s' is a polymorphic lookup, you must provide a value for the target lookup logical name '%s'",
                                        referencingAttr, lookupLogicalNameKey));
                            }
                            parsedPayload.put(
                                    String.format("%s_%s@odata.bind", referencingAttr, targetLookupLogicalName),
                                    String.format("/%s(%s)", entitySetName, lookupVal));
                        } else {
                            String targetEntity;
                            if (referencedTable.equals("owner")) {
                                // TO-DO alternate keys can't be used on systemusers/teams
                                continue;
                                // Object targetLookupLogicalName = payload.get(lookupLogicalNameKey);
                                // if (targetLookupLogicalName == null) {
                                // throw new IllegalArgumentException(String.format(
                                // "Attribute '%s' is a polymorphic lookup to owners table and you must provide
                                // a value for the target lookup logical name '%s'",
                                // referencingAttr, lookupLogicalNameKey));
                                // }
                                // targetEntity = metadataCache.getEntitySetName((String)
                                // targetLookupLogicalName);
                                // if (payload.get("owninguser") != null) {
                                // Map<String, Object> tmp = new LinkedHashMap<>((Map<String, Object>)
                                // payload.get("owninguser"));
                                // tmp.remove("ownerid");
                                // tmp.remove("systemuserid");
                                // lookupVal = buildKeyQueryFromMap("systemusers", tmp);
                                // } else if (payload.get("owningteam") != null) {
                                // Map<String, Object> tmp = new LinkedHashMap<>((Map<String, Object>)
                                // payload.get("owningteam"));
                                // tmp.remove("ownerid");
                                // tmp.remove("teamid");
                                // lookupVal = buildKeyQueryFromMap("teams", tmp);
                                // } else {
                                // throw new IllegalArgumentException("Expected a value for owningteam or
                                // owninguser when _ownerid_value is not null");
                                // }
                            } else {
                                targetEntity = entitySetName;
                            }
                            parsedPayload.put(String.format("%s@odata.bind", referencingAttr),
                                    String.format("/%s(%s)", targetEntity, lookupVal));
                        }
                    }
                }
            }
            return parsedPayload;
        }

        private Object parseToDataverseValue(Object value, Map<String, Object> attribute) {
            if (value == null)
                return null;
            String type = (String) attribute.get("AttributeType");
            switch (type) {
                case "String":
                case "Memo":
                    return ParamConvertor.toString(value);
                case "Boolean":
                    return ParamConvertor.toBool(value);
                case "Datetime":
                    String format = (String) attribute.get("Format");
                    if ("DateOnly".equals(format)) {
                        return getDateOnlyStr(value);
                    } else {
                        // Assume DateAndTime
                        return getDateAndTimeStr(value);
                    }
                case "Integer":
                case "Bigint":
                    return ParamConvertor.toInteger(value);
                case "Decimal":
                case "Double":
                case "Money":
                    return ParamConvertor.toReal(value);
                case "Uniqueidentifier":
                case "EntityName":
                case "Lookup":
                    return ParamConvertor.toString(value);
                case "Picklist":
                case "State":
                case "Status":
                    return ParamConvertor.toInteger(value);
                case "MultiSelectPicklist":
                    return ParamConvertor.toString(value);
                default:
                    return ParamConvertor.toString(value);
            }
        }

        private static String getDateAndTimeStr(Object value) {
            Instant instant;
            if (value instanceof OffsetDateTime odt) {
                instant = odt.toInstant();
            } else if (value instanceof String s) {
                instant = OffsetDateTime.parse(s).toInstant();
            } else if (value instanceof Instant i) {
                instant = i;
            } else if (value instanceof LocalDateTime ldt) {
                // Assume UTC
                instant = ldt.toInstant(ZoneOffset.UTC);
            } else if (value instanceof LocalDate ld) {
                instant = ld.atStartOfDay()
                        .toInstant(ZoneOffset.UTC);
            } else if (value instanceof Timestamp ts) {
                instant = ts.toInstant();
            } else if (value instanceof Date sqlDate) {
                instant = sqlDate.toLocalDate()
                        .atStartOfDay()
                        .toInstant(ZoneOffset.UTC);
            } else {
                throw new IllegalArgumentException("Unexpected DateAndTime value: " + value);
            }

            return instant.toString();
        }

        private static String getDateOnlyStr(Object value) {
            LocalDate date;
            if (value instanceof LocalDate ld) {
                date = ld;
            } else if (value instanceof String s) {
                date = LocalDate.parse(s);
            } else if (value instanceof LocalDateTime ldt) {
                date = ldt.toLocalDate();
            } else if (value instanceof OffsetDateTime odt) {
                date = odt.toLocalDate();
            } else if (value instanceof Instant instant) {
                date = instant.atOffset(ZoneOffset.UTC).toLocalDate();
            } else if (value instanceof Date sqlDate) {
                date = sqlDate.toLocalDate();
            } else if (value instanceof Timestamp ts) {
                date = ts.toInstant()
                        .atOffset(ZoneOffset.UTC)
                        .toLocalDate();
            } else {
                throw new IllegalArgumentException("Unexpected DateOnly value: " + value);
            }

            return date.toString();
        }

        private String buildKeyQueryFromMap(String entitySetName, Map<String, Object> alternateKey) {
            StringBuilder sb = new StringBuilder();
            Iterator<Map.Entry<String, Object>> iterator = alternateKey.entrySet().iterator();

            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                sb.append(entry.getKey()).append("=");
                Object val = entry.getValue();
                if (val instanceof String) {
                    sb.append("'").append(val).append("'");
                } else {
                    sb.append(val);
                }
                if (iterator.hasNext()) {
                    sb.append(",");
                }
            }
            return sb.toString();
        }

        private void startChangesetEntry() {
            batchData.append("--").append("changeset").append("_").append(currChangesetId).append(EOL);
        }

        private void startBatchEntry(boolean isChangeset) {
            batchData.append(EOL).append("--").append("batch").append("_").append(currBatchId).append(EOL);
            if (isChangeset) {
                currChangesetId = Util.fastUUID().toString();
                batchData.append("Content-Type: multipart/mixed; boundary=changeset_").append(currChangesetId)
                        .append(EOL).append(EOL);
            }
        }

        @SuppressWarnings("unchecked")
        @Override
        public Result execute(Object... args) throws Exception {
            Map<String, Object> input = (Map<String, Object>) args[0];
            boolean isSkipMetadata = ParamConvertor.toBool(input.get("__skipMetadata"));
            boolean expandLookups = ParamConvertor.toBool(input.get("expand_lookups"));

            if (!isSkipMetadata && (isTableQuery || isBulkDelete)) {
                String path = (String) (isBulkDelete ? input.get("table") : input.get("path"));
                String cleanPath = path.split("\\?")[0]; // remove query string
                String[] segments = cleanPath.split("/");
                String table = segments.length > 0 ? segments[0].split("\\(")[0] : null;
                fetchAndCacheMetadata(table, !isBulkDelete, false);
            }

            if (expandLookups) {
                expandLookups(input);
            }

            if (isBulkDelete) {
                handleBulkDeleteInput(input);
            }

            input.put("timeout", 60000);
            // TO-DO make timeout configurable
            modifyParams(input);

            if (isBulkDeleteAwait) {
                return bulkDeleteAwait(input);
            }

            try (var httpSession = connection.getIoSession(input)) {
                if (input.get("method") != null && input.get("method").toString().toLowerCase().equals("post")) {
                    try (var os = httpSession.write(input)) {
                        // first write the data stream
                        writeToOutputStream(input, os);
                    }
                }
                // now read result
                log("Fetching first page");
                input.put("debug", debug(input));
                try (InputStream is = httpSession.read(input)) {
                    DataverseResult dataverseResult = new DataverseResult(is, input);
                    return dataverseResult;
                }
            }
        }

        private void handleBulkDeleteInput(Map<String, Object> input) throws Exception {
            String tableEntitySetName = (String) input.get("table");
            String tableLogicalName = metadataCache.getLogicalName(tableEntitySetName);
            String jobName = "";
            if (input.get("job_name") != null) {
                jobName = (String) input.get("job_name");
            }
            if (Util.isEmpty(jobName)) {
                jobName = String.format("Delete records from table '%s'", tableEntitySetName);
            }
            Map<String, Object> query = Util.map("EntityName", tableLogicalName);
            if (input.get("criteria") != null) {
                @SuppressWarnings("unchecked")
                Map<String, Object> criteria = (Map<String, Object>) input.get("criteria");
                query.put("Criteria", criteria);
            }
            if (ParamConvertor.toBool(input.get("retained"))) {
                query.put("DataSource", "retained");
            }
            List<Map<String, Object>> querySet = List.of(query);
            Map<String, Object> payload = Util.map(
                    "QuerySet", querySet,
                    "JobName", jobName,
                    "SendEmailNotification", false,
                    "RecurrencePattern", "",
                    "StartDateTime", "",
                    "ToRecipients", List.of(),
                    "CCRecipients", List.of());
            input.put("stream", Json.get().toJson(payload));
        }

        @SuppressWarnings("unchecked")
        private void expandLookups(Map<String, Object> input) throws Exception {
            Map<String, Object> params;
            Object paramsObj = input.get("params");
            if (paramsObj instanceof Map) {
                params = (Map<String, Object>) paramsObj;
            } else if (paramsObj instanceof String) {
                throw new UnsupportedOperationException("Please use params as object");
            } else {
                params = Util.map();
                input.put("params", params);
            }
            Map<String, Object> tableMetadata = metadataCache.tableMetadata((String) input.get("path"));
            List<Map<String, Object>> relationships = (List<Map<String, Object>>) tableMetadata
                    .get("ManyToOneRelationships");
            StringBuilder expandClause = new StringBuilder();
            Object selectClause = params.get("$select");
            Set<String> selectFields = new HashSet<>();
            if (selectClause != null) {
                selectFields
                        .addAll(Arrays.asList(selectClause.toString().split(",")).stream().map(f -> f.trim()).toList());
            }

            for (var relation : relationships) {
                String referencingCol = (String) relation.get("ReferencingAttribute");
                if (!selectFields.isEmpty() && !selectFields.contains(referencingCol)) {
                    continue;
                }
                String referencedTable = (String) relation.get("ReferencedEntity");
                if (!metadataCache.containsLogicalName(referencedTable)) {
                    fetchAndCacheMetadata(referencedTable, false, true);
                }
                Map<String, Object> referencedTableInfo = metadataCache.getByLogicalName(referencedTable);
                List<Map<String, Object>> alternateKeys = (List<Map<String, Object>>) referencedTableInfo.get("Keys");
                if (!alternateKeys.isEmpty()) {
                    List<String> alternateKeyFields = (List<String>) alternateKeys.get(0).get("KeyAttributes");
                    String fieldsToSelect = alternateKeyFields.stream().collect(Collectors.joining(","));
                    if (!expandClause.isEmpty()) {
                        expandClause.append(",");
                    }
                    expandClause.append(referencingCol).append("($select=").append(fieldsToSelect).append(")");
                }
            }
            if (!expandClause.isEmpty()) {
                String expandClauseStr = expandClause.toString();
                if (!params.containsKey("$expand")) {
                    params.put("$expand", expandClauseStr);
                } else {
                    params.put("$expand", String.format("%s,%s", params.get("$expand"), expandClauseStr));
                }
            }
        }

        @SuppressWarnings("unchecked")
        private Result bulkDeleteAwait(Map<String, Object> input) throws IOException, Exception {
            int jobStatusCode = -1;
            Map<String, Object> response = new LinkedHashMap<>();
            long waitForSeconds = (long) input.get("wait_for_seconds");
            long startTime = System.currentTimeMillis();

            // Exponential backoff: Sleep for 1,2,4,8,8,8,..
            int sleepMillis = 1000; // Start with 1 second
            int maxSleepMillis = 8000; // Cap at 8 seconds

            while (jobStatusCode != 30 && jobStatusCode != 31 && jobStatusCode != 32) {
                Util.sleep(sleepMillis);

                // Double the backoff, but cap it
                if (sleepMillis < maxSleepMillis) {
                    sleepMillis = Math.min(sleepMillis * 2, maxSleepMillis);
                }

                if (error.get() != null) {
                    throw error.get();
                }

                // Check timeout
                if (waitForSeconds > 0) {
                    long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
                    if (elapsedSeconds >= waitForSeconds) {
                        throw new TimeoutException(
                                "Timed out after waiting " + waitForSeconds + " seconds for bulk delete to complete.");
                    }
                }

                try (var httpSession = connection.getIoSession(input)) {
                    try (InputStream monitorResponse = httpSession.read(input)) {
                        ByteStream responseByteStream = InputStreamIterator.byteStream(monitorResponse);
                        String responseStr = ParamConvertor.toString(responseByteStream);
                        response = Json.get().fromJson(responseStr, response.getClass());
                        response = ((List<Map<String, Object>>) response.get("value")).getFirst();
                        jobStatusCode = (int) ParamConvertor.toInteger(response.get("statuscode"));
                    }
                    log("Delete job status code is {}", jobStatusCode);
                }
            }
            if (jobStatusCode != 30 || ParamConvertor.toInteger(response.get("failurecount")) > 0) {
                throw new Exception(
                        "Expected statuscode=30 and failurecount=0 in BulkDeleteOperation API response. Response="
                                + Json.get().toJson(response));
            }
            return new IoSimpleResultSet(response.keySet().toArray(new String[0]), null,
                    Collections.singletonList(response.values().toArray()));
        }

        private class DataverseResult implements Result {
            private String previousNextLink = null;
            private String nextLink;
            private Iterator<Map<String, Object>> rowsIterator;
            private final Json json = Json.get(JsonStringify.REGULAR_FEATURES);
            private final String origPath;
            private final Map<String, Object> input;
            private Map<String, Map<String, Object>> attributes;
            private Iterator<Row> iterator;

            DataverseResult(InputStream is, Map<String, Object> input) throws IOException {
                this.origPath = (String) input.get("path");
                this.input = input;
                updateRowsFromStream(is);
            }

            @SuppressWarnings("unchecked")
            private void updateRowsFromStream(InputStream is) throws IOException {
                ByteStream responseByteStream = InputStreamIterator.byteStream(is);
                Map<String, Object> response = new LinkedHashMap<>();
                String responseStr = ParamConvertor.toString(responseByteStream);
                response = json.fromJson(responseStr, response.getClass());
                if (response.containsKey("@odata.nextLink")) {
                    nextLink = (String) response.get("@odata.nextLink");
                } else {
                    nextLink = null;
                }
                Object value = response.get("value");
                List<Map<String, Object>> rows;
                if (value instanceof List) {
                    rows = (List<Map<String, Object>>) value;
                } else if (value == null) {
                    rows = List.of(response);
                } else {
                    rows = List.of((Map<String, Object>) value);
                }
                if (Util.isEmpty(rows)) {
                    rowsIterator = Collections.emptyIterator();
                } else {
                    rowsIterator = rows.iterator();
                }
            }

            private Row next() {
                while (true) {
                    if (error.get() != null) {
                        return null;
                    }
                    if (rowsIterator == null || !rowsIterator.hasNext()) {
                        if (nextLink == null) {
                            return null;
                        }
                        if (nextLink.equals(previousNextLink)) {
                            error.set(new RuntimeException(String.format(
                                    "Detected repeated @odata.nextLink, aborting to avoid infinite loop: %s",
                                    nextLink)));
                            nextLink = null;
                            return null;
                        }
                        previousNextLink = nextLink;

                        String nextLinkWithoutHost = nextLink.substring(nextLink.indexOf(origPath));
                        String nextLinkWithoutBasePath = nextLinkWithoutHost.substring(BASE_PATH.length());
                        log("Fetching next page using link {}", nextLinkWithoutHost);
                        input.put("path", nextLinkWithoutBasePath);
                        input.remove("params");

                        modifyParams(input);
                        try (var httpSession = connection.getIoSession(input);
                                InputStream newStream = httpSession.read(input)) {
                            updateRowsFromStream(newStream);
                        } catch (Exception e) {
                            nextLink = null;
                            rowsIterator = Collections.emptyIterator();
                            error.set(e);
                            return null;
                        }

                        // If updateRowsFromStream didn't populate anything useful, exit
                        if (rowsIterator == null || !rowsIterator.hasNext()) {
                            nextLink = null;
                            return null;
                        }
                    }

                    if (rowsIterator.hasNext()) {
                        Map<String, Object> row = rowsIterator.next();
                        if (isTableQuery && !ParamConvertor.toBool(input.get("__skipMetadata"))) {
                            row.replaceAll((k, v) -> parseValue(k, v));
                        }
                        return IoSimpleRow.factory(row.keySet()).apply(row.values().toArray());
                    }
                }
            }

            private Object parseValue(String key, Object v) {
                if (v == null)
                    return null;
                if (attributes == null) {
                    String[] split = ((String) input.get("path")).split("/");
                    String entitySetName = split[split.length - 1];
                    Map<String, Object> tableMetadata = metadataCache.tableMetadata(entitySetName);
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> attributesList = ((List<Map<String, Object>>) tableMetadata
                            .get("Attributes"));
                    attributes = attributesList.stream().collect(
                            Collectors.toMap(m -> (String) m.get("LogicalName"), m -> (Map<String, Object>) m));
                }
                if (!attributes.containsKey(key)) {
                    return v;
                }
                ;
                Map<String, Object> attribute = attributes.get(key);
                String format = attribute.containsKey("Format") ? (String) attribute.get("Format") : "None";
                String attributeType = (String) attribute.get("AttributeType");
                switch (attributeType) {
                    case "DateTime": {
                        String date = (String) v;
                        if ("DateOnly".equals(format)) {
                            LocalDate localDate = LocalDate.parse(date);
                            return Date.valueOf(localDate);
                        }
                        Instant instant =
                                OffsetDateTime.parse(date).toInstant();
                        return Timestamp.from(instant);
                    }
                    default:
                        return v;
                }
            }

            @Override
            public Iterator<Row> iterator() {
                if (this.iterator == null) {
                    this.iterator = new SupplierIterator<>(this::next);
                }
                return iterator;
            }

            @Override
            public void close() throws Exception {
                this.nextLink = null;
                this.rowsIterator = null;
                this.iterator = null;
                this.attributes = null;
                if (error.get() != null) {
                    throw error.get();
                }
            }
        }

    }

    void fetchAndCacheMetadata(String table, boolean recursive, boolean isLogicalTableName) throws Exception {
        if ((!isLogicalTableName && !metadataCache.containsEntitySetName(table))
                || (isLogicalTableName && !metadataCache.containsLogicalName(table))) {
            try (var s = statement()) {
                Map<String, Object> params = Util.map(
                        "$filter",
                        String.format("%s eq '%s'", isLogicalTableName ? "LogicalName" : "EntitySetName", table),
                        "$select", "EntitySetName,LogicalName,PrimaryIdAttribute",
                        "$expand",
                        "Keys($select=KeyAttributes), ManyToOneRelationships($select=ReferencingAttribute,ReferencedEntity,ReferencedAttribute),Attributes");
                Map<String, Object> input = Util.map(
                        "path", "EntityDefinitions",
                        "params", params,
                        "debug", debug,
                        "method", "GET",
                        "__skipMetadata", true);
                try (var r = s.execute(input)) {
                    Row tableMetadata = r.firstRow();
                    metadataCache.put(tableMetadata);
                    if (recursive) {
                        // Recursively fetch metadata of connected tables so we can use their alternate
                        // keys (if any)
                        @SuppressWarnings("unchecked")
                        List<Map<String, Object>> relationships = (List<Map<String, Object>>) tableMetadata
                                .get("ManyToOneRelationships");
                        for (var relationship : relationships) {
                            String referencedTable = (String) relationship.get("ReferencedEntity");
                            if (!metadataCache.containsLogicalName(referencedTable)) {
                                fetchAndCacheMetadata(referencedTable, false, true);
                            }
                        }
                    }
                }
            }
        }
    }

}
