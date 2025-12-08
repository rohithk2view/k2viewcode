package com.k2view.cdbms.usercode.common.mongodb;

import static com.k2view.cdbms.usercode.common.mongodb.MongoUtils.dataAsDoc;
import static com.k2view.cdbms.usercode.common.mongodb.MongoUtils.flattenDocument;
import static com.k2view.cdbms.usercode.common.mongodb.MongoUtils.getParentRowsFilter;
import static com.k2view.cdbms.usercode.common.mongodb.MongoUtils.normalizeMongoDocument;
import static com.k2view.cdbms.usercode.common.mongodb.MongoUtils.parseUserFilter;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.naming.OperationNotSupportedException;

import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;

import com.k2view.broadway.model.Data;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorAbstract;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorRead;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorSourceDbQuery;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorWrite;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.SupplierIterator;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.AbstractIoSession;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoSession;
import com.k2view.fabric.common.io.basic.IoSimpleResultSet;
import com.k2view.fabric.common.io.basic.IoSimpleRow;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.DeleteManyModel;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.InsertManyOptions;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateManyModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

public class MongoDbSession extends AbstractIoSession {

    private static final String IFACE_CONNECTION_METHOD = "connectionMethod";
    private static final String IFACE_CONNECTION_STRING = "connectionString";
    private static final String IFACE_HOST = "host";
    private static final String IFACE_USER = "user";
    private static final String IFACE_AUTH_DB = "authDb";
    private static final String IFACE_PASS = "password";
    private static final String IFACE_SSL_ENABLED = "sslEnabled";
    private static final String IFACE_RETRY_WRITES = "retryWrites";
    private static final String IFACE_RETRY_READS = "retryReads";
    private static final String IFACE_PORT = "port";
    private static final String IFACE_FETCH_SIZE = "fetchSize";
    private static final String IFACE_BULK_BATCH_SIZE = "batchSize";
    private static final String IFACE_CTLG_SAMPLE_SIZE = "sampleSize";
    private static final String IFACE_POOL_MIN_SIZE = "minPoolSize";
    private static final String IFACE_POOL_MAX_SIZE = "maxPoolSize";
    private static final String IFACE_EXCLUDE_CONFIG_DB = "excludeConfigDb";
    private static final String IFACE_BINARY_AS_BYTES = "binaryAsBytes";

    final int fetchSize;
    final String interfaceId;
    final int catalogSampleSize;
    final boolean excludeConfigDb;
    final boolean binaryAsBytes;
    MongoClient client;

    private final List<WriteModel<Document>> bulkOperations = new ArrayList<>();
    private final List<Document> insertManyDocuments = new ArrayList<>();
    private final int bulkBatchSize;
    private final MongoClientSettings clientSettings;

    private boolean inTrx;
    private boolean isBulkOrdered;
    private MongoCollection<Document> bulkCollection;

    public enum Operation {
        INSERT_ONE,
        INSERT_MANY,
        UPDATE_ONE,
        UPDATE_ONE_UPSERT,
        UPDATE_MANY,
        UPDATE_MANY_UPSERT,
        REPLACE_ONE,
        REPLACE_ONE_UPSERT,
        DELETE_ONE,
        DELETE_MANY,
        FIND,
        COUNT
    }

    public MongoDbSession(Map<String, Object> args) {
        String connectionStringTemplate = ParamConvertor.toString(args.get(IFACE_CONNECTION_STRING));
        String connectionMethod = ParamConvertor.toString(args.get(IFACE_CONNECTION_METHOD));
        if (connectionMethod.equals("connectionString")) {
            String connectionString = connectionStringTemplate
                    .replace("[USER]", (String) args.get(IFACE_USER))
                    .replace("[PASSWORD]", (String) args.get(IFACE_PASS))
                    .replace("[HOST]", (String) args.get(IFACE_HOST))
                    .replace("[PORT]", String.valueOf(args.get(IFACE_PORT)));

            this.clientSettings = MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(connectionString))
                    .build();
        } else {
            long minPoolSize = ParamConvertor.toInteger(args.get(IFACE_POOL_MIN_SIZE));
            long maxPoolSize = ParamConvertor.toInteger(args.get(IFACE_POOL_MAX_SIZE));
            MongoCredential credential = MongoCredential.createCredential(
                    (String) args.get(IFACE_USER),
                    (String) args.getOrDefault(IFACE_AUTH_DB, "admin"),
                    ((String) args.get(IFACE_PASS)).toCharArray());
            Builder clientsSettingsBuilder = MongoClientSettings.builder()
                    .applyToClusterSettings(builder -> builder.hosts(
                            List.of(
                                    new ServerAddress((String) args.get(IFACE_HOST),
                                            (int) ParamConvertor.toInteger(args.get(IFACE_PORT))))))
                    .credential(credential)
                    .retryWrites(ParamConvertor.toBool(args.get(IFACE_RETRY_WRITES)))
                    .retryReads(ParamConvertor.toBool(args.get(IFACE_RETRY_READS)))
                    .applyToSslSettings(builder -> builder
                            .enabled(ParamConvertor.toBool(args.get(IFACE_SSL_ENABLED)))
                            .invalidHostNameAllowed(false));
            if (minPoolSize >= 0) {
                clientsSettingsBuilder.applyToConnectionPoolSettings(c -> c.minSize((int) minPoolSize));
            }
            if (maxPoolSize >= 0) {
                clientsSettingsBuilder.applyToConnectionPoolSettings(c -> c.maxSize((int) maxPoolSize));
            }
            this.clientSettings = clientsSettingsBuilder.build();
        }

        this.client = MongoClients.create(clientSettings);
        this.fetchSize = (int) ParamConvertor.toInteger(args.get(IFACE_FETCH_SIZE));
        this.bulkBatchSize = (int) ParamConvertor.toInteger(args.get(IFACE_BULK_BATCH_SIZE));
        this.interfaceId = (String) args.get("interface");
        this.catalogSampleSize = (int) ParamConvertor.toInteger(args.get(IFACE_CTLG_SAMPLE_SIZE));
        if (bulkBatchSize <= 0) {
            throw new IllegalArgumentException("Bulk operations batch size must be greater than 1.");
        }
        this.excludeConfigDb = ParamConvertor.toBool(args.get(IFACE_EXCLUDE_CONFIG_DB));
        this.binaryAsBytes = ParamConvertor.toBool(args.get(IFACE_BINARY_AS_BYTES));
    }

    @Override
    public void close() throws Exception {
        client.close();
        this.client = null;
        this.bulkCollection = null;
        this.bulkOperations.clear();
        this.insertManyDocuments.clear();
        super.close();
    }

    @Override
    public void abort() throws Exception {
        // TO-DO handle abort
        super.abort();
    }

    @Override
    public void beginTransaction() throws Exception {
        this.inTrx = true;
    }

    @Override
    public void commit() throws Exception {
        flush();
        this.inTrx = false;
    }

    @Override
    public IoSessionCompartment compartment() {
        return IoSessionCompartment.NOT_SHARED;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getMetadata(Map<String, Object> params) throws Exception {
        return (T) new MongoDbMetadata(this, params);
    }

    @Override
    public Statement statement() throws Exception {
        return new MongoDbStatement();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends IoSession> T unwrap() {
        return (T) this;
    }

    @Override
    public void testConnection() {
        MongoDatabase db = client.getDatabase("admin");
        db.runCommand(new Document("ping", 1));
    }

    @Override
    public void rollback() throws Exception {
        this.inTrx = false;
        bulkOperations.clear();
        insertManyDocuments.clear();
    }

    @Override
    public String productName() {
        return "MongoDb";
    }

    @Override
    public boolean isTransactional() throws Exception {
        return true;
    }

    private void flush() {
        if (bulkOperations.size() > 0) {
            bulkCollection.bulkWrite(bulkOperations, new BulkWriteOptions().ordered(isBulkOrdered));
            bulkOperations.clear();
        } else if (insertManyDocuments.size() > 0) {
            bulkCollection.insertMany(insertManyDocuments, new InsertManyOptions().ordered(isBulkOrdered));
            insertManyDocuments.clear();
        }
    }

    private class MongoDbStatement implements IoCommand.Statement {

        private MongoDatabase database;
        private MongoCollection<Document> collection;

        @Override
        public Result execute(Object... args) throws Exception {
            if (client == null) {
                MongoDbSession.this.client = MongoClients.create(clientSettings);
            }
            if (inTrx) {
                batch(args);
                return IoSimpleResultSet.ONE_ROW_AFFECTED;
            }
            Data input = (Data) args[0];
            Operation op = Util.parseEnum(Operation.class, input.string(MongoDbActorAbstract.INPUT_OPERATION));
            if (database == null) {
                database = client.getDatabase(input.string(MongoDbActorAbstract.INPUT_DB));
                collection = database.getCollection(input.string(MongoDbActorAbstract.INPUT_COLLECTION));
            }

            Object data = input.get(MongoDbActorWrite.INPUT_DATA);

            Document filterDoc = filterAsDoc(input);

            int affectedRows = -1;

            Document parsedData = dataAsDoc(data);

            boolean isUpsert = op.name().contains("UPSERT");
            UpdateOptions updateOptions = new UpdateOptions().upsert(isUpsert);

            switch (op) {
                case INSERT_ONE -> {
                    collection.insertOne(parsedData);
                    affectedRows = 1;
                }
                case UPDATE_ONE, UPDATE_ONE_UPSERT -> {
                    affectedRows = (int) collection.updateOne(filterDoc, parsedData, updateOptions).getModifiedCount();
                }
                case UPDATE_MANY, UPDATE_MANY_UPSERT -> {
                    affectedRows = (int) collection.updateMany(filterDoc, parsedData, updateOptions).getModifiedCount();
                }
                case REPLACE_ONE, REPLACE_ONE_UPSERT -> {
                    ReplaceOptions replaceOptions = new ReplaceOptions().upsert(isUpsert);
                    affectedRows = (int) collection.replaceOne(filterDoc, parsedData, replaceOptions)
                            .getModifiedCount();
                }
                case DELETE_ONE -> {
                    affectedRows = (int) collection.deleteOne(filterDoc).getDeletedCount();
                }
                case DELETE_MANY -> {
                    affectedRows = (int) collection.deleteMany(filterDoc).getDeletedCount();
                }
                case FIND -> {
                    return new MongoFindResult(collection, filterDoc, input);
                }
                case INSERT_MANY -> throw new UnsupportedOperationException(
                        String.format("INSERT_MANY is supported only in transaction mode."));
                case COUNT -> {
                    List<Object[]> resultData = new ArrayList<>();
                    resultData.add(new Object[] { collection.countDocuments(filterDoc) });
                    return new IoSimpleResultSet(new String[] { "count" }, new Type[] { Long.class }, resultData);
                }
                default -> throw new IllegalArgumentException(String.format("Unsupported operation '%s'", op));
            }

            return new IoSimpleResultSet(affectedRows);
        }

        @SuppressWarnings("unchecked")
        private Iterable<Map<String, Object>> parentRowsIterable(Object parentRows) {
            if (parentRows instanceof Map) {
                return Collections.singletonList((Map<String, Object>) parentRows);
            } else {
                return (Iterable<Map<String, Object>>) parentRows;
            }
        }

        @Override
        public void batch(Object... params) throws Exception {
            if (!inTrx) {
                throw new IllegalStateException("Batch is only supported in transactions.");
            }
            Data input = (Data) params[0];
            Operation op = Util.parseEnum(Operation.class, input.string(MongoDbActorAbstract.INPUT_OPERATION));
            if (database == null) {
                database = client.getDatabase(input.string(MongoDbActorAbstract.INPUT_DB));
                collection = database.getCollection(input.string(MongoDbActorAbstract.INPUT_COLLECTION));
                MongoDbSession.this.bulkCollection = collection;
            }
            MongoDbSession.this.isBulkOrdered = input.bool(MongoDbActorWrite.INPUT_BULK_ORDERED);

            Object data = input.get(MongoDbActorWrite.INPUT_DATA);

            Document filterDoc = filterAsDoc(input);

            Document parsedData = dataAsDoc(data);
            boolean isUpsert = op.name().contains("UPSERT");
            UpdateOptions updateOptions = new UpdateOptions().upsert(isUpsert);

            if ((Operation.INSERT_MANY.equals(op) && bulkOperations.size() > 0)
                    || (!Operation.INSERT_MANY.equals(op) && insertManyDocuments.size() > 0)) {
                throw new OperationNotSupportedException(
                        "INSERT_MANY can't be mixed with other operations, use INSERT_ONE instead.");
            }

            switch (op) {
                case INSERT_ONE -> {
                    bulkOperations.add(new InsertOneModel<>(parsedData));
                }
                case INSERT_MANY -> {
                    insertManyDocuments.add(parsedData);
                    if (insertManyDocuments.size() > bulkBatchSize) {
                        flush();
                    }
                }
                case UPDATE_ONE, UPDATE_ONE_UPSERT -> {
                    bulkOperations.add(new UpdateOneModel<>(filterDoc, parsedData, updateOptions));
                }
                case UPDATE_MANY, UPDATE_MANY_UPSERT -> {
                    bulkOperations.add(new UpdateManyModel<>(filterDoc, parsedData, updateOptions));
                }
                case REPLACE_ONE, REPLACE_ONE_UPSERT -> {
                    ReplaceOptions replaceOptions = new ReplaceOptions().upsert(isUpsert);
                    bulkOperations.add(new ReplaceOneModel<Document>(filterDoc, parsedData, replaceOptions));
                }
                case DELETE_ONE -> {
                    bulkOperations.add(new DeleteOneModel<>(filterDoc));
                }
                case DELETE_MANY -> {
                    bulkOperations.add(new DeleteManyModel<>(filterDoc));
                }
                default -> {
                    throw new IllegalArgumentException(
                            String.format("Unsupported operation type '%s' in batch mode.", op));
                }
            }
            if (bulkOperations.size() >= bulkBatchSize) {
                flush();
            }
        }

        private Document filterAsDoc(Data input) {
            Document filterDoc;
            Object filter;
            if (input.get(MongoDbActorAbstract.INPUT_FILTER) instanceof Map<?, ?>) {
                filter = input.map(MongoDbActorAbstract.INPUT_FILTER);
            } else {
                filter = parseUserFilter(input);
            }
            filterDoc = dataAsDoc(filter);
            return filterDoc;
        }

        @Override
        public void close() throws Exception {
            flush();
            Statement.super.close();
            database = null;
            collection = null;
        }

        private class MongoFindResult implements Result {

            private final Iterator<Map<String, Object>> parentRowsIterator;
            private final MongoCollection<Document> collection;
            private final Document userFilter;
            private final Map<String, ?> projection;
            private final Map<String, ?> sort;
            private final int limit;
            private final int parentRowsFetchSize;
            private final boolean flatten;
            private int iteratorOffset = 0;
            private MongoCursor<Document> iterator;

            @SuppressWarnings("unchecked")
            public MongoFindResult(MongoCollection<Document> collection, Document filterDoc, Data input) {
                this.collection = collection;
                this.userFilter = filterDoc;
                this.projection = (Map<String, ?>) input.map(MongoDbActorRead.INPUT_PROJECTION);
                this.sort = (Map<String, ?>) input.map(MongoDbActorRead.INPUT_SORT);
                this.parentRowsFetchSize = (int) input.integer(MongoDbActorSourceDbQuery.INPUT_SIZE);
                this.limit = (int) input.integer(MongoDbActorRead.INPUT_LIMIT);
                this.parentRowsIterator = Optional.ofNullable(
                        parentRowsIterable(input.get(MongoDbActorSourceDbQuery.INPUT_PARENT_ROWS)))
                        .map(Iterable::iterator)
                        .orElse(Collections.emptyIterator());
                this.flatten = input.bool(MongoDbActorRead.INPUT_FLATTEN);
                fetch();
            }

            @Override
            public void close() throws Exception {
                iterator.close();
            }

            @Override
            public Iterator<Row> iterator() {
                return new SupplierIterator<>(this::next);
            }

            private Row next() {
                if (this.iterator.hasNext()) {
                    iteratorOffset++;
                    return currRow();
                }
                if (parentRowsIterator.hasNext()) {
                    iterator.close();
                    fetch();
                    return next();
                }
                return null;
            }

            private void fetch() {
                Document parentRowsFilter = getParentRowsFilter(parentRowsIterator, parentRowsFetchSize);
                boolean isUserFilterEmpty = Util.isEmpty(userFilter);
                boolean isParentFilterEmpty = Util.isEmpty(parentRowsFilter);
                Document combinedFilter;
                if (isUserFilterEmpty && isParentFilterEmpty) {
                    combinedFilter = new Document();
                } else if (isUserFilterEmpty) {
                    combinedFilter = parentRowsFilter;
                } else if (isParentFilterEmpty) {
                    combinedFilter = userFilter;
                } else {
                    combinedFilter = new Document("$and", Arrays.asList(userFilter, parentRowsFilter));
                }
                int effectiveLimit = Math.max(limit - iteratorOffset, 0);
                FindIterable<Document> result = collection.find(combinedFilter).projection(new Document(projection))
                        .sort(new Document(sort)).limit(effectiveLimit);
                if (fetchSize > 0) {
                    result.batchSize(fetchSize);
                }
                this.iterator = result.iterator();
            }

            private Row currRow() {
                Document next = iterator.next();
                normalizeMongoDocument(next, MongoDbSession.this.binaryAsBytes);

                if (flatten) {
                    flattenDocument(next);
                }

                return IoSimpleRow.factory(next.keySet()).apply(next.values().toArray());
            }

            // private Object parseMongoValue(Object value) {
            // if (value instanceof ObjectId o) {
            // return o;
            // } else if (value instanceof Decimal128 d) {
            // return d;
            // } else if (value instanceof Document doc) {
            // Document res = new Document();
            // doc.forEach((k, v) -> res.append(k, parseMongoValue(v)));
            // return res;
            // } else if (value instanceof List list) {
            // List<Object> newList = new ArrayList<>();
            // for (Object item : list) {
            // newList.add(parseMongoValue(item));
            // }
            // return newList;
            // }
            // return value;
            // }
        }
    }

    // private static Object parseToMongoValue(Object value) {
    // if (value instanceof Map<?,?> valueMap) {
    // if (valueMap.containsKey("$oid")) {
    // return new ObjectId(valueMap.get("$oid").toString());
    // } else if (valueMap.containsKey("$date")) {
    // Map<?, ?> dateMap = (Map<?, ?>) valueMap.get("$date");
    // if (dateMap.containsKey("$numberLong")) {
    // return
    // Date.from(Instant.ofEpochMilli(Long.parseLong(dateMap.get("$numberLong").toString())));
    // }
    // } else if (valueMap.containsKey("$numberInt")) {
    // return Integer.parseInt(valueMap.get("$numberInt").toString());
    // } else if (valueMap.containsKey("$numberLong")) {
    // return Long.parseLong(valueMap.get("$numberLong").toString());
    // } else if (valueMap.containsKey("$numberDouble")) {
    // return Double.parseDouble(valueMap.get("$numberDouble").toString());
    // } else if (valueMap.containsKey("$numberDecimal")) {
    // return new Decimal128(new
    // BigDecimal(valueMap.get("$numberDecimal").toString())); // Convert to
    // Decimal128
    // } else if (valueMap.containsKey("$binary")) {
    // Map<?, ?> binaryMap = (Map<?, ?>) valueMap.get("$binary");
    // String base64Data = binaryMap.get("base64").toString();
    // byte[] decodedBytes = Base64.getDecoder().decode(base64Data);
    // return new Binary(decodedBytes); // Convert to Binary
    // } else if (valueMap.containsKey("$regex")) {
    // String pattern = valueMap.get("$regex").toString();
    // String options = valueMap.containsKey("$options") ?
    // valueMap.get("$options").toString() : "";
    // return Pattern.compile(pattern, getRegexOptions(options)); // Convert to
    // Pattern (Regular Expression)
    // } else if (valueMap.containsKey("$timestamp")) {
    // Map<?, ?> timestampMap = (Map<?, ?>) valueMap.get("$timestamp");
    // int t = Integer.parseInt(timestampMap.get("t").toString());
    // int i = Integer.parseInt(timestampMap.get("i").toString());
    // return new BsonTimestamp(t, i); // Convert to BsonTimestamp
    // } else if (valueMap.containsKey("$minKey")) {
    // return new MinKey();
    // } else if (valueMap.containsKey("$maxKey")) {
    // return new MaxKey();
    // } else {
    // Document doc = new Document();
    // for (Map.Entry<?, ?> entry : valueMap.entrySet()) {
    // doc.append((String) entry.getKey(), parseToMongoValue(entry.getValue()));
    // }
    // return doc;
    // }
    // } else if (value instanceof List<?> list) {
    // List<Object> newList = new ArrayList<>();
    // for (Object item : list) {
    // newList.add(parseToMongoValue(item)); // Recursively parse lists
    // }
    // return newList;
    // }
    // return value;
    // }

    // private static int getRegexOptions(String options) {
    // int flags = 0;
    // if (options.contains("i")) flags |= Pattern.CASE_INSENSITIVE;
    // if (options.contains("m")) flags |= Pattern.MULTILINE;
    // if (options.contains("x")) flags |= Pattern.COMMENTS;
    // if (options.contains("s")) flags |= Pattern.DOTALL;
    // return flags;
    // }

}
