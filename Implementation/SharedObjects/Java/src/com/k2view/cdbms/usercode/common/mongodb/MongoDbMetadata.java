package com.k2view.cdbms.usercode.common.mongodb;

import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import com.k2view.broadway.metadata.Any;
import com.k2view.broadway.metadata.ObjectType;
import com.k2view.broadway.metadata.Properties;
import com.k2view.broadway.metadata.Schema;
import com.k2view.broadway.metadata.Type;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.usercode.common.mongodb.DatasetFieldsBuilder.SchemaPropertyContext;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorAbstract;
import com.k2view.cdbms.usercode.common.mongodb.actors.MongoDbActorRead;
import com.k2view.discovery.MonitorStatusUpdater;
import com.k2view.discovery.rules.CrawlerRules;
import com.k2view.discovery.rules.DataPlatformMetaDataInfo;
import com.k2view.discovery.schema.io.CrawlerAbortedException;
import com.k2view.discovery.schema.io.IoMetadata;
import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.model.Category;
import com.k2view.discovery.schema.model.DataPlatform;
import com.k2view.discovery.schema.model.impl.ConcreteClassNode;
import com.k2view.discovery.schema.model.impl.ConcreteDataPlatform;
import com.k2view.discovery.schema.model.impl.ConcreteDataset;
import com.k2view.discovery.schema.model.impl.ConcreteField;
import com.k2view.discovery.schema.model.impl.ConcreteNode;
import com.k2view.discovery.schema.model.impl.ConcreteSchemaNode;
import com.k2view.discovery.schema.model.types.UnknownClass;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.IteratorTranslate;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.fabric.common.io.IoCommand.Row;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class MongoDbMetadata implements IoMetadata {

    private static final String PARAM_UUID = "uuid";
    private static final String STATUS_CRAWLER = "crawler";
    private static final String DATA_PLATFORM = "dataPlatform";
    private static final String SCHEMA = "schema";
    private static final String CLASS = "class";
    private static final String ENTITY_NAME = "entityName";
    private static final String CRAWLER = "Crawler";

    private final String jobUid;
    private final MongoDbSession session;
    private final AtomicInteger fieldsCnt = new AtomicInteger(0);
    private final Log log = Log.a(this.getClass());
    private final Map<String, List<String>> tablesExclude = new HashMap<>();
    private final Map<String, List<String>> tablesInclude = new HashMap<>();
    private final DataPlatformMetaDataInfo dataPlatformMetaDataInfo;
    private Set<String> schemasExclude = new HashSet<>();
    private Set<String> schemasInclude = new HashSet<>();
    private boolean aborted;

    public MongoDbMetadata(MongoDbSession mongoDbSession, Map<String, Object> params) {
        this.session = mongoDbSession;
        this.jobUid = (String) params.get(PARAM_UUID);
        this.dataPlatformMetaDataInfo = ((CrawlerRules) params.get("rules")).getMetaData(session.interfaceId);
        if (this.dataPlatformMetaDataInfo == null) {
            return;
        }
        setIncludeSchema();
    }

    @SuppressWarnings("deprecation")
    private void setIncludeSchema() {
        Set<String> schemaSet = this.dataPlatformMetaDataInfo.getSchemaMetadata().getSet();
        if (!Util.isEmpty(schemaSet) && this.dataPlatformMetaDataInfo.getSchemaMetadata().isIncludeOrExcludeList) {
            schemasInclude = schemaSet;
            populateTableLists(schemasInclude);
        } else {
            schemasExclude = schemaSet;
        }
        populateTableLists(this.dataPlatformMetaDataInfo.getTableSetPerSchema());
    }

    private void populateTableLists(Set<String> schemas) {
        if (schemas.isEmpty()) {
            handleSchema("");
        } else {
            for (String schema : schemas) {
                handleSchema(schema);
            }
        }
    }

    private void handleSchema(String schema) {
        DataPlatformMetaDataInfo.MetaDataListInfo tableListInfo = this.dataPlatformMetaDataInfo
                .getTableListPerSchema(schema);
        if (tableListInfo != null) {
            if (tableListInfo.isIncludeOrExcludeList) {
                tablesInclude.put(schema, new ArrayList<>(tableListInfo.getSet()));
            } else {
                tablesExclude.put(schema, new ArrayList<>(tableListInfo.getSet()));
            }
        }
    }

    @Override
    public DataPlatform getDataPlatform() throws Exception {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ConcreteDataPlatform dataPlatform = new ConcreteDataPlatform(session.interfaceId, 1.0, CRAWLER, "",
                "Data platform", session.interfaceId);
        MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid, 0);
        MonitorStatusUpdater.getInstance().registerDuration(STATUS_CRAWLER, jobUid);
        MonitorStatusUpdater.getInstance().updateProgress(STATUS_CRAWLER, jobUid, 0);
        MongoClient client = session.client;
        String platformIdPrefix = this.idPrefix(DATA_PLATFORM, dataPlatform);
        dataPlatform.addProperty(platformIdPrefix, ENTITY_NAME, "Data Platform Name", dataPlatform.getName(), 1.0,
                CRAWLER, "");
        dataPlatform.addProperty(platformIdPrefix, "type", "Data Platform Type", "MongoDB", 1.0, CRAWLER, "");
        for (String dbName : client.listDatabaseNames()) {
            assertAborted();
            if (("config".equals(dbName) && session.excludeConfigDb) || schemasExclude.contains(dbName)
                    || (!schemasInclude.isEmpty() && !schemasInclude.contains(dbName))) {
                continue;
            }
            ConcreteSchemaNode schemaNode = new ConcreteSchemaNode(dbName, 1.0, CRAWLER, "", "Database", dbName);
            String schemaIdPrefix = this.idPrefix(SCHEMA, schemaNode);
            schemaNode.addProperty(schemaIdPrefix, ENTITY_NAME, "MongoDB Database", schemaNode.getName(), 1.0, CRAWLER,
                    "");
            MongoDatabase database = client.getDatabase(dbName);

            for (String collectionName : database.listCollectionNames()) {
                assertAborted();
                if ((tablesExclude.containsKey(dbName) && tablesExclude.get(dbName).contains(collectionName)) ||
                        (tablesInclude.containsKey(dbName) && !tablesInclude.get(dbName).isEmpty()
                                && !tablesInclude.get(dbName).contains(collectionName))) {
                    continue;
                }
                ConcreteDataset datasetNode = new ConcreteDataset(collectionName, 1.0, CRAWLER, "", "Collection",
                        collectionName);
                ConcreteClassNode datasetClassNode = new ConcreteClassNode(collectionName, 1.0, CRAWLER, "",
                        "Collection", collectionName);
                String datasetIdPrefix = this.idPrefix(CLASS, datasetClassNode);
                datasetClassNode.addProperty(datasetIdPrefix, ENTITY_NAME, "Collection", collectionName, 1.0, CRAWLER,
                        "");
                datasetNode.addProperty(datasetIdPrefix, ENTITY_NAME, "Collection", collectionName, 1.0, CRAWLER, "");
                datasetNode.definedBy(datasetClassNode, 1.0, CRAWLER, "");
                schemaNode.contains(datasetNode, 1.0, CRAWLER, "");

                MongoCollection<Document> collection = database.getCollection(collectionName);
                List<String> indexes = StreamSupport.stream(collection.listIndexes().spliterator(), false)
                        .map(doc -> doc.toJson()).toList();
                datasetClassNode.addProperty(datasetIdPrefix, "Namespace", "", collection.getNamespace().getFullName(),
                        1.0, CRAWLER, "");
                datasetClassNode.addProperty(datasetIdPrefix, "Indexes", "", indexes, 1.0, CRAWLER, "");

                assertAborted();
                try (var statement = session.statement();
                        var result = statement.execute(Data.from(
                                MongoDbActorAbstract.INPUT_DB, dbName,
                                MongoDbActorAbstract.INPUT_COLLECTION, collectionName,
                                MongoDbActorAbstract.INPUT_OPERATION, MongoDbSession.Operation.FIND,
                                MongoDbActorRead.INPUT_LIMIT, session.catalogSampleSize))) {
                    Schema docSchema = SchemaCombiner.combineSchemas(result);
                    if (docSchema == null) {
                        log.warn("Couldn't infer schema of {} - no documents found.",
                                collection.getNamespace().getFullName());
                        continue;
                    }
                    Properties properties = docSchema.properties();
                    int fieldsSize = properties.keySet().size();
                    this.fieldsCnt.addAndGet(fieldsSize);
                    MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid, fieldsSize);
                    DatasetFieldsBuilder.fromObjectSchema(datasetClassNode, (ObjectType) docSchema,
                            schemaContextConsumer());
                    scheduler.schedule(
                            () -> MonitorStatusUpdater.getInstance().updateProgress(STATUS_CRAWLER, jobUid, fieldsSize),
                            500, TimeUnit.MILLISECONDS);
                }
            }
            dataPlatform.contains(schemaNode, 1.0, CRAWLER, "");
        }
        assertAborted();
        scheduler.shutdown();
        return dataPlatform;
    }

    private Consumer<SchemaPropertyContext> schemaContextConsumer() {
        return context -> {
            ConcreteField field = context.field();
            String idPrefix = context.idPrefix();
            Schema schema = context.schema();
            boolean isId = context.isTopLevel() && context.field().getName().equals("_id");

            if (isId) {
                field.addProperty(idPrefix, "pk", "Primary Key", true, 1.0, "Crawler", "");
            }
            field.addProperty(idPrefix, Category.sourceNullable.name(), "Nullability of the field 1 or 0", "TRUE", 1.0,
                    CRAWLER, "");
            field.addProperty(idPrefix, Category.sqlDataType.name(), "Sql Column type", getSqlType(schema), 1.0,
                    CRAWLER, "");
            field.addProperty(idPrefix, Category.ordinalPosition.name(), "Ordinal position", context.ordinalPosition(),
                    1.0, CRAWLER, "");
            // TO-DO column size shouldn't be mandatory
            // field.addProperty(idPrefix, Category.columnSize.name(), "Column size", 0,
            // 1.0, CRAWLER, "");

            if (context.isTopLevel()) {
                field.addProperty(idPrefix, Category.sourceEntityType.name(), "Role", "Column", 1.0, CRAWLER, "");
                // TO-DO find a way to get actual source data type
                field.addProperty(idPrefix, Category.sourceDataType.name(), "Source Data Type",
                        isId || schema.type() == null ? "any" : schema.type(), 1.0, CRAWLER, "");
            }

            if (schema.equals(Any.ANY)) {
                field.addProperty(idPrefix, Category.definedBy.name(), "Data type for field",
                        UnknownClass.UNKNOWN.getClassName(), 1.0, CRAWLER, "");
            } else if (schema.type().isPrimitive()) {
                field.addProperty(idPrefix, Category.definedBy.name(), "Data type for field", isId
                        ? UnknownClass.UNKNOWN.getName().toUpperCase()
                        : DatasetFieldsBuilder.SCHEMA_TO_CTLG_TYPE_MAPPING.get(schema.type()).getName().toUpperCase(),
                        1.0, CRAWLER, "");
            }
        };
    }

    private static int getSqlType(Schema innerFieldSchema) {
        if (innerFieldSchema.equals(Any.ANY)) {
            return Types.VARCHAR;
        }
        switch (innerFieldSchema.type()) {
            case object:
                return Types.VARCHAR;
            case array:
                return Types.VARCHAR;
            case string:
                return Types.VARCHAR;
            case integer:
                return Types.INTEGER;
            case real:
                return Types.DOUBLE;
            case date:
                return Types.TIMESTAMP;
            case bool:
                return Types.BOOLEAN;
            case blob:
                return Types.BLOB;
            default:
                throw new IllegalArgumentException("Unsupported Type: " + innerFieldSchema);
        }
    }

    private void assertAborted() {
        if (aborted) {
            throw new CrawlerAbortedException(
                    String.format("Crawler for dataPlatform '%s' is aborted", session.interfaceId));
        }
    }

    private String idPrefix(String prefix, ConcreteNode node) {
        return prefix + ":" + node.getId();
    }

    @Override
    public SnapshotDataset snapshotDataset(String dataset, String schema, SampleSize size, Map<String, Object> args)
            throws Exception {
        return new MongoDbSnapshot(dataset, schema, size);
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void abort() throws Exception {
        this.aborted = true;
    }

    private class MongoDbSnapshot implements SnapshotDataset {

        private final SampleSize size;
        private final MongoCollection<Document> collection;
        private final String dbName;
        private final String collectionName;
        private Result result;

        public MongoDbSnapshot(String collection, String database, SampleSize size) {
            this.size = size;
            this.collection = session.client.getDatabase(database).getCollection(collection);
            this.dbName = database;
            this.collectionName = collection;
        }

        @Override
        public void close() throws Exception {
            Util.safeClose(result);
        }

        @Override
        public Iterator<Map<String, Object>> fetch() throws Exception {
            int limit = getLimit();
            Util.safeClose(result);
            try (var statement = session.statement()) {
                this.result = statement.execute(Data.from(
                        MongoDbActorAbstract.INPUT_DB, dbName,
                        MongoDbActorAbstract.INPUT_COLLECTION, collectionName,
                        MongoDbActorAbstract.INPUT_OPERATION, MongoDbSession.Operation.FIND,
                        MongoDbActorRead.INPUT_LIMIT, limit));
                return new IteratorTranslate<>(result.iterator(), this::replaceDocumentWithMap);
            }
        }

        private Object convertBsonValue(Object value) {
            if (value instanceof Document doc) {
                return replaceDocumentWithMap(doc);
            } else if (value instanceof List l) {
                List<Object> list = new ArrayList<>();
                for (Object item : l) {
                    list.add(convertBsonValue(item));
                }
                return list;
            } else if (value instanceof Decimal128 dec) {
                if (dec.isFinite()) {
                    return dec.doubleValue();
                }
                return null;
            } else if (value instanceof Binary b) {
                return b.getData();
            } else if (value instanceof BsonTimestamp t) {
                return new Timestamp(t.getTime() * 1000L);
            } else if (value instanceof ObjectId oid) {
                return oid.toString();
            } else {
                return value;
            }
        }
    
        private Map<String, Object> replaceDocumentWithMap(Map<String, Object> map) {
            Map<String, Object> newMap = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                newMap.put(entry.getKey(), convertBsonValue(entry.getValue()));
            }
            return newMap;
        }

        private int getLimit() throws Exception {
            int limit;
            int count = (int) getNumberOfItems();
            int countPercentage = Math.toIntExact(count * size.getPercentage() / 100);
            if (countPercentage < size.getMin()) {
                limit = Math.toIntExact(size.getMin());
            } else if (countPercentage >= size.getMax()) {
                limit = Math.toIntExact(size.getMax());
            } else {
                limit = countPercentage;
            }
            return limit;
        }

        private long getNumberOfItems() throws Exception {
            return collection.estimatedDocumentCount();
        }

    }

    // @Override in 8.3
    public SnapshotDataset snapshotDataset(String arg0, String arg1, String arg2, SampleSize arg3,
            Map<String, Object> arg4) throws Exception {
        throw new UnsupportedOperationException("Unimplemented method 'snapshotDataset'");
    }
}