package com.k2view.cdbms.usercode.common.dataverse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.management.RuntimeErrorException;

import com.k2view.broadway.util.InputStreamIterator;
import com.k2view.cdbms.shared.Utils;
import com.k2view.cdbms.usercode.common.dataverse.SharedLogic.DataverseAttributeType;
import com.k2view.discovery.MonitorStatusUpdater;
import com.k2view.discovery.rules.CrawlerRules;
import com.k2view.discovery.rules.DataPlatformMetaDataInfo;
import com.k2view.discovery.schema.io.CrawlerAbortedException;
import com.k2view.discovery.schema.io.IoMetadata;
import com.k2view.discovery.schema.io.SnapshotDataset;
import com.k2view.discovery.schema.model.Category;
import com.k2view.discovery.schema.model.ClassNode;
import com.k2view.discovery.schema.model.Contains;
import com.k2view.discovery.schema.model.DataPlatform;
import com.k2view.discovery.schema.model.Dataset;
import com.k2view.discovery.schema.model.Property;
import com.k2view.discovery.schema.model.Schema;
import com.k2view.discovery.schema.model.impl.ConcreteClassNode;
import com.k2view.discovery.schema.model.impl.ConcreteDataPlatform;
import com.k2view.discovery.schema.model.impl.ConcreteDataset;
import com.k2view.discovery.schema.model.impl.ConcreteField;
import com.k2view.discovery.schema.model.impl.ConcreteNode;
import com.k2view.discovery.schema.model.impl.ConcreteRefersToRelation.FkCategory;
import com.k2view.discovery.schema.model.impl.ConcreteSchemaNode;
import com.k2view.discovery.schema.model.impl.PrimitiveClass;
import com.k2view.discovery.schema.model.impl.PropertyImpl;
import com.k2view.discovery.schema.utils.SampleSize;
import com.k2view.fabric.common.ByteStream;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.Log;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.fabric.common.io.IoCommand.Row;
import com.k2view.fabric.common.io.IoCommand.Statement;
import com.k2view.fabric.common.io.basic.exception.HttpException;

// TO-DO identify alternate keys

public class DataverseMetadata implements IoMetadata {
    // private static final String RANDOM_MULTI_PICKLIST_ACTOR =
    // "DataverseRandomMultiPicklist.actor";
    private static final String MASKING_SETUP = "catalog_classification_generators";
    private static final String DUMMY_SOLUTION = "_no_solution";

    enum EnumType {
        PICKLIST, MULTISELECT, STATE, STATUS
    }

    private static final String TABLES_API_PATH = "EntityDefinitions";
    private static final String SOLUTIONS_API_PATH = "solutions";
    private static final String SOLUTION_COMPONENTS_API_PATH = "solutioncomponents";
    // private static final String ATTRIBUTE_METADATA_API_PATH =
    // "EntityDefinitions(LogicalName='%s')/Attributes/%s";
    private static final String TABLE_COUNT_API_PATH = "%s/$count";
    private static final String TABLE_DATA_API_PATH = "%s";

    private static final String STATUS_CRAWLER = "crawler";
    private static final String TYPE = "type";
    private static final String FIELD = "field";
    private static final String DATA_PLATFORM = "dataPlatform";
    private static final String SCHEMA = "schema";
    private static final String DATASET = "dataset";
    private static final String CLASS = "class";
    private static final String ENTITY_NAME = "entityName";
    private static final String CRAWLER = "Crawler";

    private final DataverseIoSession session;
    private final String jobUid;
    private final DataPlatformMetaDataInfo dataPlatformMetaDataInfo;
    private final Map<String, List<String>> tablesExclude = new HashMap<>();
    private final Map<String, List<String>> tablesInclude = new HashMap<>();
    private final Map<String, List<Map<String, Object>>> manyToOneRelationships = new HashMap<>();
    private final Map<String, Map<String, Object>> tableMetadataByName = new HashMap<>();
    private final Map<String, String> tableNameById = new HashMap<>();
    private final Log log = Log.a(this.getClass());
    private final String interfaceName;

    private volatile boolean aborted;
    private Set<String> schemasExclude = new HashSet<>();
    private Set<String> schemasInclude = new HashSet<>();
    private final AtomicInteger fieldsCnt = new AtomicInteger(0);
    private boolean isSolutionGroupingMode;
    private int tableCnt;

    public DataverseMetadata(DataverseIoSession dataverseIoSession, Map<String, Object> params,
            String interfaceName) {
        this.session = dataverseIoSession;
        this.jobUid = ParamConvertor.toString(params.get("uuid"));
        this.interfaceName = interfaceName;
        this.isSolutionGroupingMode = session.discoveryMode.equals("GROUP_BY_SOLUTIONS");

        this.dataPlatformMetaDataInfo = ((CrawlerRules) params.get("rules")).getMetaData(interfaceName);
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
    public void close() throws Exception {
        this.schemasExclude.clear();
        this.schemasInclude.clear();
        this.tablesExclude.clear();
        this.tablesInclude.clear();
        this.manyToOneRelationships.clear();
        this.tableMetadataByName.clear();
        this.tableNameById.clear();
    }

    @Override
    public DataPlatform getDataPlatform() throws Exception {
        // if (schemasInclude.isEmpty() && schemasExclude.isEmpty()
        // && tablesInclude.values().stream().allMatch(l -> Util.isEmpty(l))
        // && tablesExclude.values().stream().allMatch(l -> Util.isEmpty(l))) {
        // throw new IllegalArgumentException(
        // "Considering the large amount of Dataverse tables, fetching the metadata for
        // all of the tables is not supported. Please exclude/include specific tables or
        // solutions via a crawler rule under Catalog Settings > Discovery Pipeline.");
        // }
        MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid, 0);
        MonitorStatusUpdater.getInstance().registerDuration(STATUS_CRAWLER, jobUid);
        MonitorStatusUpdater.getInstance().updateProgress(STATUS_CRAWLER, jobUid, 0);
        ConcreteDataPlatform dataPlatform = new ConcreteDataPlatform(interfaceName, 1.0, CRAWLER, "",
                "Data platform", interfaceName);
        String platformIdPrefix = this.idPrefix(DATA_PLATFORM, dataPlatform);
        dataPlatform.addProperty(platformIdPrefix, ENTITY_NAME, "Data Platform Name", dataPlatform.getName(), 1.0, "",
                CRAWLER);
        dataPlatform.addProperty(platformIdPrefix, TYPE, "Data Platform Type", "Dataverse", 1.0, "", CRAWLER);

        assertAborted();

        if (isSolutionGroupingMode) {
            log("Running discovery in solution-grouping mode..");
            handleSolutions(dataPlatform);
        } else {
            log("Running discovery in all tables under one solution mode..");
            handleTablesPartition(createSchemaNode(dataPlatform, "main"), Set.of());
        }

        if (session.addForeignKeys) {
            addForeignKeys(dataPlatform);
        }

        MonitorStatusUpdater.getInstance().updateTotal(STATUS_CRAWLER, jobUid, fieldsCnt.get());
        // MonitorStatusUpdater.getInstance().updateDuration(STATUS_CRAWLER, jobUid);
        log("Found and processed a total of {} tables.", tableCnt);
        return dataPlatform;
    }

    private void handleSolutions(ConcreteDataPlatform dataPlatform) throws Exception {
        try (Statement s = session.statement()) {
            StringBuilder filters = new StringBuilder(session.catalogSchemasFilter);

            this.modifyFilters(filters,
                    Stream.concat(schemasInclude.stream(),
                            tablesInclude.keySet().stream().filter(k -> !k.equals("")))
                            .filter(k -> !k.equals(DUMMY_SOLUTION))
                            .collect(Collectors.toSet()),
                    "uniquename", "eq", "or");
            this.modifyFilters(filters, schemasExclude, "uniquename", "ne", "and");

            Map<String, Object> params = Util.map(
                    "$select", "uniquename,friendlyname,description");
            if (!filters.isEmpty()) {
                params.put("$filter", filters.toString());
            }
            Map<String, Object> headers = Util.map(
                    "OData-Version", "4.0",
                    "OData-MaxVersion", "4.0"
            // "Prefer", "odata.maxpagesize=100"
            );

            try (Result res = executeWithRetry(() -> Util.rte(() -> s
                    .execute(Util.map("path", SOLUTIONS_API_PATH, "params", params, "headers",
                            headers, "debug", session.debug))))) {

                for (Row row : res) {
                    String solutionName = (String) row.get("uniquename");
                    String solutionId = (String) row.get("solutionid");
                    ConcreteSchemaNode schemaNode = createSchemaNode(dataPlatform, solutionName);
                    handleSolution(schemaNode, solutionId);
                }
            }
        }
    }

    private void handleSolution(ConcreteSchemaNode schemaNode, String solutionId) throws Exception {
        log("Processing solution {}", schemaNode.getName());
        Set<String> tablesIds = new HashSet<>();
        try (Statement s = session.statement()) {
            // componenttype = 1 means component is a table
            StringBuilder filters = new StringBuilder("componenttype eq 1 and _solutionid_value eq ")
                    .append(solutionId);

            Map<String, Object> params = Util.map(
                    "$filter", filters.toString(),
                    "$select", "objectid");

            Map<String, Object> headers = Util.map(
                    "OData-Version", "4.0",
                    "OData-MaxVersion", "4.0");

            try (Result res = executeWithRetry(() -> Util.rte(() -> s
                    .execute(Util.map("path", SOLUTION_COMPONENTS_API_PATH, "params", params, "headers",
                            headers, "debug", session.debug))))) {

                for (Row row : res) {
                    // objectid is table (i.e., solution component) id
                    String objectid = (String) row.get("objectid");
                    tablesIds.add(objectid);
                }
            }
        }
        log("Found {} tables for solution '{}'.", tablesIds.size(), schemaNode.getName());
        handleTables(schemaNode, tablesIds);
    }

    private void handleTables(ConcreteSchemaNode schemaNode, Set<String> tablesIds) throws Exception {
        List<Set<String>> partitions = partition(tablesIds, 100);
        for (var partition : partitions) {
            handleTablesPartition(schemaNode, partition);
        }
    }

    private void handleTablesPartition(ConcreteSchemaNode schemaNode, Set<String> partition) throws Exception {
        StringBuilder filters = new StringBuilder(session.catalogTablesFilter);
        modifyTablesFilter(schemaNode.getName(), filters, partition);
        Map<String, Object> params = Util.map(
                "LabelLanguages", 1033,
                "$select",
                "LogicalName,EntitySetName,DisplayName,Description,IsCustomEntity,TableType,IsSolutionAware,PrimaryIdAttribute");

        if (!filters.isEmpty()) {
            params.put("$filter", filters.toString());
        }

        try (Statement s = session.statement()) {
            try (Result res = executeWithRetry(() -> Util.rte(() -> s
                    .execute(Util.map("path", TABLES_API_PATH, "params", params, "debug", session.debug))))) {
                for (Row tableInfo : res) {

                    String tableName = (String) tableInfo.get("EntitySetName");
                    String tableLogicalName = (String) tableInfo.get("LogicalName");
                    String tableId = (String) tableInfo.get("MetadataId");
                    tableMetadataByName.put(tableLogicalName, tableInfo);
                    tableNameById.put(tableId, tableName);

                    Row fieldsAndRelations = fetchFieldsAndRelations(tableLogicalName);
                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> manyToOne = (List<Map<String, Object>>) fieldsAndRelations
                            .get("ManyToOneRelationships");
                    manyToOneRelationships.put(tableName, manyToOne);

                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> attributesInfo = (List<Map<String, Object>>) fieldsAndRelations
                            .get("Attributes");
                    processTable(schemaNode, tableMetadataByName.get(tableLogicalName), attributesInfo);
                }
            }
        }
    }

    // private void writeToOptionSetsMtable(String schema, String table,
    // List<Map<String,String>> data) throws IOException {
    // String projectFolder =
    // Utils.projectDir().orElseThrow(IllegalStateException::new);
    // Path dir = Path.of(projectFolder, "Implementation", "LogicalUnits", "k2_ref",
    // "Mtable");
    // File csv = dir.resolve(String.format("catalog_field_option_set___%s_%s.csv",
    // schema, table)).toFile();
    // writeToCSV(data, csv);
    // }

    // private void createGeneratorsMTable(String schema, String table,
    // Iterable<Map<String, String>> enums) throws IOException {
    // Iterator<Map<String, String>> iterator = enums.iterator();
    // if (iterator.hasNext()) {
    // Map<String, String> generatorRow = new LinkedHashMap<>();
    // generatorRow.put("classification", null);
    // generatorRow.put("generator", null);
    // generatorRow.put("params", "{}");
    // generatorRow.put("consistent", "false");
    // generatorRow.put("unique", "false");
    // generatorRow.put("seed", "false");
    // generatorRow.put("useEnvironment", "false");
    // generatorRow.put("useExecutionId", "false");
    // generatorRow.put("useInstanceId", "true");
    // generatorRow.put("onEmpty", "MASK_NO_CACHE");
    // generatorRow.put("formatterName", "");
    // generatorRow.put("formatterParams", "{}");
    // generatorRow.put("category", "enable_masking");
    // generatorRow.put("preExecution", "");
    // generatorRow.put("preExecutionParams", "{}");

    // Set<Map<String, String>> generatorsData = new LinkedHashSet<>();
    // while (iterator.hasNext()) {
    // Map<String,String> enumInfo = iterator.next();
    // Map<String, String> map = new LinkedHashMap<>(generatorRow);
    // String enumName = enumInfo.get("enum_type");
    // boolean multiPicklist = Boolean.parseBoolean(enumInfo.get("multipicklist"));
    // map.put("classification", enumName);
    // map.put("generator", multiPicklist ? RANDOM_MULTI_PICKLIST_ACTOR :
    // "MTableRandom.actor");
    // map.put("params", Json.get().toJson(Map.of("enum_type", enumName, "mtable",
    // "catalog_field_option_set")));
    // generatorsData.add(map);
    // }

    // String projectFolder =
    // Utils.projectDir().orElseThrow(IllegalStateException::new);
    // Path dir = Path.of(projectFolder, "Implementation", "SharedObjects",
    // "Interfaces", "Discovery", "Mtable");
    // File csv = dir.resolve(String.format("%s___dataverse_%s_%s.csv",
    // MASKING_SETUP, schema, table)).toFile();
    // writeToCSV(new ArrayList<>(generatorsData), csv);
    // }
    // }

    private void modifyTablesFilter(String solution, StringBuilder filters, Set<String> partition) {
        Set<String> filteredTables = new HashSet<>();
        Set<String> filteredOutTables = new HashSet<>();
        this.modifyFilters(filters, partition, "MetadataId", "eq", "or");

        if (tablesInclude.containsKey("")) {
            // table should be included in all solutions
            filteredTables.addAll(tablesInclude.get(""));
        }
        if (isSolutionGroupingMode && tablesInclude.containsKey(solution)) {
            filteredTables.addAll(tablesInclude.get(solution));
        }

        this.modifyFilters(filters,
                filteredTables,
                "EntitySetName", "eq", "or");

        if (tablesExclude.containsKey("")) {
            // table should be excluded in all solutions
            filteredOutTables.addAll(tablesExclude.get(""));
        }

        if (isSolutionGroupingMode && tablesExclude.containsKey(solution)) {
            filteredOutTables.addAll(tablesExclude.get(solution));
        }

        this.modifyFilters(filters,
                filteredOutTables,
                "EntitySetName", "ne", "and");
    }

    private Row fetchFieldsAndRelations(String tableLogicalName) throws Exception {
        try (Statement s = session.statement()) {
            String expandAttributes = "Attributes" + (Util.isEmpty(session.catalogAttributesFilter) ? ""
                    : String.format("($filter=%s)", session.catalogAttributesFilter));
            String select = "Attributes" + (session.addForeignKeys ? ",ManyToOneRelationships" : "");
            String expand = expandAttributes + (session.addForeignKeys
                    ? ",ManyToOneRelationships($select=ReferencingEntity,ReferencingAttribute,ReferencedEntity,ReferencedAttribute)"
                    : "");
            Map<String, Object> params = Util.map(
                    "LabelLanguages", 1033,
                    "$filter",
                    String.format("LogicalName eq '%s'", tableLogicalName),
                    "$select",
                    select,
                    "$expand",
                    expand);

            try (Result res = executeWithRetry(() -> Util.rte(() -> s
                    .execute(Util.map("path", TABLES_API_PATH, "params", params, "debug", session.debug))))) {
                return res.firstRow();
            }
        }
    }

    private void processTable(ConcreteSchemaNode schemaNode, Map<String, Object> tableInfo,
            List<Map<String, Object>> attributesInfo) throws Exception {
        this.tableCnt++;
        String tableName = (String) tableInfo.get("EntitySetName");
        ConcreteClassNode datasetClassNode = new ConcreteClassNode(tableName, 1.0, "",
                CRAWLER, "Class", tableName);
        String datasetIdPrefix = this.idPrefix(CLASS, datasetClassNode);

        datasetClassNode.addProperty(datasetIdPrefix, ENTITY_NAME,
                "", tableName, 1.0, CRAWLER, "");
        datasetClassNode.addProperty(datasetIdPrefix, "isCustomEntity", "", tableInfo.get("IsCustomEntity"), 1.0,
                CRAWLER, "");
        datasetClassNode.addProperty(datasetIdPrefix, "tableType", "", tableInfo.get("TableType"), 1.0, CRAWLER,
                "");
        datasetClassNode.addProperty(datasetIdPrefix, "isSolutionAware", "", tableInfo.get("IsSolutionAware"), 1.0,
                CRAWLER, "");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> descriptionLabels = (List<Map<String, Object>>) ((Map<String, Object>) tableInfo
                .get("Description")).get("LocalizedLabels");
        if (!Util.isEmpty(descriptionLabels)) {
            datasetClassNode.addProperty(datasetIdPrefix, "description", "",
                    descriptionLabels.get(0).get("Label"), 1.0, CRAWLER, "");
        }
        ConcreteDataset datasetNode = new ConcreteDataset(tableName, 1.0, "", CRAWLER,
                "Dataset", tableName);
        datasetNode.addProperty(this.idPrefix(DATASET, datasetNode), ENTITY_NAME, "",
                tableName, 1.0, CRAWLER, "");
        datasetNode.definedBy(datasetClassNode, 1.0, CRAWLER, "");

        assertAborted();

        addTableFields(schemaNode.getName(), datasetClassNode, attributesInfo, (String) tableInfo.get("LogicalName"));

        schemaNode.contains(datasetNode, 1.0, CRAWLER, "");
    }

    private void addForeignKeys(ConcreteDataPlatform dataPlatform) {
        manyToOneRelationships.forEach((table, relationships) -> {
            for (var relation : relationships) {
                Map<String, Object> pkTableInfo = tableMetadataByName.get((String) relation.get("ReferencedEntity"));
                if (pkTableInfo == null) {
                    // pk table excluded
                    continue;
                }
                String pkTable = ParamConvertor.toString(pkTableInfo.get("EntitySetName"));
                String fkTable = table;
                String pkField = (String) relation.get("ReferencedAttribute");
                String fkField = (String) relation.get("ReferencingAttribute");
                // String fkField = "_" + (String) relation.get("ReferencingAttribute") +
                // "_value";
                // if (pkTable.equals(fkTable)) {
                // // TO-DO self links create dettached nodes in Catalog
                // continue;
                // }
                Map<String, Property> properties = new HashMap<>();
                properties.put(FkCategory.fkTableName.name(),
                        new PropertyImpl("property:" + FkCategory.fkTableName.name(), FkCategory.fkTableName.name(),
                                fkTable, FkCategory.fkTableName.name(), 1.0, "Crawler", ""));
                properties.put(FkCategory.pkTableName.name(),
                        new PropertyImpl("property:" + FkCategory.pkTableName.name(), FkCategory.pkTableName.name(),
                                pkTable, FkCategory.pkTableName.name(), 1.0, "Crawler", ""));
                properties.put(FkCategory.fkColumnName.name(),
                        new PropertyImpl("property:" + FkCategory.fkColumnName.name(), FkCategory.fkColumnName.name(),
                                fkField, FkCategory.fkColumnName.name(), 1.0, "Crawler", ""));
                properties.put(FkCategory.pkColumnName.name(),
                        new PropertyImpl("property:" + FkCategory.pkColumnName.name(), FkCategory.pkColumnName.name(),
                                pkField, FkCategory.pkColumnName.name(), 1.0, "Crawler", ""));

                dataPlatform.getSchemas().stream()
                        .forEach(schema -> {
                            Optional<Contains<Dataset>> fkDatasetOpt = schema.getNode().dataset(fkTable);
                            Optional<Contains<Dataset>> pkDatasetOpt = schema.getNode().dataset(pkTable);

                            if (fkDatasetOpt.isPresent() && pkDatasetOpt.isPresent()) {
                                Optional<ClassNode> fkClassOpt = fkDatasetOpt.get().getNode().classNode(fkTable);
                                Optional<ClassNode> pkClassOpt = pkDatasetOpt.get().getNode().classNode(pkTable);

                                if (fkClassOpt.isPresent() && pkClassOpt.isPresent()) {
                                    ((ConcreteClassNode) fkClassOpt.get()).refersTo(
                                            pkClassOpt.get(),
                                            fkField,
                                            pkField,
                                            1.0,
                                            "Crawler",
                                            "",
                                            "",
                                            properties);
                                }
                            }
                        });
            }
        });
    }

    private List<Set<String>> partition(Set<String> components, int size) {
        List<Set<String>> partitions = new ArrayList<>();
        Set<String> currentPartition = new HashSet<>(size);
        for (String item : components) {
            currentPartition.add(item);
            if (currentPartition.size() == size) {
                partitions.add(currentPartition);
                currentPartition = new HashSet<>(size);
            }
        }
        if (!currentPartition.isEmpty()) {
            partitions.add(currentPartition);
        }
        return partitions;
    }

    private ConcreteSchemaNode createSchemaNode(ConcreteDataPlatform dataPlatform, String solutionName) {
        ConcreteSchemaNode schemaNode = new ConcreteSchemaNode(solutionName, 1.0, CRAWLER, "",
                "Schema name", solutionName);
        dataPlatform.contains(schemaNode, 1.0, CRAWLER, "");
        schemaNode.addProperty(idPrefix(SCHEMA, schemaNode), ENTITY_NAME, "Name of the schema",
                solutionName, 1.0, CRAWLER, "");
        return schemaNode;
    }

    private void modifyFilters(StringBuilder filters, Set<String> values, String field, String comparisonOperator,
            String booleanOperator) {
        if (!Util.isEmpty(values)) {
            if (!filters.isEmpty()) {
                filters.append(" and ");
            }
            // if (values.size() > 1) {
            filters.append("( ");
            // }
            Iterator<String> valuesItr = values.iterator();
            for (int i = 0; i < values.size(); i++) {
                filters.append(String.format("%s %s '%s'", field, comparisonOperator, valuesItr.next()));
                if (i < values.size() - 1) {
                    filters.append(String.format(" %s ", booleanOperator));
                }
            }
            // if (values.size() > 1) {
            filters.append(" )");
            // }
        }
    }

    private void addTableFields(String schemaName, ConcreteClassNode datasetClassNode,
            List<Map<String, Object>> attributes,
            String tableLogicalName)
            throws Exception {
        String datasetIdPrefix = this.idPrefix(CLASS, datasetClassNode);
        int cnt = 0;
        // Map<String, Object> picklistAttributes = null;
        // Map<String, Object> multiSelectPicklistAttributes = null;
        // Map<String, Object> stateAttributes = null;
        // Map<String, Object> statusAttributes = null;
        String primaryIdAttr = (String) tableMetadataByName.get(tableLogicalName).get("PrimaryIdAttribute");
        // String tableName = datasetClassNode.getName();
        // Set<Map<String,String>> tableEnumTypes = new LinkedHashSet<>();

        for (Map<String, Object> attribute : attributes) {
            String fieldName = (String) attribute.get("LogicalName");
            String attributeType = (String) attribute.get("AttributeType");
            @SuppressWarnings("unchecked")
            String attributeTypeName = (String) ((Map<String, Object>) attribute.get("AttributeTypeName")).get("Value");
            String attributeOf = (String) attribute.get("AttributeOf");
            String format = attribute.containsKey("Format") ? (String) attribute.get("Format") : "None";
            @SuppressWarnings("unchecked")
            String requiredLevel = ((Map<String, String>) attribute.get("RequiredLevel")).get("Value");

            ConcreteField columnNode = new ConcreteField(fieldName, 1.0, CRAWLER, "", "Field name",
                    fieldName);
            datasetClassNode.contains(columnNode, 1.0, "Crawler", "");
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> descriptionLabels = (List<Map<String, Object>>) ((Map<String, Object>) attribute
                    .get("Description")).get("LocalizedLabels");
            if (!Util.isEmpty(descriptionLabels)) {
                columnNode.addProperty(datasetIdPrefix, "description", "",
                        descriptionLabels.get(0).get("Label"), 1.0, CRAWLER, "");
            }
            columnNode.addProperty(idPrefix(FIELD, columnNode), Category.sourceDataType.name(),
                    "", attributeType, 1.0, "Crawler", "");
            columnNode.addProperty(idPrefix(FIELD, columnNode), Category.sourceNullable.name(),
                    "Nullability of the field 1 or 0", requiredLevel.equalsIgnoreCase("none"), 1.0, "Crawler", "");
            columnNode.addProperty(idPrefix(FIELD, columnNode), Category.sqlDataType.name(), "Sql Column type",
                    getSqlDataType(attributeType, format), 1.0, "Crawler", "");
            columnNode.addProperty(idPrefix(FIELD, columnNode), Category.sourceEntityType.name(), "Role",
                    "Column", 1.0, "Crawler", "");
            columnNode.addProperty(idPrefix(FIELD, columnNode), Category.ordinalPosition.name(),
                    "Column ordinal position", cnt, 1.0, "Crawler", "");
            String catalogType = getCatalogDataType(attributeType, format).getClassName();
            columnNode.addProperty(
                    idPrefix(FIELD, columnNode),
                    Category.definedBy.name(),
                    "Data type for field",
                    attributeTypeName.equalsIgnoreCase("multiselectpicklisttype") ? "String" : catalogType,
                    1.0,
                    "Crawler",
                    "");

            if (!Util.isEmpty(attributeOf)) {
                columnNode.addProperty(idPrefix(FIELD, columnNode), "attributeOf",
                        "", attributeOf, 1.0, "Crawler", "");
            }
            if (fieldName.equals(primaryIdAttr)) {
                columnNode.addProperty(idPrefix(FIELD, columnNode), "pk", "Primary Key", true, 1.0, "Crawler",
                        "");
            }
            cnt++;

            // picklistAttributes = fetchAndClassifyIfNeeded(
            // tableLogicalName,
            // attribute,
            // EnumType.PICKLIST,
            // picklistAttributes,
            // columnNode,
            // tableEnumTypes);

            // multiSelectPicklistAttributes = fetchAndClassifyIfNeeded(
            // tableLogicalName,
            // attribute,
            // EnumType.MULTISELECT,
            // multiSelectPicklistAttributes,
            // columnNode,
            // tableEnumTypes);

            // stateAttributes = fetchAndClassifyIfNeeded(
            // tableLogicalName,
            // attribute,
            // EnumType.STATE,
            // stateAttributes,
            // columnNode,
            // tableEnumTypes);

            // statusAttributes = fetchAndClassifyIfNeeded(
            // tableLogicalName,
            // attribute,
            // EnumType.STATUS,
            // statusAttributes,
            // columnNode,
            // tableEnumTypes);

        }
        // writeToOptionSetsMtable(schemaName, tableName, new
        // ArrayList<>(tableEnumTypes));
        // createGeneratorsMTable(schemaName, tableName, tableEnumTypes);
        fieldsCnt.addAndGet(cnt);
        MonitorStatusUpdater.getInstance().updateProgress(STATUS_CRAWLER, jobUid, cnt);
    }

    // private Map<String, Object> fetchAndClassifyIfNeeded(
    // String tableLogicalName,
    // Map<String, Object> attribute,
    // EnumType type,
    // Map<String, Object> attributesMap,
    // ConcreteField columnNode, Collection<Map<String, String>> tableEnumTypes)
    // throws Exception {

    // String attributeType = (String) attribute.get("AttributeType");
    // @SuppressWarnings("unchecked")
    // String attributeTypeName = (String) ((Map<String,
    // Object>)attribute.get("AttributeTypeName")).get("Value");
    // String fieldName = (String) attribute.get("LogicalName");

    // boolean isMatch = switch (type) {
    // case PICKLIST -> attributeType.equalsIgnoreCase("picklist");
    // case MULTISELECT ->
    // attributeTypeName.equalsIgnoreCase("multiselectpicklisttype");
    // case STATE -> attributeType.equalsIgnoreCase("state");
    // case STATUS -> attributeType.equalsIgnoreCase("status");
    // };

    // if (isMatch) {
    // if (attributesMap == null) {
    // attributesMap = fetchEnumAttributeMetadata(tableLogicalName, attribute);
    // attributesMap.forEach((k, v) -> {

    // @SuppressWarnings("unchecked")
    // Map<String, Object> valAsMap = (Map<String, Object>)v;

    // @SuppressWarnings("unchecked")
    // Map<String, Object> optionSet = (Map<String, Object>)
    // valAsMap.get("OptionSet");

    // @SuppressWarnings("unchecked")
    // List<Map<String, Object>> options = (List<Map<String, Object>>)
    // optionSet.get("Options");

    // options.forEach(o -> {
    // Map<String, String> enumTypeForCSV = new LinkedHashMap<>();
    // enumTypeForCSV.put("enum_type", k);
    // enumTypeForCSV.put("multipicklist",
    // String.valueOf(type.equals(EnumType.MULTISELECT)));

    // String val = String.valueOf(o.get("Value"));
    // @SuppressWarnings("unchecked")
    // Map<String, Object> label = (Map<String, Object>) o.get("Label");
    // @SuppressWarnings("unchecked")
    // List<Map<String, Object>> localizedLabels = (List<Map<String, Object>>)
    // label.get("LocalizedLabels");
    // String labelStr = String.valueOf(localizedLabels.get(0).get("Label"));
    // enumTypeForCSV.put("value", val);
    // enumTypeForCSV.put("label", labelStr);
    // tableEnumTypes.add(enumTypeForCSV);
    // });
    // });

    // }
    // @SuppressWarnings("unchecked")
    // Map<String, Object> info = (Map<String, Object>)
    // attributesMap.get(fieldName);
    // @SuppressWarnings("unchecked")
    // Map<String, Object> optionSet = (Map<String, Object>) info.get("OptionSet");

    // String enumName = (String) optionSet.get("Name");
    // columnNode.addProperty(idPrefix(FIELD, columnNode),
    // Category.classification.name(), "",
    // enumName, 1.0, "Crawler", "");
    // }

    // return attributesMap;
    // }

    // private Map<String, Object> fetchEnumAttributeMetadata(String logicalName,
    // Map<String, Object> attribute)
    // throws Exception {
    // Map<String, Object> metadataByAttr = new HashMap<>();
    // try (var s = session.statement()) {
    // Map<String, Object> params = Util.map(
    // "$select", "LogicalName",
    // "$expand",
    // "OptionSet($select=Name,OptionSetType,Description,DisplayName,Options)",
    // "LanguageCode", 1033);
    // String path = String.format(ATTRIBUTE_METADATA_API_PATH, logicalName,
    // ((String) attribute.get("@odata.type")).substring(1));
    // try (Result res = executeWithRetry(() -> Util.rte(() ->
    // s.execute(Util.map("path", path, "params", params, "debug",
    // session.debug))))) {
    // for (Row r : res) {
    // metadataByAttr.put((String) r.get("LogicalName"), r);
    // }
    // }
    // }
    // return metadataByAttr;
    // }

    private Result executeWithRetry(Supplier<Result> func) {
        int attempt = 0;
        while (true) {
            try {
                return func.get();
            } catch (Exception t) {
                if (++attempt < 3) {
                    throw t;
                }
                Util.sleep(1000);
            }
        }
    }

    private PrimitiveClass getCatalogDataType(String attributeType, String format) {
        return DataverseAttributeType.from(attributeType, format).getPrimitiveClass();
    }

    private int getSqlDataType(String attributeType, String format) {
        return DataverseAttributeType.from(attributeType, format).getSqlType();
    }

    private String idPrefix(String prefix, ConcreteNode node) {
        return prefix + ":" + node.getId();
    }

    private void assertAborted() {
        if (aborted) {
            throw new CrawlerAbortedException(
                    String.format("Crawler for dataPlatform '%s' is aborted", interfaceName));
        }
    }

    private void log(String message, Object... args) {
        if (session.debug) {
            log.info(message, args);
        } else {
            log.debug(message, args);
        }
    }

    public static void writeToCSV(List<Map<String, String>> data, File csv) throws IOException {
        if (data.isEmpty()) {
            return;
        }
        // String projectFolder =
        // Utils.projectDir().orElseThrow(IllegalStateException::new);

        // Path dir;
        // if (file.startsWith("wd") || file.startsWith("_")) {
        // dir = Path.of(projectFolder, "Implementation", "LogicalUnits", "k2_ref",
        // "Mtable");
        // } else {
        // dir = Path.of(projectFolder, "Implementation", "SharedObjects", "Interfaces",
        // "Discovery", "Mtable");
        // }

        // File csv = dir.resolve(file.concat(".csv")).toFile();

        String type = csv.getName().replace(".csv", "");

        BufferedWriter writer = new BufferedWriter(new FileWriter(csv));
        Map<String, String> firstObj = data.get(0);
        Set<Map.Entry<String, String>> entries = firstObj.entrySet();
        Iterator<Map.Entry<String, String>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            writer.write(entry.getKey());
            if (iterator.hasNext()) {
                writer.write(",");
            }
        }
        writer.newLine();

        Iterator<Map<String, String>> dataIterator = data.iterator();
        while (dataIterator.hasNext()) {
            Map<String, String> element = dataIterator.next();
            Iterator<Map.Entry<String, String>> rowIterator = element.entrySet().iterator();

            StringBuilder row = new StringBuilder();

            while (rowIterator.hasNext()) {
                Map.Entry<String, String> entry = rowIterator.next();
                String value = entry.getValue();

                if (value == null) {
                    value = "";
                } else {
                    value = value.replace("\\\\", "\\");

                    if (type.startsWith(MASKING_SETUP) && (entry.getKey().equalsIgnoreCase("params") ||
                            entry.getKey().equalsIgnoreCase("formatterParams") ||
                            entry.getKey().equalsIgnoreCase("preExecutionParams"))) {

                        value = value.replace("\\\"", "\"\"");
                    }
                }

                // Standard CSV escaping
                boolean containsSpecial = value.contains(",") || value.contains("\"") || value.contains("\n")
                        || value.contains("\r");
                if (containsSpecial) {
                    value = value.replace("\"", "\"\"");
                    value = "\"" + value + "\"";
                }

                row.append(value);

                if (rowIterator.hasNext()) {
                    row.append(",");
                }
            }

            writer.write(row.toString());
            if (dataIterator.hasNext()) {
                writer.newLine();
            }
        }

        writer.close();
    }

    @Override
    public void abort() throws Exception {
        this.aborted = true;
    }

    @Override
    public SnapshotDataset snapshotDataset(String dataset, String schema, SampleSize size, Map<String, Object> params)
            throws Exception {
        return new DataverseSnapshot(dataset, size, params);
    }

    private class DataverseSnapshot implements SnapshotDataset {

        private final String dataset;
        private final SampleSize size;
        private Result data;

        public DataverseSnapshot(String dataset, SampleSize size, Map<String, Object> params) {
            this.dataset = dataset;
            this.size = size;
        }

        @Override
        public void close() throws Exception {
            Util.safeClose(data);
            data = null;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Iterator<Map<String, Object>> fetch() throws Exception {
            if (data == null) {
                int limit = getLimit();
                Map<String, Object> input = Util.map(
                        "path", String.format(TABLE_DATA_API_PATH, dataset),
                        "params", Util.map("$top", limit),
                        "debug", session.debug);

                if (limit > 0) {
                    try (var s = session.statement()) {
                        this.data = executeWithRetry(() -> Util.rte(() -> s.execute(input)));
                    } catch (HttpException e) {
                        if (e.getCode() == 400 && e.getMessage().contains("0x80040800")) {
                            return Collections.emptyIterator();
                        }
                        throw e;
                    }
                } else {
                    return Collections.emptyIterator();
                }

            }
            return (Iterator<Map<String, Object>>) (Iterator<?>) data.iterator();
        }

        private int getLimit() throws Exception {
            int limit;
            int count = getNumberOfRows(dataset);
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

        private int getNumberOfRows(String table) throws IOException, Exception {
            Map<String, Object> input = Util.map(
                    "path", String.format(TABLE_COUNT_API_PATH, table),
                    "headers", Util.map("Accept", "text/plain"),
                    "debug", session.debug);
            try (InputStream is = session.read(input)) {
                String cnt = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                if (cnt.startsWith("\uFEFF")) {
                    cnt = cnt.substring(1);
                }
                return Integer.parseInt(cnt);
            } catch (HttpException e) {
                log.error(e);
                // if (e.getCode() == 400 && e.getMessage().contains("0x80040800")) {
                return 0;
                // }
                // throw e;
            } finally {
                session.resetIoStreamHttpSession();
            }
        }

    }

    // @Override
    // overrides in 8.3
    public SnapshotDataset snapshotDataset(String catalog, String schema, String dataset, SampleSize size,
            Map<String, Object> props) throws Exception {
        throw new UnsupportedOperationException();
    }

}
