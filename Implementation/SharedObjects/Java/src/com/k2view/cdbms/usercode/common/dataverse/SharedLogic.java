package com.k2view.cdbms.usercode.common.dataverse;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import com.k2view.cdbms.interfaces.jobs.custom.CustomConnection;
import com.k2view.fabric.common.IteratorTranslate;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;
import com.k2view.fabric.common.mtable.MTable;
import com.k2view.fabric.common.mtable.MTables;
import com.k2view.cdbms.interfaces.jobs.http.HTTPConnection;
import com.k2view.cdbms.shared.Utils;
import com.k2view.discovery.schema.model.impl.PrimitiveClass;
import com.k2view.discovery.schema.model.types.BooleanClass;
import com.k2view.discovery.schema.model.types.BytesClass;
import com.k2view.discovery.schema.model.types.CollectionClass;
import com.k2view.discovery.schema.model.types.DateClass;
import com.k2view.discovery.schema.model.types.DateTimeClass;
import com.k2view.discovery.schema.model.types.IntegerClass;
import com.k2view.discovery.schema.model.types.RealClass;
import com.k2view.discovery.schema.model.types.StringClass;

public class SharedLogic {
    public static IoProvider dataverseIoProvider() {
        return new IoProvider() {
            @Override
            public IoSession createSession(String providerFunc, Map<String, Object> sessionArgs) throws Exception {
                return new DataverseIoSession(sessionArgs);
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T extends IoProvider> T unwrap(Class<T> clz) {
                return (T) this;
            }
        };
    }

    public static String dataverseConvertTDMFilter(String filter) {
        String res = DataverseTDMFilterConverter.convertToODataFilter(filter);
        if (Util.isEmpty(res)) {
            return null;
        }
        return res;
    }

    public enum DataverseAttributeType {
        STRING("String", "None", StringClass.STRING, Types.VARCHAR), 
        MEMO("Memo", "None", StringClass.STRING, Types.CLOB),
        BOOLEAN("Boolean", "None", BooleanClass.BOOLEAN, Types.BOOLEAN),
        DATETIME_DATEONLY("DateTime", "DateOnly", DateClass.DATE, Types.DATE),
        DATETIME_DATEANDTIME("DateTime", "DateAndTime", DateTimeClass.DATETIME, Types.TIMESTAMP),
        INTEGER("Integer", "None", IntegerClass.INTEGER, Types.INTEGER),
        BIGINT("BigInt", "None", IntegerClass.INTEGER, Types.BIGINT),
        DECIMAL("Decimal", "None", RealClass.REAL, Types.DECIMAL),
        DOUBLE("Double", "None", RealClass.REAL, Types.DOUBLE),
        MONEY("Money", "None", RealClass.REAL, Types.DECIMAL),
        UNIQUEIDENTIFIER("Uniqueidentifier", "None", StringClass.STRING, Types.CHAR),
        ENTITYNAME("EntityName", "None", StringClass.STRING, Types.VARCHAR),
        LOOKUP("Lookup", "None", StringClass.STRING, Types.VARCHAR),
        PICKLIST("Picklist", "None", IntegerClass.INTEGER, Types.INTEGER),
        STATE("State", "None", IntegerClass.INTEGER, Types.INTEGER),
        STATUS("Status", "None", IntegerClass.INTEGER, Types.INTEGER),
        // Collection of integers
        MULTISELECTPICKLIST("MultiSelectPicklist", "None", CollectionClass.COLLECTION, Types.VARCHAR),
        IMAGE("Image", "None", BytesClass.BYTES, Types.VARBINARY),
        UNKNOWN("Unknown", "None", StringClass.STRING, Types.VARCHAR);

        private final String attributeType;
        private final PrimitiveClass primitiveClass;
        private final int sqlType;
        private final String format;

        DataverseAttributeType(String attributeType, String format, PrimitiveClass primitiveClass, int sqlType) {
            this.attributeType = attributeType;
            this.primitiveClass = primitiveClass;
            this.sqlType = sqlType;
            this.format = format;
        }

        public PrimitiveClass getPrimitiveClass() {
            return primitiveClass;
        }

        public int getSqlType() {
            return sqlType;
        }

        public static DataverseAttributeType from(String type, String format) {
            for (DataverseAttributeType t : values()) {
                if (t.attributeType.equalsIgnoreCase(type) && format.equalsIgnoreCase(t.format)) {
                    return t;
                }
            }
            return UNKNOWN;
        }
    }

    // public static void dataverseCreateGeneratorsMTable(Iterable<String>
    // enumerations) throws IOException {
    // if (enumerations != null) {
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

    // List<Map<String, String>> generatorsData = new LinkedList<>();
    // for (String enumName : enumerations) {
    // Map<String, String> map = new LinkedHashMap<>(generatorRow);
    // map.put("classification", enumName);
    // map.put("generator", RANDOM_ENUM_ACTOR);
    // map.put("params", Json.get().toJson(Map.of("mtable", enumName)));
    // generatorsData.add(map);
    // }

    // Map<String, String> map = new LinkedHashMap<>(generatorRow);
    // map.put("classification", WorkdayIoMetadata.WORKDAY_VALUE_BASED_ON_ATTR);
    // map.put("generator", RANDOM_ID_ACTOR);
    // generatorsData.add(map);

    // writeToCSV(generatorsData, MASKING_SETUP + "___dataverse");
    // }

    // // String projectFolder =
    // // Utils.projectDir().orElseThrow(IllegalStateException::new);
    // // Path dir = Path.of(projectFolder, "Implementation", "SharedObjects",
    // // "Interfaces", "Discovery", "Mtable");
    // // MTableDeployUtil.initMTableFromCsv(dir.toFile(), "@discovery");
    // }

    public static Iterable<Object> dataverseRandomMultiPicklist(Iterable<Object> iterable) {
        List<Object> list = new ArrayList<>();
        for (var item : iterable) {
            list.add(item);
        }
        Collections.shuffle(list);
        Random random = new Random();
        int numberOfElements = random.nextInt(list.size() + 1);
        return list.subList(0, numberOfElements);
    }

}
