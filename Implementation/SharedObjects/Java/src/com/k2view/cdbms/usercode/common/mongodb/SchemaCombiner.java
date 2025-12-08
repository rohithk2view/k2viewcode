package com.k2view.cdbms.usercode.common.mongodb;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.broadway.metadata.Any;
import com.k2view.broadway.metadata.ArrayType;
import com.k2view.broadway.metadata.ByExample;
import com.k2view.broadway.metadata.ObjectType;
import com.k2view.broadway.metadata.Primitive;
import com.k2view.broadway.metadata.Schema;
import com.k2view.broadway.metadata.Type;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.broadway.metadata.Properties;

public class SchemaCombiner {

    /**
     * Combines schemas from an Iterable of objects into a single Schema.
     * It's preferred to provide sample data that is null-free, because null has
     * type ANY, so it's indifferentiable from other ANY types,
     * and then upon a type conflict, ANY would be the resulting type.
     *
     * @param objects an Iterable of objects
     * @return the combined Schema
     */
    public static ObjectType combineSchemas(Iterable<?> objects) {
        Properties combinedProps = new Properties();
        for (Object obj : objects) {
            ObjectType schema = (ObjectType) ByExample.fromInstance(obj);
            combineProps(combinedProps, schema.properties());
        }
        ObjectType objectType = new ObjectType(combinedProps, null);
        normalizeObject(objectType);
        return objectType;
    }

    private static void combineProps(Properties combinedProps, Properties currProps) {
        for (var key : currProps.keySet()) {
            Schema value = currProps.get(key);
            Schema existingValue = combinedProps.get(key);
            if (!combinedProps.containsKey(key)) {
                combinedProps.put(key, value);
            } else {
                combinedProps.put(key, resolve(value, existingValue));
            }
        }
    }

    private static Schema resolve(Schema value, Schema existingValue) {
        if (existingValue != null && existingValue.equals(Any.ANY)) {
            return value;
        } else if (existingValue == null || value.equals(Any.ANY)) {
            // if existingValue==null => indication that there was some conflict earlier in
            // this prop - keep it null
            // if value is ANY, means data was null => also no need to change
            return existingValue;
        } else if (!value.getClass().equals(existingValue.getClass()) || !value.type().equals(existingValue.type())) {
            // Conflict => put null
            return null;
        } else if (value.type().equals(Type.array)) {
            Schema schema = resolve(value.items(), existingValue.items());
            return new ArrayType(schema);
        } else if (value.type().equals(Type.object)) {
            // merge objects
            Properties combinedProps = existingValue.properties();
            Properties currProps = value.properties();
            combineProps(combinedProps, currProps);
            return existingValue;
        } else {
            // equal primitives
            return value;
        }
    }

    private static void normalizeObject(ObjectType schema) {
        Properties props = schema.properties();
        for (var entry : props.entrySet()) {
            Schema value = entry.getValue();
            if (value == null || value.equals(Any.ANY)) {
                entry.setValue(Any.ANY);
            } else if (value instanceof ArrayType a) {
                entry.setValue(normalizeArray(a));
            } else if (value instanceof ObjectType o) {
                normalizeObject(o);
            } else {
                // do nothing for primitives
            }
        }
    }

    private static ArrayType normalizeArray(ArrayType arr) {
        Schema items = arr.items();
        if (items == null) {
            return new ArrayType(Any.ANY);
        }
        if (items.equals(Any.ANY) || items.type().isPrimitive()) {
            return arr;
        }
        if (items instanceof ObjectType o) {
            normalizeObject(o);
            return arr;
        }
        if (items instanceof ArrayType a) {
            return new ArrayType(normalizeArray(a));
        }
        return new ArrayType(Any.ANY);
    }
}
