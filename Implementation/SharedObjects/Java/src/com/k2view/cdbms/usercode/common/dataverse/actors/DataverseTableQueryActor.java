package com.k2view.cdbms.usercode.common.dataverse.actors;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.broadway.actors.builtin.AbstractIoSession;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class DataverseTableQueryActor extends DataverseAbstractActor {

    private QueryParamsKey previousKey = null;
    private Map<String, Object> cachedParams = null;

    @Override
    public void close() {
        this.previousKey = null;
        if (cachedParams != null) {
            cachedParams.clear();
        }
        this.cachedParams = null;
    }

    @Override
    protected void execute(Data input, Data output) throws Exception {
        QueryParamsKey currentKey = QueryParamsKey.from(input);

        if (cachedParams == null || !currentKey.equals(previousKey)) {
            cachedParams = buildParams(currentKey);
            previousKey = currentKey;
        }

        input.put("params", cachedParams);
        input.put("path", input.get("table"));

        Result result = statement.execute(input.fields());
        output.put("result", result);
    }

    private Map<String, Object> buildParams(QueryParamsKey key) {
        Map<String, Object> params = Util.map();

        if (!Util.isEmpty(key.filter()))
            params.put("$filter", key.filter());
        if (!Util.isEmpty(key.expand()))
            params.put("$expand", key.expand());
        if (!Util.isEmpty(key.orderby()))
            params.put("$orderby", key.orderby());
        if (!Util.isEmpty(key.apply()))
            params.put("$apply", key.apply());
        if (key.limit() > 0)
            params.put("$top", key.limit());
        if (!key.fields().isEmpty()) {
            params.put("$select", String.join(",", key.fields()));
        }

        return params;
    }

    // Record to hold the cache key
    private record QueryParamsKey(
            String filter,
            String expand,
            String orderby,
            String apply,
            long limit,
            List<String> fields) {
        static QueryParamsKey from(Data input) {
            Iterable<?> rawFields = input.iterable("fields");
            List<String> list = new ArrayList<>();
            if (rawFields != null) {
                for (Object f : rawFields) {
                    if (f instanceof String s && s.length() > 0)
                        list.add(s);
                }
            }
            return new QueryParamsKey(
                    input.string("filter"),
                    input.string("expand"),
                    input.string("orderby"),
                    input.string("apply"),
                    input.integer("limit"),
                    Collections.unmodifiableList(list));
        }
    }
}
