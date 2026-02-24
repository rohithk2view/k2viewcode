package com.k2view.cdbms.usercode.common.dataverse.actors;

import java.util.*;
import java.util.stream.Collectors;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class DataverseBulkDelete extends DataverseAbstractActor{

    @Override
    protected void execute(Data input, Data output) throws Exception {
        buildHeader(input);
        Object criteria = input.get("criteria");
        if (criteria != null) {
            if (criteria instanceof String s) {
                input.put("criteria", Json.get().fromJson(s));
            } else if (!(criteria instanceof Map)) {
                throw new IllegalArgumentException("Input 'criteria' must be either a map or JSON string");
            }
        }
        Result result = statement.execute(input.fields());
        output.put("job_id", result.firstRow().get("JobId"));
    }

    private void buildHeader(Data input) {
        @SuppressWarnings("unchecked")
        Map<String, Object> headers = (Map<String, Object>) input.map("headers");
        Set<String> existing = headers.keySet().stream().map(s -> ParamConvertor.toString(s).toLowerCase()).collect(Collectors.toSet());

        setHeader("accept", existing, headers, "application/json");
        setHeader("content-type", existing, headers, "application/json");

        input.put("headers", headers);
    }

    private void setHeader(String key, Set<String> existing, Map<String, Object> headers, String value) {
        if (value!=null && !existing.contains(key)) {
            headers.put(key, value);
        }
    }
	
}

