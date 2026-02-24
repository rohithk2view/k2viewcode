package com.k2view.cdbms.usercode.common.dataverse.actors;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.fabric.common.io.IoCommand.Row;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class DataverseBulkDeleteAwait extends DataverseAbstractActor {

    @Override
    protected void execute(Data input, Data output) throws Exception {
        String jobId = input.string("job_id");
        long waitForSeconds = input.integer("wait_for_seconds");
        Map<String, Object> fields = Util.map(
                "path", "bulkdeleteoperations", 
                "method", "GET",
                "wait_for_seconds", waitForSeconds,
                "params", Map.of(
                    "$filter", "_asyncoperationid_value eq " + jobId,
                    "$select", "successcount,failurecount,statecode,statuscode"
                    ));
       Result res = statement.execute(fields);
       output.put("result", res.firstRow());
    }
}