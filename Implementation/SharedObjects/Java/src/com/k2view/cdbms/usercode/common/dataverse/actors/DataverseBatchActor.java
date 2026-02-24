package com.k2view.cdbms.usercode.common.dataverse.actors;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.fabric.common.Json;
import com.k2view.fabric.common.ParamConvertor;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.broadway.actors.builtin.JsonStringify;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class DataverseBatchActor extends DataverseAbstractActor{
	@Override
    protected void execute(Data input, Data output) throws Exception {
        Json json = Json.get();
        Object payload = input.get("payload");
        if (payload instanceof String s) {
            input.put("payload", json.fromJson(s));
        }
        // input.put("stream", input.get("payload"));
        statement.batch(input.fields());
        // output.put("affectedRows", 1);
    }
}