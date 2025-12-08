package com.k2view.cdbms.usercode.common.mongodb.actors;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.usercode.common.mongodb.MongoDbSession;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.fabric.common.io.IoCommand.Result;

public class MongoDbActorCount extends MongoDbActorAbstract{

    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        input.put(INPUT_OPERATION, MongoDbSession.Operation.COUNT);
        super.action(input, output, context);
        output.put("result", ((Result) output.get("result")).firstValue());
    }

    @Override
    String subIdentifier() {
        return "__count";
    }
	
}

