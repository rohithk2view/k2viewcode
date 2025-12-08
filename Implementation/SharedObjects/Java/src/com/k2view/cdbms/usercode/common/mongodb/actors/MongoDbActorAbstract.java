package com.k2view.cdbms.usercode.common.mongodb.actors;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.cdbms.usercode.common.mongodb.MongoDbSession;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.broadway.actors.builtin.AbstractIoSession;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.broadway.tx.TxManager;
import com.k2view.broadway.util.DescribedIoResult;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public abstract class MongoDbActorAbstract extends AbstractIoSession {
    public static final String INPUT_DB = "database";
    public static final String INPUT_COLLECTION = "collection";
    public static final String INPUT_OPERATION = "operation";
    public static final String INPUT_FILTER = "filter";
    public static final String INPUT_FILTER_PARAMS = "filter_params";

    private IoCommand.Statement statement;
    private String prevDb;
    private String prevCollection;
    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        super.action(input, output, context);
        String collection = input.string(INPUT_COLLECTION);
        String db = input.string(INPUT_DB);
        if (statement == null || !collection.equals(prevCollection) || !db.equals(prevDb)) {
            Util.safeClose(statement);
            statement = this.session.statement();
            prevCollection = collection;
            prevDb = db;
        }
        Result result = statement.execute(input);
        output.put("result", result);
        output.put("affectedRows", result.rowsAffected());
    }
    
	@Override
    public void close() {
        Util.safeClose(statement);
        statement = null;
        super.close();
    }

    abstract String subIdentifier();

    @Override
    protected Map<String, Object> createSessionParams(Data input) {
        return Map.of(TxManager.SUB_IDENTIFIER, this.subIdentifier());
    }
}

