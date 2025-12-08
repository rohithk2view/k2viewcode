package com.k2view.cdbms.usercode.common.mongodb.actors;

import java.util.*;
import java.sql.*;
import java.math.*;
import java.io.*;
import com.k2view.cdbms.shared.*;
import com.k2view.cdbms.sync.*;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.session.Session;
import com.k2view.fabric.session.broadway.sourcedbquery.SourceDbQuery;
import com.k2view.broadway.model.Context;
import com.k2view.broadway.model.Data;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.lut.map.BroadwayFlowMapObject;
import com.k2view.cdbms.shared.logging.LogEntry.*;
import com.k2view.fabric.common.AnIterable;
import com.k2view.fabric.common.IteratorTranslate;
import com.k2view.fabric.common.Util;
import com.k2view.fabric.common.io.IoCommand.Result;
import com.k2view.fabric.common.io.IoCommand.Row;
import com.k2view.fabric.common.io.basic.IoJdbc;
import com.k2view.fabric.common.io.basic.IoSimpleResultSet;

public class MongoDbActorSourceDbQuery extends MongoDbActorRead{
    public static final String INPUT_PARENT_ROWS = "parent_rows";
    public static final String INPUT_SIZE = "size";

    private static final Runnable EMPTY = () -> {};
    private Session fabricSession;
    private Runnable rowCount;

    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        if (parentRowsEmpty(input.object(INPUT_PARENT_ROWS))) {
            output.put("result", IoSimpleResultSet.ZERO_ROWS_AFFECTED);
            return;
        }
        this.rowCount = (Runnable)context.externals().getOrDefault(BroadwayFlowMapObject.DATA_PARAM_ROW_COUNT, EMPTY);
        if (this.fabricSession == null)
            this.fabricSession = (Session)((IoJdbc)context.ioProvider().createSession("fabric").unwrap()).connection().unwrap(Session.class);
        if (this.fabricSession.scope().bool("ROWS_GENERATOR")) {
            // No need to connect to MongoDB system if we're generating rows, pass on parent rows to be handled by DocumentQuery actor
            output.put("result", input.object(INPUT_PARENT_ROWS));
        } else {
            // Otherwise use MongoDbSession
            super.action(input, output, context);
            // Trigger rowCount during result iteration
            Result res = (Result) output.get("result");
            output.put("result", new Result() {
                @Override
                public Iterator<Row> iterator() {
                    return new IteratorTranslate<>(res.iterator(), o -> {
                        rowCount.run();
                        return o;
                    });
                }
            });
        }
    }

    @Override
    String subIdentifier() {
        return "__sourceDbQuery";
    }
	
    private static boolean parentRowsEmpty(Object parentRows) {
        if (parentRows == null) return true;
        if (parentRows instanceof Map m) {
            return Util.isEmpty(m);
        } else if (parentRows instanceof Iterable<?> iterable){
            return !iterable.iterator().hasNext();
        }
        return true;
    }

    @Override
    public void close() {
        this.fabricSession = null;
        super.close();
    }
}

