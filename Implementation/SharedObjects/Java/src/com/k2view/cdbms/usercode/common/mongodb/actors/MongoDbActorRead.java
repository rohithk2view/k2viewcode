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
import com.k2view.broadway.util.DescribedIoResult;
import com.k2view.cdbms.lut.*;
import com.k2view.cdbms.shared.logging.LogEntry.*;

public class MongoDbActorRead extends MongoDbActorAbstract {
    public static final String INPUT_PROJECTION = "projection";
    public static final String INPUT_LIMIT = "limit";
    public static final String INPUT_FLATTEN = "flatten";
    public static final String INPUT_SORT = "sort";

    @Override
    public void action(Data input, Data output, Context context) throws Exception {
        input.put(INPUT_OPERATION, MongoDbSession.Operation.FIND);
        super.action(input, output, context);
    }
    @Override
    String subIdentifier() {
        return "__read";
    }
}

