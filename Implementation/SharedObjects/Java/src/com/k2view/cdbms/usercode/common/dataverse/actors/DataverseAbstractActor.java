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

public abstract class DataverseAbstractActor extends AbstractIoSession {
	protected IoCommand.Statement statement;
	@Override
    public void action(Data input, Data output, Context context) throws Exception {
        super.action(input, output, context);
        if (this.statement == null) {
            statement = session.statement();
        }
        execute(input, output);
    }

    protected abstract void execute(Data input, Data output) throws Exception;

    @Override
    protected Map<String, Object> createSessionParams(Data input) {
        return Util.map("ActorClass", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        Util.safeClose(statement);
        statement = null;
        super.close();
    }
}

