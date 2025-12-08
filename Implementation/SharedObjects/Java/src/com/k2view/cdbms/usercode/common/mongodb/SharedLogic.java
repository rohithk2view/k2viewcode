package com.k2view.cdbms.usercode.common.mongodb;

import java.util.Map;

import com.k2view.fabric.common.io.IoProvider;
import com.k2view.fabric.common.io.IoSession;

public class SharedLogic {
	public static IoProvider mongoDbIoProvider() {
        return new IoProvider() {

            @Override
            public IoSession createSession(String ioProviderFunc, Map<String, Object> args) throws Exception {
                return new MongoDbSession(args);
            }

            @SuppressWarnings("unchecked")
            @Override
            public <T extends IoProvider> T unwrap(Class<T> clz) {
                return (T) this;
            }
            
        };
    }

    public static String mongoConvertTdmFilter(String filter) {
        return MongoTdmFilterConverter.convertToMongoFilter(filter).toBsonDocument().toJson();
    }
}

