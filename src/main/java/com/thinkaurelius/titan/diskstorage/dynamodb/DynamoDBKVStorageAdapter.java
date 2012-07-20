package com.thinkaurelius.titan.diskstorage.dynamodb;

import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManager;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStorageManagerAdapter;
import org.apache.commons.configuration.Configuration;


public class DynamoDBKVStorageAdapter extends KeyValueStorageManagerAdapter {

    public DynamoDBKVStorageAdapter(Configuration config) {
        super(new DynamoDBKVStorageManager(config),config);
    }
}
