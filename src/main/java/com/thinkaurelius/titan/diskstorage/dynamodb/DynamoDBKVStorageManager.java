package com.thinkaurelius.titan.diskstorage.dynamodb;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.GraphDatabaseException;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import com.amazonaws.services.dynamodb.AmazonDynamoDBAsyncClient;


/**
 * Experimental storage manager for AWS DynamoDBDB.
 *  This version uses the simpler key-value storage approach...
 *
 * @author Garrick Evans <garrick.evans@autodesk.com>
 */
public class DynamoDBKVStorageManager implements KeyValueStorageManager {

  public static final String TABLE_PREFIX                       = "table-prefix";
  public static final String TABLE_PREFIX_DEFAULT               = "titan";
  public static final String LOCAL_LOCK_MEDIATOR_PREFIX_DEFAULT = "dynamodb";

  private static final Logger _logger = LoggerFactory.getLogger(DynamoDBKVStorageManager.class);

  private final byte[]                          _rid;
  private final int                             _lockRetryCount;
  private final long                            _lockWaitMS;
  private final long                            _lockExpiresMS;
  private final String                          _llmPrefix;
  private final String                          _tablePrefix;
  private final OrderedKeyColumnValueIDManager  _idManager;
  private final DynamoDBClient                  _dynamo;


  public DynamoDBKVStorageManager(org.apache.commons.configuration.Configuration config) {

    _dynamo = new DynamoDBClient(config);
    _tablePrefix = config.getString(TABLE_PREFIX, TABLE_PREFIX_DEFAULT);


    _lockRetryCount = config.getInt(GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
                                    GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT);

    _lockWaitMS = config.getLong(GraphDatabaseConfiguration.LOCK_WAIT_MS,
                                 GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT);

    _lockExpiresMS = config.getLong(GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
                                    GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);

    _llmPrefix = config.getString(LOCAL_LOCK_MEDIATOR_PREFIX_KEY, LOCAL_LOCK_MEDIATOR_PREFIX_DEFAULT);


    _rid = ConfigHelper.getRid(config);
    _idManager = new OrderedKeyColumnValueIDManager(_openDatabase("blocks_allocated", null, null), _rid, config);
  }

  @Override
  public long[] getIDBlock(int partition) {
    return _idManager.getIDBlock(partition);
  }

  @Override
  public OrderedKeyValueStore openDatabase(String name)
    throws GraphStorageException {

    LocalLockMediator llm = LocalLockMediators.INSTANCE.get(_llmPrefix + ":" + name);
    OrderedKeyValueStore lockStore = _openDatabase(name + "_locks", null, null);

    return _openDatabase(name, llm, lockStore);
  }

  @Override
  public TransactionHandle beginTransaction() {
    return new DynamoDBTransaction();
  }

  @Override
  public void close() {}

  @Override
  public void clearStorage() {
    _dynamo.deleteTables(_tablePrefix+".");
  }

  private final OrderedKeyValueStore _openDatabase(String name, LocalLockMediator llm, OrderedKeyValueStore lockStore)
    throws GraphStorageException {

    _dynamo.ensureTable( _tablePrefix+"."+name );

    return new DynamoDBOrderedKeyValueStore(_tablePrefix, name, _dynamo, lockStore, llm, _rid, _lockRetryCount, _lockWaitMS, _lockExpiresMS);
  }

}
