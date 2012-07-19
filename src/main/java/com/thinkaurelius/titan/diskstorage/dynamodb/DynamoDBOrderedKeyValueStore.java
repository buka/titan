package com.thinkaurelius.titan.diskstorage.dynamodb;


import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CountDownLatch;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.binary.Base64;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.AmazonDynamoDBAsyncClient;

/**
 * Experimental AWS DynamoDB backing store.
 *  This version utilitizes the ordered key-value store
 *
 * @author Garrick Evans <garrick.evans@autodesk.com>
 */
public class DynamoDBOrderedKeyValueStore implements OrderedKeyValueStore {

  private static final Logger _logger = LoggerFactory.getLogger(DynamoDBOrderedKeyValueStore.class);
  private static final ByteBuffer _EmptyBuffer = ByteBuffer.allocate(0);
  private static final String _ValueAttribute = "_value";


  private final String 					_prefix;
	private final String 					_family;
	private final String 					_table;
	private final LockConfig 			_internals;
	private final DynamoDBClient 	_dynamoClient;
	private final boolean					_forceConsistentRead;
	private final long						_futuresTimeout;
  //private final long            _readCapacity;
  private final long            _writeCapacity;

	DynamoDBOrderedKeyValueStore(String tablePrefix,
															 String tableName,
															 DynamoDBClient dynamoClient,
															 OrderedKeyValueStore lockStore,
															 LocalLockMediator llm,
															 byte[] rid,
															 int lockRetryCount,
															 long lockWaitMS,
															 long lockExpireMS) {
		_prefix = tablePrefix;
		_family = tableName;
		_table = _prefix + "." + _family;
		_dynamoClient = dynamoClient;
		_futuresTimeout = dynamoClient.futuresTimeoutMillis();
		_forceConsistentRead = dynamoClient.forceConsistentRead();
    //_readCapacity = dynamoClient.readCapacity();
    _writeCapacity = dynamoClient.writeCapacity();

		if (null != llm && null != lockStore) {
			_internals = new SimpleLockConfig(this, lockStore, llm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
		} else {
			_internals = null;
		}
	}

	@Override
	public void close() throws GraphStorageException {
    _dynamoClient.shutdown();
	}

	@Override
	public ByteBuffer get(ByteBuffer key,TransactionHandle txh) {

		try {

      String dynKey = _EncodeBuffer(key);

			GetItemResult res = _dynamoClient.client()
														.getItem(new GetItemRequest()
                                         	.withKey(new Key(new AttributeValue(dynKey)))
                                          .withTableName(_table)
                                          .withAttributesToGet(_ValueAttribute)
                                          .withConsistentRead(_forceConsistentRead));

			Map<String,AttributeValue> item = res.getItem();
			if (item == null) {
				_logger.debug("Item does not exist for key {}",dynKey);
				return null;
			}

			AttributeValue val = item.get(_ValueAttribute); 
			if (val == null) {
				_logger.debug("No data for key {}", dynKey);
				return _EmptyBuffer;
			}

			return _DecodeString(val.getS());
		}
    catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
	}

	@Override
	public boolean isLocalKey(ByteBuffer key) {
		return false;
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		return (null != get(key, txh));
	}

  @Override
  public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, TransactionHandle txh) {
    return getSlice(keyStart, keyEnd, Integer.MAX_VALUE, txh);
  }
  
  @Override
  public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, int limit, TransactionHandle txh) {
    return getSlice(keyStart, keyEnd, new LimitedSelector(limit), txh);
  }
  
  @Override
  public List<KeyValueEntry> getSlice(ByteBuffer keyStart, ByteBuffer keyEnd, KeySelector selector, TransactionHandle txh) {
    ArrayList<KeyValueEntry> results = null;

    try {
      Map<String,KeysAndAttributes> query = new HashMap<String,KeysAndAttributes>();
      ByteBuffer nextKey = keyStart;

      while (!selector.reachedLimit()) {
        if (selector.include(nextKey)) {
          query.put(_table, new KeysAndAttributes().withKeys(new Key().withHashKeyElement(new AttributeValue(_EncodeBuffer(nextKey)))).withAttributesToGet(_ValueAttribute));
          nextKey = ByteBufferUtil.nextBiggerBuffer(nextKey);
          if (!ByteBufferUtil.isSmallerThanWithEqual(nextKey, keyEnd, false)) {
            break;
          }
        }
      }

      while (true) {
          // first time through perform the query, any subsequent passes will just be collecting
          // any unprocessed items leftover from previous call
        BatchGetItemResult res = _dynamoClient.client().batchGetItem(new BatchGetItemRequest().withRequestItems(query));

        for (BatchResponse r: res.getResponses().values()) {
          for (Map<String,AttributeValue> item: r.getItems()) {
            for (Map.Entry<String,AttributeValue> e: item.entrySet()) {
              _logger.info("BATCH RETURNED: {} -> {}",new Object[]{e.getKey(), e.getValue().getS()});
              results.add(new KeyValueEntry(_DecodeString(e.getKey()), _DecodeString(e.getValue().getS())));
            }
          }
        }

        query = res.getUnprocessedKeys();
        if (query == null || query.size() == 0) break;
      }
    }
    catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }

    return results;
  }

  @Override
  public void insert(List<KeyValueEntry> entries, TransactionHandle txh) {
    try {
      ArrayList<Map<String,List<WriteRequest>>> all = new ArrayList<Map<String,List<WriteRequest>>>();
      Map<String,List<WriteRequest>> req;
      ArrayList<WriteRequest> writes;

      int count = 0;
      for (KeyValueEntry entry: entries) {
        if (count%25 == 0) {
          req = new HashMap<String,List<WriteRequest>>();
          writes = new ArrayList<WriteRequest>();
          req.put(_table, writes);
          all.add(req);
        }

        Map<String,AttributeValue> item = new HashMap<String,AttributeValue>();
        item.put(_EncodeBuffer(entry.getKey()), new AttributeValue(_EncodeBuffer(entry.getValue())));
        writes.add(new WriteRequest().withPutRequest(new PutRequest().withItem(item)));
        ++count;
      }

      for (Map<String,List<WriteRequest>> reqs: all) {
        while (reqs != null && reqs.size() > 0) {
          BatchWriteItemResult res = _dynamoClient.client().batchWriteItem(new BatchWriteItemRequest().withRequestItems(reqs));
          reqs = res.getUnprocessedItems();
        }
      }

    } catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
  }

  @Override
  public void delete(List<ByteBuffer> keys, TransactionHandle txh) {
    try {
      ArrayList<Map<String,List<WriteRequest>>> all = new ArrayList<Map<String,List<WriteRequest>>>();
      Map<String,List<WriteRequest>> req;
      ArrayList<WriteRequest> writes;

      int count = 0;
      for (ByteBuffer key: keys) {
        if (count%25 == 0) {
          req = new HashMap<String,List<WriteRequest>>();
          writes = new ArrayList<WriteRequest>();
          req.put(_table, writes);
          all.add(req);
        }

        writes.add(new WriteRequest().withDeleteRequest(new DeleteRequest().withKey(new Key().withHashKeyElement(new AttributeValue(_EncodeBuffer(key))))));
        ++count;
      }

      for (Map<String,List<WriteRequest>> reqs: all) {
        while (reqs != null && reqs.size() > 0) {
          BatchWriteItemResult res = _dynamoClient.client().batchWriteItem(new BatchWriteItemRequest().withRequestItems(reqs));
          reqs = res.getUnprocessedItems();
        }
      }

    } catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
  }

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer expectedValue, TransactionHandle txh) {
		DynamoDBTransaction lt = (DynamoDBTransaction)txh;
		if (lt.isMutationStarted()) {
			throw new GraphStorageException("Attempted to obtain a lock after one or more mutations");
		}

		lt.writeBlindLockClaim(_internals, key, expectedValue);
	}

  private static String _EncodeBuffer(ByteBuffer buf) {
    if (buf == null || buf.limit() == 0) {
      return "~";
    }

    return Base64.encodeBase64URLSafeString(Arrays.copyOf(buf.array(), buf.limit()));
  }

  private static ByteBuffer _DecodeString(String s) {
    if (s == "~") {
      return _EmptyBuffer;
    }

    byte[] raw = Base64.decodeBase64(s);
    ByteBuffer buf = ByteBuffer.allocate(raw.length);
    buf.put(raw).rewind();
    return buf;
  }

}
