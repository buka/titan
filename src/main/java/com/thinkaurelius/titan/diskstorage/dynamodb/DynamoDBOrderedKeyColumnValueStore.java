package com.thinkaurelius.titan.diskstorage.dynamodb;


import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.codec.binary.Base64;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.SimpleLockConfig;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.writeaggregation.MultiWriteKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.writeaggregation.Mutation;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.AmazonDynamoDBAsyncClient;

/**
 * Experimental AWS DynamoDB backing store.
 *
 *  A few notes:
 *    - This is pretty much just a best-guess implementation of a datastore by looking at the HBase & Cassandra codes.
 *    - DynamoDB is not columnar and the others are.  Rather than use one table, this driver uses one per column-family.
 *      So you specify a 'table prefix' in the configuration and it uses that.  Keyed elements are Dynamo items.
 *      The key data itself is stored in a special '_titan_key' field which is also the primary index.
 *      Columns are mapped to additional fields in the items.  It's pretty important to recognize that DynamoDB items
 *      are constrained to 64k each.  I have no idea if this will become an issue but I certainly suspect it would.
 *      That said, Cassandra columns are limited to 2GB so there is that too...
 *    - DynamoDB uses strings for fields.  The API is ByteBuffer-oriented so everything is Base64-encoded which is a
 *      little cheaper than Hex.  Empty values are mapped to the '~' token in DynamoDB which doesn't like empty/null
 *      attributes.  These are remapped to the empty buffer in the driver.
 *    - For this kind of thing, I'd prefer to use the asynchronous AWS client, and one will notice that
 *      (a) I actually am but only activate the synchronous API and (b) there are configurations to tune it.
 *      If I might, I think that this API and the Titan code above it, ought to give consideration to an asynchronous
 *      futures-based design.
 *    - GetSlice grabs all fields of the specified item since I don't know enough about how the rules around
 *      what columns are passed to this call. It then sorts are filters those lexigraphically outside the
 *      desired range (if applicable).
 *    - MutateMany is implemented as a loop over mutate() since DynamoDB doesn't (yet?) provide a multiple item
 *      batch command for attribute updates.  Also the loop is something that can and will be parallelized by using
 *      the async API and blocking on all futures before returning.  Again, would be great knowing more about the
 *      design and constraints and expectations regarding timing and consistency...
 *    - DynamoDB is a little funny... tables take a while to reach 'created' states. Low lock times can lead to
 *      unnecessary timeouts in the Titan code too.
 *    - Did I mention this is an experiment and shouldn't be used?
 *
 * @author Garrick Evans <garrick.evans@autodesk.com>
 */
public class DynamoDBOrderedKeyColumnValueStore implements
		OrderedKeyColumnValueStore,
		MultiWriteKeyColumnValueStore {

  private static final Logger _logger = LoggerFactory.getLogger(DynamoDBOrderedKeyColumnValueStore.class);
  private static final ByteBuffer _empty = ByteBuffer.allocate(0);
  private final EntryColumnComparator _columnCompare = new EntryColumnComparator();
  private final ArrayList<String> _EmptyList = null;


  private final String 					_prefix;
	private final String 					_family;
	private final String 					_table;
	private final LockConfig 			_internals;
	private final DynamoDBClient 	_dynamoClient;
	private final boolean					_forceConsistentRead;
	private final long						_futuresTimeout;
  //private final long            _readCapacity;
  private final long            _writeCapacity;
  private DataWindow            _writeWindow;

	DynamoDBOrderedKeyColumnValueStore(String tablePrefix,
																		 String tableName,
																		 DynamoDBClient dynamoClient,
																		 OrderedKeyColumnValueStore lockStore,
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
    _writeWindow = new DataWindow(_writeCapacity * 1024);

		if (null != llm && null != lockStore) {
			_internals = new SimpleLockConfig(this, lockStore, llm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
		} else {
			_internals = null;
		}
	}

	@Override
	public void close() throws GraphStorageException {
	}

	@Override
	public ByteBuffer get(ByteBuffer key, ByteBuffer column, TransactionHandle txh) {

		try {

      String dynKey = _EncodeBuffer(key);
      String dynAttr = _EncodeBuffer(column);

      _logger.debug("get key {} column {} ", new Object[]{dynKey, dynAttr});


			GetItemResult res = _dynamoClient.client()
														.getItem(new GetItemRequest()
                                         	.withKey(new Key(new AttributeValue(dynKey)))
                                          .withTableName(_table)
                                          .withAttributesToGet(dynAttr)
                                          .withConsistentRead(_forceConsistentRead));

			Map<String,AttributeValue> item = res.getItem();
			if (item == null) {
				_logger.debug("Item does not exist for key {}",dynKey);
				return null;
			}

			AttributeValue val = item.get(dynAttr);
			if (val == null) {
				_logger.debug("No data for key {} column {}", new Object[] {dynKey, dynAttr});
				return _empty;
			}

				// convert encoded attribute string to byte buffer
      _logger.debug("Get returning {}", val.getS());
			return _DecodeString(val.getS());
		}
    catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
	}

	@Override
	public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, TransactionHandle txh) {
		return (null != get(key, column, txh));
	}

	@Override
	public boolean isLocalKey(ByteBuffer key) {
		return false;
	}

	@Override
	public boolean containsKey(ByteBuffer key, TransactionHandle txh) {
		return (null != get(key, null, txh));
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, TransactionHandle txh) {
    try {
			String dynKey = _EncodeBuffer(key);

      _logger.debug("getSlice key {} start {} end {} limit {}",
              new Object[]{
                      dynKey,
                      _EncodeBuffer(columnStart),
                      _EncodeBuffer(columnEnd),
                      limit
              });

      // without knowing more about how the columns are constructed,
      // right now, we'll just pull all attributes and trim the others away...
      // TODO: definitely can improve this with more internal info

      _logger.debug("Getting item... consistent reads: {}",Boolean.toString(_forceConsistentRead));
			GetItemResult res = _dynamoClient.client()
														.getItem(new GetItemRequest()
                                         	.withKey(new Key(new AttributeValue(dynKey)))
                                          .withTableName(_table)
                                          .withAttributesToGet(_EmptyList)
                                          .withConsistentRead(_forceConsistentRead));

			Map<String,AttributeValue> item = res.getItem();
			if (item == null) {
				_logger.debug("Item does not exist for key {}",dynKey);
				return null;
			}

			_logger.debug("Got {} attributes for item {}",new Object[] {item.size(), dynKey});

			ArrayList<Entry> results = new ArrayList<Entry>(item.size());

			for (Map.Entry<String, AttributeValue> e : item.entrySet()) {
        String k = e.getKey();
        if (!k.equals("_titan_key")) {
          results.add(new Entry(_DecodeString(k), _DecodeString(e.getValue().getS())));
          _logger.debug("Got Item {} Field {} Value {}", new Object[] {dynKey, k, e.getValue().getS()});
        }
			}

        // lexigraphically sort the results...
      Collections.sort(results, _columnCompare);

      int iend = results.size();
      int istart = 0;

      if (columnEnd != null && columnEnd.capacity() > 0) {
        int x = results.indexOf(_EncodeBuffer(columnEnd));
        if (x >= 0) {
          iend = 1+x;
        }
      }
      if (columnStart != null && columnStart.capacity() > 0) {
        int x = results.indexOf(_EncodeBuffer(columnStart));
        if (x >= 0) {
          istart = x;
        }
      }

      List<Entry> slice = results.subList(istart, Math.min(iend, limit));

      String adds = new String();
      for (Entry e : slice) {
        adds += "col: "+_EncodeBuffer(e.getColumn()) + " val: " + _EncodeBuffer(e.getValue()) +", ";
      }
      _logger.debug("getSlice returning {}", adds);
      return slice;
		}
    catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
	}

	@Override
	public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, TransactionHandle txh) {
		return getSlice(key, columnStart, columnEnd, Integer.MAX_VALUE, txh);
	}

	@Override
	public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, TransactionHandle txh) {
    Map<ByteBuffer, Mutation> map = new HashMap<ByteBuffer, Mutation>(1);
    map.put(key, new Mutation(additions, deletions));

    mutateMany(map, txh);
	}

	@Override
	public void mutateMany(Map<ByteBuffer, Mutation> mutations, TransactionHandle txh) {
    List<Future<?>> futures = _mutate(mutations, txh);

      // would kill for composition here...
    for (Future<?> f: futures) {
      try {
        f.get(_futuresTimeout, TimeUnit.MILLISECONDS);
      }
      catch (Exception ex) {
        throw new GraphStorageException(ex);
      }
    }
	}

  private List<Future<?>> _mutate(Map<ByteBuffer, Mutation> mutations, TransactionHandle txh) {

    // null txh means a LockingTransaction is calling this method
    if (null != txh) {
        // non-null txh -> make sure locks are valid
      DynamoDBTransaction lt = (DynamoDBTransaction)txh;
      if (! lt.isMutationStarted()) {
          // This is the first mutate call in the transaction
        lt.mutationStarted();
          // Verify all blind lock claims now
        lt.verifyAllLockClaims(); // throws GSE and unlocks everything on any lock failure
      }
    }

    try {
      ArrayList<Future<?>> futures = new ArrayList<Future<?>>(mutations.size());

      for (Map.Entry<ByteBuffer, Mutation> mutant : mutations.entrySet()) {
        String dynKey = _EncodeBuffer(mutant.getKey());
        Mutation mutation = mutant.getValue();

        UpdateItemRequest req = new UpdateItemRequest().withTableName(_table).withKey(new Key(new AttributeValue(dynKey)));
        Map<String, AttributeValueUpdate> attrUpdates = new HashMap<String, AttributeValueUpdate>();

        List<Entry> additions = mutation.getAdditions();
        List<ByteBuffer> deletions = mutation.getDeletions();

        if (null != deletions) {
          for (ByteBuffer attr: deletions) {
            attrUpdates.put(_EncodeBuffer(attr), new AttributeValueUpdate().withAction(AttributeAction.DELETE));
          }
        }

        if (null != additions) {
          for (Entry e: additions) {
            attrUpdates.put(_EncodeBuffer(e.getColumn()),
                            new AttributeValueUpdate(new AttributeValue(_EncodeBuffer(e.getValue())), AttributeAction.PUT));
          }
        }

        _logger.debug("Mutating: {}",attrUpdates);
        
        //_writeWindow.push(req.toString().length());

        futures.add(_dynamoClient.client().updateItemAsync(req.withAttributeUpdates(attrUpdates)));
      }

      return futures;
    }
    catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
  }  

	@Override
	public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, TransactionHandle txh) {
		DynamoDBTransaction lt = (DynamoDBTransaction)txh;
		if (lt.isMutationStarted()) {
			throw new GraphStorageException("Attempted to obtain a lock after one or more mutations");
		}

		lt.writeBlindLockClaim(_internals, key, column, expectedValue);
	}

  private static String _EncodeBuffer(ByteBuffer buf) {
    if (buf == null || buf.limit() == 0) {
      return "~";
    }

    return Base64.encodeBase64URLSafeString(Arrays.copyOf(buf.array(), buf.limit()));
  }

  private static ByteBuffer _DecodeString(String s) {
    if (s == "~") {
      return _empty;
    }

    byte[] raw = Base64.decodeBase64(s);
    ByteBuffer buf = ByteBuffer.allocate(raw.length);
    buf.put(raw).rewind();
    return buf;
  }

  private final class EntryColumnComparator implements Comparator<Entry> {
    @Override
    public boolean equals(Object that) {
      return this.equals(that);
    }

    public int compare(Entry left, Entry right) {
      return _EncodeBuffer(left.getColumn()).compareTo(_EncodeBuffer((right.getColumn())));
    }
  }

  private final class DataWindow {
    private final long _limit;
    private final long _rate = 1500L;
    private TreeMap<Long, Integer> _table = new TreeMap<Long, Integer>();

    private final Logger _logger = LoggerFactory.getLogger(DynamoDBOrderedKeyColumnValueStore.class);

    public DataWindow(long limit) {
      _limit = limit;
    }

    public void push(int bytes) {
        // as capacity units
      bytes = (1+(bytes/1024))*1024;

      long now = System.currentTimeMillis();

      _table.put(now, bytes);

      SortedMap<Long, Integer> stale = _table.headMap(now - _rate);
      SortedMap<Long, Integer> live = _table.tailMap(now - _rate, true);

      int payload = 0;
      for (int v: live.values()) {
        payload += v;
      }

      while (payload >= _limit) {
        long delay = System.currentTimeMillis() - (live.firstKey());
        _logger.warn("Attempt to write {} bytes/sec exceeds capacity limits - throttling back for {} ms.", new Object[]{payload, delay});
        try {
          Thread.sleep(delay);
        }
        catch (Exception ex) {
          _logger.error("Trapped exception from thread sleep: {}", ex.getMessage());
        }

        live.remove(live.firstKey());

        payload = 0;
        for (int v: live.values()) {
          payload += v;
        }
      }

      _table = new TreeMap<Long, Integer>(live);
    }
  }
}
