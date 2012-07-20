package com.thinkaurelius.titan.diskstorage.dynamodb;

import java.nio.ByteBuffer;
import com.thinkaurelius.titan.diskstorage.LockConfig;
import com.thinkaurelius.titan.diskstorage.LockingFailureException;
import com.thinkaurelius.titan.diskstorage.util.KeyValueStoreAdapter;
import com.thinkaurelius.titan.diskstorage.locking.LockingTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link LockingTransaction}; however, it creates a transaction type specific
 * to DynamoDB, which lets us check for user errors like passing a Cassandra
 * transaction into a DynamoDB method.
 */
public class DynamoDBTransaction extends LockingTransaction {

  private final int _keyLength = 0;

  public DynamoDBTransaction() {
    super();
  }

  public void blindClaim(LockConfig backer, ByteBuffer key, ByteBuffer expectedValue)
    throws LockingFailureException 
  {
    writeBlindLockClaim(backer, _getKey(key), _getColumn(key), expectedValue);
  }

  // ripped from KeyValueStoreAdapter

  protected final ByteBuffer _getKey(ByteBuffer concat) {
    ByteBuffer key = concat.duplicate();
    key.limit(key.position()+_getKeyLength(concat));
    return key;
  }

  protected final ByteBuffer _getColumn(ByteBuffer concat) {
    concat.position(_getKeyLength(concat));
    ByteBuffer column = concat.slice();
    if (_keyLength<=0) { //variable key length => remove length at end
      column.limit(column.limit()-KeyValueStoreAdapter.variableKeyLengthSize);
    }
    return column;
  }

  protected final int _getKeyLength(ByteBuffer concat) {
    int length = _keyLength;
    if (_keyLength<=0) { //variable key length
      length = concat.getShort(concat.limit()-KeyValueStoreAdapter.variableKeyLengthSize);
    }
    return length;
  }
}
