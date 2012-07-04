package com.thinkaurelius.titan.diskstorage.dynamodb;

import com.thinkaurelius.titan.diskstorage.locking.LockingTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link LockingTransaction}; however, it creates a transaction type specific
 * to DynamoDB, which lets us check for user errors like passing a Cassandra
 * transaction into a DynamoDB method.
 */
public class DynamoDBTransaction extends LockingTransaction {

  public DynamoDBTransaction() {
    super();
  }

}
