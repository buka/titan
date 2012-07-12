package com.thinkaurelius.titan.diskstorage.dynamodb;


import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.dynamodb.model.*;
import com.amazonaws.services.dynamodb.AmazonDynamoDBAsyncClient;

import com.thinkaurelius.titan.core.GraphStorageException;


final class DynamoDBClient {

  static final String FORCE_CONSISTENT_READ                    = "force-consistent-read";
  static final String FUTURES_TIMEOUT                          = "futures-timeout";
  static final String READ_THROUGHPUT                          = "read-throughput";
  static final String WRITE_THROUGHPUT                         = "write-throughput";
  static final String VERBOSE_LOGGING                          = "verbose-logging";
  static final String CREDENTIALS_KEY                          = "credentials-key";
  static final String CREDENTIALS_SECRET                       = "credentials-secret";
  static final String CLIENT_CONN_TIMEOUT                      = "connection-timeout";
  static final String CLIENT_MAX_CONN                          = "max-connections";
  static final String CLIENT_MAX_ERROR_RETRY                   = "max-error-retry";
  static final String CLIENT_PROXY_DOMAIN                      = "proxy-domain";
  static final String CLIENT_PROXY_WORKSTATION                 = "proxy-workstation";
  static final String CLIENT_PROXY_HOST                        = "proxy-host";
  static final String CLIENT_PROXY_PORT                        = "proxy-port";
  static final String CLIENT_PROXY_USERNAME                    = "proxy-username";
  static final String CLIENT_PROXY_PASSWORD                    = "proxy-password";
  static final String CLIENT_SOCKET_BUFFER_SEND_HINT           = "socket-buffer-send-hint";
  static final String CLIENT_SOCKET_BUFFER_RECV_HINT           = "socket-buffer-recv-hint";
  static final String CLIENT_SOCKET_TIMEOUT                    = "socket-timeout";
  static final String CLIENT_USER_AGENT                        = "user-agent";
  //static final String CLIENT_EXECUTOR_CORE_POOL_SIZE         = "executor-core-pool-size";
  //static final String CLIENT_EXECUTOR_MAX_POOL_SIZE          = "executor-max-pool-size";
  //static final String CLIENT_EXECUTOR_KEEP_ALIVE             = "executor-keep-alive";

  static final boolean FORCE_CONSISTENT_READ_DEFAULT           = false;
  static final boolean VERBOSE_LOGGING_DEFAULT                 = false;
  static final long    FUTURES_TIMEOUT_DEFAULT                 = 60000L;   // ms
  static final long    READ_THROUGHPUT_DEFAULT                 = 5L;
  static final long    WRITE_THROUGHPUT_DEFAULT                = 10L;
  static final int     CLIENT_CONN_TIMEOUT_DEFAULT             = 60000;    // ms
  static final int     CLIENT_SOCKET_BUFFER_SEND_HINT_DEFAULT  = 1048576;  // 1MB
  static final int     CLIENT_SOCKET_BUFFER_RECV_HINT_DEFAULT  = 1048576;  // 1MB
  //static final int    CLIENT_EXECUTOR_CORE_POOL_SIZE         = Runtime.getRuntime().availableProcessors() * 2;
  //static final int    CLIENT_EXECUTOR_MAX_POOL_SIZE          = CLIENT_EXECUTOR_CORE_POOL_SIZE * 4;
  //static final long   CLIENT_EXECUTOR_KEEP_ALIVE             = 60000;


  private static final Logger             _logger = LoggerFactory.getLogger(DynamoDBClient.class);

  private final boolean                   _forceConsistentRead;
  private final boolean                   _verbose;
  private final long                      _futuresTimeout;
  private final AmazonDynamoDBAsyncClient _dynamoClient;
  private final ExecutorService           _dynamoClientThreadPool;

  private long                            _readCap;
  private long                            _writeCap;

  DynamoDBClient(org.apache.commons.configuration.Configuration config) {

    BasicAWSCredentials credentials = new BasicAWSCredentials(config.getString(CREDENTIALS_KEY),
                                                              config.getString(CREDENTIALS_SECRET));

    ClientConfiguration clientConfig = new ClientConfiguration();
    clientConfig.withConnectionTimeout(config.getInt(CLIENT_CONN_TIMEOUT, CLIENT_CONN_TIMEOUT_DEFAULT))
                .withMaxConnections(config.getInt(CLIENT_MAX_CONN, ClientConfiguration.DEFAULT_MAX_CONNECTIONS))
                .withMaxErrorRetry(config.getInt(CLIENT_MAX_ERROR_RETRY, ClientConfiguration.DEFAULT_MAX_RETRIES))
                .withUserAgent(config.getString(CLIENT_USER_AGENT, ClientConfiguration.DEFAULT_USER_AGENT))
                .withSocketTimeout(config.getInt(CLIENT_SOCKET_TIMEOUT, ClientConfiguration.DEFAULT_SOCKET_TIMEOUT))
                .withSocketBufferSizeHints(config.getInt(CLIENT_SOCKET_BUFFER_SEND_HINT, CLIENT_SOCKET_BUFFER_SEND_HINT_DEFAULT),
                                           config.getInt(CLIENT_SOCKET_BUFFER_RECV_HINT, CLIENT_SOCKET_BUFFER_RECV_HINT_DEFAULT))
                .withProxyDomain(config.getString(CLIENT_PROXY_DOMAIN))
                .withProxyWorkstation(config.getString(CLIENT_PROXY_WORKSTATION))
                .withProxyHost(config.getString(CLIENT_PROXY_HOST))
                .withProxyPort(config.getInt(CLIENT_PROXY_PORT, 0))
                .withProxyUsername(config.getString(CLIENT_PROXY_USERNAME))
                .withProxyPassword(config.getString(CLIENT_PROXY_PASSWORD));

    _verbose = config.getBoolean(VERBOSE_LOGGING, VERBOSE_LOGGING_DEFAULT);
    _forceConsistentRead = config.getBoolean(FORCE_CONSISTENT_READ, FORCE_CONSISTENT_READ_DEFAULT);
    _futuresTimeout = config.getLong(FUTURES_TIMEOUT, FUTURES_TIMEOUT_DEFAULT);
    _readCap = config.getLong(READ_THROUGHPUT, READ_THROUGHPUT_DEFAULT);
    _writeCap = config.getLong(WRITE_THROUGHPUT, WRITE_THROUGHPUT_DEFAULT);

      // use this one for now, can build executors from config if really need be...
    _dynamoClientThreadPool = Executors.newCachedThreadPool();
    _dynamoClient = new AmazonDynamoDBAsyncClient(credentials, clientConfig, _dynamoClientThreadPool);
  }

  AmazonDynamoDBAsyncClient client() {
    return _dynamoClient;
  }

  boolean forceConsistentRead() {
    return _forceConsistentRead;
  }

  long futuresTimeoutMillis() {
    return _futuresTimeout;
  }

  long readCapacity() {
    return _readCap;
  }

  long writeCapacity() {
    return _writeCap;
  }

  boolean verbose() {
    return _verbose;
  }

  /**
   * Ensures the named table exists.
   *
   * If missing, will create the table; if exists, checks if being deleted.
   * @return true if the table was created, false if already exists
   */
  boolean ensureTable(String name)
    throws GraphStorageException {

    try {
        // request to create the table
        // note that we don't bother waiting for the table to be created
        // also note we just use the synchronous call here...
        // TODO: look into provisioned throughput param
      CreateTableResult res =
        _dynamoClient.createTable(new CreateTableRequest()
          .withTableName(name)
          .withKeySchema(new KeySchema()
            .withHashKeyElement(new KeySchemaElement()
              .withAttributeName("_titan_key")
              .withAttributeType("S")))
          .withProvisionedThroughput(new ProvisionedThroughput()
              .withReadCapacityUnits(_readCap)
              .withWriteCapacityUnits(_writeCap)));

      _logger.debug("Table {} created. STATE ({})", new Object[] {name, res.getTableDescription().getTableStatus()});

        // bozo time
      boolean ready = false;
      long ticks = 3000L;
      while (!ready) {
        try { Thread.sleep(ticks); } catch (Exception ex) {}
        String status = _dynamoClient.describeTable(new DescribeTableRequest().withTableName(name)).getTable().getTableStatus();
        if (status.equals("ACTIVE")) {
          ready = true;
        }
        else if (status.equals("DELETING")) {
          throw new GraphStorageException("Table {} is currently being deleted".format(name));
        }
        else {
          _logger.debug("Status: {}...",status);
        }
      }

      return true;
    }
      // ensure the table is not in a bad state...
    catch (ResourceInUseException ex) {
      try {
        TableDescription desc = _dynamoClient.describeTable(new DescribeTableRequest().withTableName(name)).getTable();
        String status = desc.getTableStatus();

        if (status.equals("DELETING")) {
          throw new GraphStorageException("Table {} is currently being deleted".format(name));
        }

        ProvisionedThroughputDescription prov = desc.getProvisionedThroughput();
        _readCap = prov.getReadCapacityUnits();
        _writeCap = prov.getWriteCapacityUnits();

        _logger.debug("Table {} already exists. Status ({}) Size ({} MB) Created ({})", new Object []{status, (desc.getTableSizeBytes()/1048576), desc.getCreationDateTime().toString()});
        return false;
      }
      catch (AmazonClientException ax) {
        throw new GraphStorageException(ax);
      }
    }
    catch (AmazonClientException ex) {
      throw new GraphStorageException(ex);
    }
  }

}
