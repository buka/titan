package com.thinkaurelius.titan.diskstorage.dynamodb;

import com.thinkaurelius.titan.core.GraphStorageException;
import com.amazonaws.services.dynamodb.model.*;

import java.io.IOException;

/**
 * @author Garrick Evans <garrick.evans@autodesk.com>
 */

public class DynamoDBHelper {

    public static void deleteAll(org.apache.commons.configuration.Configuration config) {
      try {
        DynamoDBClient dynamo = new DynamoDBClient(config);
        String prefix = config.getString(DynamoDBStorageManager.TABLE_PREFIX, DynamoDBStorageManager.TABLE_PREFIX_DEFAULT) + ".";

        java.util.List<String> tables = dynamo.client().listTables().getTableNames();
        for (String table : tables) {
          if (table.startsWith(prefix)) {
            dynamo.client().deleteTable(new DeleteTableRequest().withTableName(table));
          }
        }
      }
      catch (Exception ex) {
        throw new GraphStorageException(ex);
      }
    }
    
}
