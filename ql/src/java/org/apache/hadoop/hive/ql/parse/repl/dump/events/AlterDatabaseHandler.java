/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.repl.dump.events;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.AlterDatabaseMessage;
import org.apache.hadoop.hive.metastore.messaging.MessageFactory;
import org.apache.hadoop.hive.ql.parse.repl.DumpType;
import org.apache.hadoop.hive.ql.parse.repl.load.DumpMetaData;

class AlterDatabaseHandler extends AbstractEventHandler {

  AlterDatabaseHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} ALTER_DATABASE message : {}", fromEventId(), event.getMessage());
    DumpMetaData dmd = withinContext.createDmd(this);

    AlterDatabaseMessage alterDatabaseMessage =
        deserializer.getAlterDatabaseMessage(event.getMessage());
    Database dbObjBefore = alterDatabaseMessage.getDbObjBefore();
    dbObjBefore.setName(dbObjBefore.getName().toLowerCase());
    Database dbObjAfter = alterDatabaseMessage.getDbObjAfter();
    dbObjAfter.setName(dbObjAfter.getName().toLowerCase());
    dmd.setPayload(
        MessageFactory.getInstance().buildAlterDatabaseMessage(dbObjBefore, dbObjAfter).toString());
    dmd.write();
  }

  @Override
  public DumpType dumpType() {
    return DumpType.EVENT_ALTER_DATABASE;
  }
}
