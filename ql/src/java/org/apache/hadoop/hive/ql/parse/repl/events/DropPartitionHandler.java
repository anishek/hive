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
package org.apache.hadoop.hive.ql.parse.repl.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;

import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DUMPTYPE;
import static org.apache.hadoop.hive.ql.parse.ReplicationSemanticAnalyzer.DumpMetaData;

public class DropPartitionHandler extends AbstractHandler {

  DropPartitionHandler(NotificationEvent event) {
    super(event);
  }

  @Override
  public void handle(Context withinContext) throws Exception {
    LOG.info("Processing#{} DROP_PARTITION message : {}", fromEventId(), event.getMessage());
    DumpMetaData dmd = withinContext.createDmd(this);
    dmd.setPayload(event.getMessage());
    dmd.write();
  }

  @Override
  public DUMPTYPE dumpType() {
    return DUMPTYPE.EVENT_DROP_PARTITION;
  }
}
