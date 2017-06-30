/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.ql.exec.ReplCopyTask;
import org.apache.hadoop.hive.ql.parse.EximUtil;
import org.apache.hadoop.hive.ql.parse.ReplicationSpec;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class FileOperations {
  private static Logger logger = LoggerFactory.getLogger(FileOperations.class);
  private final Path dataFileListPath;
  private final Path exportRootDataDir;
  private HiveConf hiveConf;
  private final FileSystem dataFileSystem, exportFileSystem;

  public FileOperations(Path dataFileListPath, Path exportRootDataDir, HiveConf hiveConf)
      throws IOException {
    this.dataFileListPath = dataFileListPath;
    this.exportRootDataDir = exportRootDataDir;
    this.hiveConf = hiveConf;
    dataFileSystem = dataFileListPath.getFileSystem(hiveConf);
    exportFileSystem = exportRootDataDir.getFileSystem(hiveConf);
  }

  public void export(ReplicationSpec forReplicationSpec) throws IOException, SemanticException {
    if (forReplicationSpec.isLazy()) {
      exportFilesAsList();
    } else {
      copyFiles();
    }
  }

  /**
   * This writes the actual data in the exportRootDataDir from the source.
   */
  private void copyFiles() throws IOException {
    RemoteIterator<LocatedFileStatus> itr = dataFileSystem.listFiles(dataFileListPath, true);
    while (itr.hasNext()) {
      LocatedFileStatus fileStatus = itr.next();
      if (shouldExport(fileStatus)) {
        ReplCopyTask.doCopy(exportRootDataDir, exportFileSystem, fileStatus.getPath(), dataFileSystem, hiveConf);
      }
    }
  }

  /**
   * This needs the root data directory to which the data needs to be exported to.
   * The data export here is a list of files either in table/partition that are written to the _files
   * in the exportRootDataDir provided.
   */
  private void exportFilesAsList() throws SemanticException, IOException {
    try (BufferedWriter writer = writer()) {
      RemoteIterator<LocatedFileStatus> itr = dataFileSystem.listFiles(dataFileListPath, true);
      while (itr.hasNext()) {
        LocatedFileStatus fileStatus = itr.next();
        if (shouldExport(fileStatus)) {
          writer.write(encodedUri(fileStatus));
          writer.newLine();
        }
      }
    }
  }

  private boolean shouldExport(LocatedFileStatus fileStatus) {
    String name = fileStatus.getPath().getName();
    return !(fileStatus.isDirectory() || name.startsWith("_") || name.startsWith("."));
  }

  private BufferedWriter writer() throws IOException {
    Path exportToFile = new Path(exportRootDataDir, EximUtil.FILES_NAME);
    if (exportFileSystem.exists(exportToFile)) {
      throw new IllegalArgumentException(
          exportToFile.toString() + " already exists and cant export data from path(dir) "
              + dataFileListPath);
    }
    logger.debug("exporting data files in dir : " + dataFileListPath + " to " + exportToFile);
    return new BufferedWriter(
        new OutputStreamWriter(exportFileSystem.create(exportToFile))
    );
  }

  private String encodedUri(LocatedFileStatus fileStatus) throws IOException {
    Path currentDataFilePath = fileStatus.getPath();
    String checkSum = ReplChangeManager.checksumFor(currentDataFilePath, dataFileSystem);
    return ReplChangeManager.encodeFileUri(currentDataFilePath.toUri().toString(), checkSum);
  }
}
