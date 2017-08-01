package org.apache.hadoop.hive.ql.parse.repl.dump.io;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CopyUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CopyUtils.class);

  private final HiveConf hiveConf;
  private final long maxCopyFileSize;
  private final long maxNumberOfFiles;
  private final boolean hiveInTest;
  private final String copyAsUser;

  CopyUtils(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    maxNumberOfFiles = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXNUMFILES);
    maxCopyFileSize = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE);
    hiveInTest = hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_IN_TEST);
    this.copyAsUser = hiveConf.getVar(HiveConf.ConfVars.HIVE_DISTCP_DOAS_USER);
  }

  void doCopy(Path destination, List<Path> srcPaths) throws IOException {
    Map<FileSystem, List<Path>> map = fsToFileMap(srcPaths);
    FileSystem destinationFs = destination.getFileSystem(hiveConf);

    for (Map.Entry<FileSystem, List<Path>> entry : map.entrySet()) {
      if (regularCopy(destinationFs, entry)) {
        Path[] paths = entry.getValue().toArray(new Path[] {});
        FileUtil.copy(entry.getKey(), paths, destinationFs, destination, false, true, hiveConf);
      } else {
        FileUtils.distCp(
            entry.getKey(),   // source file system
            entry.getValue(), // list of source paths
            destination,
            false,
            copyAsUser,
            hiveConf,
            ShimLoader.getHadoopShims()
        );
      }
    }
  }

  /*
      Check for conditions that will lead to local copy, checks are:
      1. we are testing hive.
      2. both source and destination are same FileSystem
      3. either source or destination is a "local" FileSystem("file")
      4. aggregate fileSize of all source Paths(can be directory /  file) is less than configured size.
      5. number of files of all source Paths(can be directory /  file) is less than configured size.
  */
  private boolean regularCopy(FileSystem destinationFs, Map.Entry<FileSystem, List<Path>> entry)
      throws IOException {
    if (hiveInTest) {
      return true;
    }
    FileSystem sourceFs = entry.getKey();
    boolean isLocalFs = isLocal(sourceFs) || isLocal(destinationFs);
    boolean sameFs = sourceFs.equals(destinationFs);
    if (isLocalFs || sameFs) {
      return true;
    }

    /*
       we have reached the point where we are transferring files across fileSystems.
    */
    long size = 0;
    long numberOfFiles = 0;

    for (Path path : entry.getValue()) {
      ContentSummary contentSummary = sourceFs.getContentSummary(path);
      size += contentSummary.getLength();
      numberOfFiles += contentSummary.getFileCount();
      if (limitReachedForLocalCopy(size, numberOfFiles)) {
        return false;
      }
    }
    return true;
  }

  private boolean limitReachedForLocalCopy(long size, long numberOfFiles) {
    boolean result = size > maxCopyFileSize || numberOfFiles > maxNumberOfFiles;
    if (result) {
      LOG.info("Source is {} bytes. (MAX: {})", size, maxCopyFileSize);
      LOG.info("Source is {} files. (MAX: {})", numberOfFiles, maxNumberOfFiles);
      LOG.info("going to launch distributed copy (distcp) job.");
    }
    return result;
  }

  private boolean isLocal(FileSystem fs) {
    return fs.getScheme().equals("file");
  }

  private Map<FileSystem, List<Path>> fsToFileMap(List<Path> srcPaths) throws IOException {
    Map<FileSystem, List<Path>> result = new HashMap<>();
    for (Path path : srcPaths) {
      FileSystem fileSystem = path.getFileSystem(hiveConf);
      if (!result.containsKey(fileSystem)) {
        result.put(fileSystem, new ArrayList<>());
      }
      result.get(fileSystem).add(path);
    }
    return result;
  }
}
