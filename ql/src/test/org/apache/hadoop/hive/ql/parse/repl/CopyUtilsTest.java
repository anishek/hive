package org.apache.hadoop.hive.ql.parse.repl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class CopyUtilsTest {
  /*
  Distcp currently does not copy a single file in a distributed manner hence we dont care about
  the size of file, if there is only file, we dont want to launch distcp.
   */
  @Test
  public void distcpShouldNotBeCalledOnlyForOneFile() {
    HiveConf conf = new HiveConf();
    conf.setLongVar(HiveConf.ConfVars.HIVE_EXEC_COPYFILE_MAXSIZE, 1);
    CopyUtils copyUtils = new CopyUtils("", conf);
    long MB_128 = 128 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_128, 1L));
  }

  @Test
  public void distcpShouldNotBeCalledForSmallerFileSize() {
    HiveConf conf = new HiveConf();
    CopyUtils copyUtils = new CopyUtils("", conf);
    long MB_16 = 16 * 1024 * 1024;
    assertFalse(copyUtils.limitReachedForLocalCopy(MB_16, 100L));
  }
}