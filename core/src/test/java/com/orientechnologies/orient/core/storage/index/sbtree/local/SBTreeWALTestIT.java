package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.assertj.core.api.Assertions;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/27/13
 */
public class SBTreeWALTestIT extends SBTreeTestIT {
  static {
    OGlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private String buildDirectory;

  private ODatabaseDocumentTx expectedDatabaseDocumentTx;

  private String actualStorageDir;
  private String expectedStorageDir;

  private OLocalPaginatedStorage actualStorage;
  private OWriteCache            actualWriteCache;

  private OReadCache     expectedReadCache;
  private OWriteCache    expectedWriteCache;
  private OWriteAheadLog expectedWAL;

  @Before
  public void before() throws IOException {
    buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/sbtreeWithWALTest";

    final File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    createExpectedSBTree();
    createActualSBTree();
  }

  @After
  @Override
  public void afterMethod() throws Exception {
    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.drop();

    expectedDatabaseDocumentTx.open("admin", "admin");
    expectedDatabaseDocumentTx.drop();

    final File actualStorage = new File(actualStorageDir);
    if (actualStorage.exists())
      Assert.assertTrue(actualStorage.delete());

    final File expectedStorage = new File(expectedStorageDir);
    if (expectedStorage.exists())
      Assert.assertTrue(expectedStorage.delete());

    final File buildDir = new File(buildDirectory);
    if (buildDir.exists())
      Assert.assertTrue(buildDir.delete());
  }

  private void createActualSBTree() throws IOException {
    actualStorageDir = buildDirectory + "/sbtreeWithWALTestActual";

    File actualStorageDirFile = new File(actualStorageDir);
    if (!actualStorageDirFile.exists())
      actualStorageDirFile.mkdirs();

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + actualStorageDir);
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    } else {
      databaseDocumentTx.create();
    }

    actualStorage = (OLocalPaginatedStorage) databaseDocumentTx.getStorage();
    ODiskWriteAheadLog writeAheadLog = (ODiskWriteAheadLog) actualStorage.getWALInstance();

    actualStorage.synch();
    writeAheadLog.addCutTillLimit(writeAheadLog.getFlushedLsn());

    OReadCache actualReadCache = ((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getReadCache();
    actualWriteCache = ((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getWriteCache();

    sbTree = new OSBTree<>("actualSBTree", ".sbt", ".nbt", actualStorage);
    sbTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false);
  }

  private void createExpectedSBTree() {
    expectedStorageDir = buildDirectory + "/sbtreeWithWALTestExpected";

    File expectedStorageDirFile = new File(expectedStorageDir);
    if (!expectedStorageDirFile.exists())
      expectedStorageDirFile.mkdirs();

    expectedDatabaseDocumentTx = new ODatabaseDocumentTx("plocal:" + expectedStorageDir);
    if (expectedDatabaseDocumentTx.exists()) {
      expectedDatabaseDocumentTx.open("admin", "admin");
      expectedDatabaseDocumentTx.drop();
    } else {
      expectedDatabaseDocumentTx.create();
    }

    OLocalPaginatedStorage expectedStorage = (OLocalPaginatedStorage) expectedDatabaseDocumentTx.getStorage();
    expectedReadCache = expectedStorage.getReadCache();
    expectedWriteCache = expectedStorage.getWriteCache();
    expectedWAL = expectedStorage.getWALInstance();
  }

  @Override
  @Test
  public void testKeyPut() throws Exception {
    super.testKeyPut();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyPutRandomUniform() throws Exception {
    super.testKeyPutRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    super.testKeyPutRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    super.testKeyDeleteRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyDeleteRandomGaussian() throws Exception {
    super.testKeyDeleteRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyDelete() throws Exception {
    super.testKeyDelete();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyAddDelete() throws Exception {
    super.testKeyAddDelete();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeFirstEmpty();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeLastEmpty();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    super.testAddKeyValuesAndRemoveFirstMiddleAndLastPages();

    assertFileRestoreFromWAL();
  }

  @Test
  @Ignore
  @Override
  public void testNullKeysInSBTree() {
    super.testNullKeysInSBTree();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesMajor() {
    super.testIterateEntriesMajor();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesMinor() {
    super.testIterateEntriesMinor();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesBetween() {
    super.testIterateEntriesBetween();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    long sbTreeFileId = actualWriteCache.fileIdByName(sbTree.getName() + ".sbt");
    String nativeSBTreeFileName = ((OWOWCache) actualWriteCache).nativeFileNameById(sbTreeFileId);

    OStorage storage = databaseDocumentTx.getStorage();
    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.close();
    storage.close(true, false);

    restoreDataFromWAL();

    long expectedSBTreeFileId = expectedWriteCache.fileIdByName("expectedSBTree.sbt");
    String expectedSBTreeNativeFileName = ((OWOWCache) expectedWriteCache).nativeFileNameById(expectedSBTreeFileId);

    expectedDatabaseDocumentTx.activateOnCurrentThread();
    expectedDatabaseDocumentTx.close();
    storage = expectedDatabaseDocumentTx.getStorage();
    storage.close(true, false);

    assertFileContentIsTheSame(expectedSBTreeNativeFileName, nativeSBTreeFileName);
  }

  private void restoreDataFromWAL() throws IOException {
    ODiskWriteAheadLog log = new ODiskWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, null, false, actualStorage,
        16 * OWALPage.PAGE_SIZE, 120);
    OLogSequenceNumber lsn = log.begin();

    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();

    boolean atomicChangeIsProcessed = false;
    while (lsn != null) {
      OWALRecord walRecord = log.read(lsn);
      if (walRecord instanceof OOperationUnitBodyRecord)
        atomicUnit.add(walRecord);

      if (!atomicChangeIsProcessed) {
        if (walRecord instanceof OAtomicUnitStartRecord)
          atomicChangeIsProcessed = true;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        atomicChangeIsProcessed = false;

        for (OWALRecord restoreRecord : atomicUnit) {
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord
              || restoreRecord instanceof ONonTxOperationPerformedWALRecord)
            continue;

          if (restoreRecord instanceof OFileCreatedWALRecord) {
            final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord) restoreRecord;
            final String fileName = fileCreatedCreatedRecord.getFileName().replace("actualSBTree", "expectedSBTree");

            if (!expectedWriteCache.exists(fileName))
              expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
          } else {
            final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

            final long fileId = updatePageRecord.getFileId();
            final long pageIndex = updatePageRecord.getPageIndex();

            OCacheEntry cacheEntry = expectedReadCache.loadForWrite(fileId, pageIndex, true, expectedWriteCache, 1, false);
            if (cacheEntry == null) {
              do {
                if (cacheEntry != null)
                  expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);

                cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache, false);
              } while (cacheEntry.getPageIndex() != pageIndex);
            }

            try {
              ODurablePage durablePage = new ODurablePage(cacheEntry);
              durablePage.restoreChanges(updatePageRecord.getChanges());
              durablePage.setLsn(new OLogSequenceNumber(0, 0));

              cacheEntry.markDirty();
            } finally {
              expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
            }
          }

        }
        atomicUnit.clear();
      } else {
        Assert.assertTrue("WAL record type is " + walRecord.getClass().getName(),
            walRecord instanceof OUpdatePageRecord || walRecord instanceof ONonTxOperationPerformedWALRecord
                || walRecord instanceof OFileCreatedWALRecord || walRecord instanceof OFuzzyCheckpointStartRecord
                || walRecord instanceof OFuzzyCheckpointEndRecord);
      }

      lsn = log.next(lsn);
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();
  }

  private void assertFileContentIsTheSame(String expectedBTreeFileName, String actualBTreeFileName) throws IOException {
    File expectedFile = new File(expectedStorageDir, expectedBTreeFileName);
    RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r");
    RandomAccessFile fileTwo = new RandomAccessFile(new File(actualStorageDir, actualBTreeFileName), "r");

    Assert.assertEquals(fileOne.length(), fileTwo.length());

    byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
    byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];

    fileOne.seek(OFileClassic.HEADER_SIZE);
    fileTwo.seek(OFileClassic.HEADER_SIZE);

    int bytesRead = fileOne.read(expectedContent);
    while (bytesRead >= 0) {
      fileTwo.readFully(actualContent, 0, bytesRead);

      Assertions.assertThat(Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.MAX_PAGE_SIZE_BYTES))
          .isEqualTo(Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.MAX_PAGE_SIZE_BYTES));
      expectedContent = new byte[OClusterPage.PAGE_SIZE];
      actualContent = new byte[OClusterPage.PAGE_SIZE];
      bytesRead = fileOne.read(expectedContent);
    }

    fileOne.close();
    fileTwo.close();
  }
}
