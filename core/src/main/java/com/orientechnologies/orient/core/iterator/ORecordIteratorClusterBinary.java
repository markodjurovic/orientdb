/*
 * Copyright 2018 OrientDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSecurityException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OResultBinary;
import com.orientechnologies.orient.core.storage.OCluster;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;


/**
 *
 * @author mdjurovi
 */
public class ORecordIteratorClusterBinary implements Iterator<OResultBinary>{
  private OResultBinary currentRecord;
  protected final ODatabaseDocumentInternal database;
  protected final ORecordId current = new ORecordId();
  private final ODatabaseDocumentInternal lowLevelDatabase;
  private final OStorage                  dbStorage;
  private final boolean                   iterateThroughTombstones;
  protected boolean                   liveUpdated     = false;
  protected long                      limit           = -1;
  protected long                      browsedRecords  = 0;
  protected OStorage.LOCKING_STRATEGY lockingStrategy = OStorage.LOCKING_STRATEGY.NONE;
  protected long                   totalAvailableRecords;
  protected List<ORecordOperation> txEntries;
  protected int  currentTxEntryPosition = -1;
  protected long firstClusterEntry      = 0;
  protected long lastClusterEntry       = Long.MAX_VALUE;
  private String fetchPlan;
  private ORecord reusedRecord = null;                          // DEFAULT = NOT
  // REUSE IT
  private Boolean directionForward;
  private long                currentEntry         = ORID.CLUSTER_POS_INVALID;
  private int                 currentEntryPosition = -1;
  private OPhysicalPosition[] positionsToProcess   = null;

  /**
   * Set of RIDs of records which were indicated as broken during cluster iteration.
   * Mainly used during JSON export/import procedure to fix links on broken records.
   */
  final Set<ORID> brokenRIDs = new HashSet<>();
  
  public ORecordIteratorClusterBinary(final ODatabaseDocumentInternal iDatabase, final ODatabaseDocumentInternal iLowLevelDatabase,
      final int iClusterId) {
    this(iDatabase, iLowLevelDatabase, iClusterId, ORID.CLUSTER_POS_INVALID, ORID.CLUSTER_POS_INVALID, false,
        OStorage.LOCKING_STRATEGY.DEFAULT);
  }

  public ORecordIteratorClusterBinary(final ODatabaseDocumentInternal iDatabase, final ODatabaseDocumentInternal iLowLevelDatabase,
      final int iClusterId, final long firstClusterEntry, final long lastClusterEntry) {
    this(iDatabase, iLowLevelDatabase, iClusterId, firstClusterEntry, lastClusterEntry, false, OStorage.LOCKING_STRATEGY.NONE);
  }  
  
  @Deprecated
  public ORecordIteratorClusterBinary(final ODatabaseDocumentInternal iDatabase, final ODatabaseDocumentInternal iLowLevelDatabase,
      final int iClusterId, final long firstClusterEntry, final long lastClusterEntry, final boolean iterateThroughTombstones,
      final OStorage.LOCKING_STRATEGY iLockingStrategy) {
   
    database = iDatabase;
    lowLevelDatabase = iLowLevelDatabase;
    this.iterateThroughTombstones = iterateThroughTombstones;
    lockingStrategy = iLockingStrategy;

    dbStorage = lowLevelDatabase.getStorage();
    current.setClusterPosition(ORID.CLUSTER_POS_INVALID); // DEFAULT = START FROM THE BEGIN

    if (iClusterId == ORID.CLUSTER_ID_INVALID)
      throw new IllegalArgumentException("The clusterId is invalid");

    checkForSystemClusters(iDatabase, new int[] { iClusterId });

    current.setClusterId(iClusterId);
    final long[] range = database.getStorage().getClusterDataRange(current.getClusterId());

    if (firstClusterEntry == ORID.CLUSTER_POS_INVALID)
      this.firstClusterEntry = range[0];
    else
      this.firstClusterEntry = firstClusterEntry > range[0] ? firstClusterEntry : range[0];

    if (lastClusterEntry == ORID.CLUSTER_POS_INVALID)
      this.lastClusterEntry = range[1];
    else
      this.lastClusterEntry = lastClusterEntry < range[1] ? lastClusterEntry : range[1];

    totalAvailableRecords = database.countClusterElements(current.getClusterId(), iterateThroughTombstones);

    txEntries = iDatabase.getTransaction().getNewRecordEntriesByClusterIds(new int[] { iClusterId });

    if (txEntries != null)
      // ADJUST TOTAL ELEMENT BASED ON CURRENT TRANSACTION'S ENTRIES
      for (ORecordOperation entry : txEntries) {
        switch (entry.type) {
        case ORecordOperation.CREATED:
          totalAvailableRecords++;
          break;

        case ORecordOperation.DELETED:
          totalAvailableRecords--;
          break;
        }
      }

    begin();
  }
  
  public long getCurrentEntry() {
    return currentEntry;
  }
  
  protected ORecord getRecord() {
    final ORecord record;
    if (reusedRecord != null) {
      // REUSE THE SAME RECORD AFTER HAVING RESETTED IT
      record = reusedRecord;
      record.reset();
    } else
      record = null;
    return record;
  }
  
  protected void checkDirection(final boolean iForward) {
    if (directionForward == null)
      // SET THE DIRECTION
      directionForward = iForward;
    else if (directionForward != iForward)
      throw new OIterationException("Iterator cannot change direction while browsing");
  }
  
  protected void checkForSystemClusters(final ODatabaseDocumentInternal iDatabase, final int[] iClusterIds) {
    for (int clId : iClusterIds) {
      final OCluster cl = iDatabase.getStorage().getClusterById(clId);
      if (cl != null && cl.isSystemCluster()) {
        final OSecurityUser dbUser = iDatabase.getUser();
        if (dbUser == null || dbUser.allow(ORule.ResourceGeneric.SYSTEM_CLUSTERS, null, ORole.PERMISSION_READ) != null)
          // AUTHORIZED
          break;
      }
    }
  }
  
  public boolean hasPrevious() {
    checkDirection(false);

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    boolean thereAreRecordsToBrowse = getCurrentEntry() > firstClusterEntry;

    if (thereAreRecordsToBrowse) {
      ORecord record = getRecord();
      currentRecord = readCurrentRecord(record, -1);
    }

    return currentRecord != null;
  }

  @Override
  public boolean hasNext() {
    checkDirection(true);

    if (Thread.interrupted())
      // INTERRUPTED
      return false;

    updateRangesOnLiveUpdate();

    if (currentRecord != null) {
      return true;
    }

    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return false;

    if (browsedRecords >= totalAvailableRecords)
      return false;

    if (!(current.getClusterPosition() < ORID.CLUSTER_POS_INVALID) && getCurrentEntry() < lastClusterEntry) {
      ORecord record = getRecord();
      try {
        currentRecord = readCurrentRecord(record, +1);
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during read of record", e);

        final ORID recordRid = record == null ? null : record.getIdentity();

        if (recordRid != null)
          brokenRIDs.add(recordRid.copy());

        currentRecord = null;
      }

      if (currentRecord != null)
        return true;
    }

    // CHECK IN TX IF ANY
    if (txEntries != null)
      return txEntries.size() - (currentTxEntryPosition + 1) > 0;

    return false;
  }

  /**
   * Return the element at the current position and move backward the cursor to the previous position available.
   *
   * @return the previous record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
   */
  @SuppressWarnings("unchecked")  
  public OResultBinary previous() {
    checkDirection(false);

    if (currentRecord != null) {
      try {
        return currentRecord;
      } finally {
        currentRecord = null;
      }
    }
    // ITERATE UNTIL THE PREVIOUS GOOD RECORD
    while (hasPrevious()) {
      try {
        return currentRecord;
      } finally {
        currentRecord = null;
      }
    }

    return null;
  }

  protected OResultBinary getTransactionEntry() {
    boolean noPhysicalRecordToBrowse;

    if (current.getClusterPosition() < ORID.CLUSTER_POS_INVALID)
      noPhysicalRecordToBrowse = true;
    else if (directionForward)
      noPhysicalRecordToBrowse = lastClusterEntry <= currentEntry;
    else
      noPhysicalRecordToBrowse = currentEntry <= firstClusterEntry;

    if (!noPhysicalRecordToBrowse && positionsToProcess.length == 0)
      noPhysicalRecordToBrowse = true;

    if (noPhysicalRecordToBrowse && txEntries != null) {
      // IN TX
      currentTxEntryPosition++;
      if (currentTxEntryPosition >= txEntries.size())
        throw new NoSuchElementException();
      else{
        //TODO maybe something better, or to transactions store byte[]
        ORecord rec = txEntries.get(currentTxEntryPosition).getRecord();
        ORecordSerializerBinary serializer = new ORecordSerializerBinary();
        byte[] stream = serializer.toStream(rec, false);
        return new OResultBinary(stream, 0, stream.length, serializer.getCurrentVersion());        
      }
    }
    return null;
  }
  
  /**
   * Return the element at the current position and move forward the cursor to the next position available.
   *
   * @return the next record found, otherwise the NoSuchElementException exception is thrown when no more records are found.
   */
  @SuppressWarnings("unchecked")
  @Override
  public OResultBinary next() {
    checkDirection(true);

    OResultBinary record;

    // ITERATE UNTIL THE NEXT GOOD RECORD
    while (hasNext()) {
      // FOUND
      if (currentRecord != null) {
        try {
          return currentRecord;
        } finally {
          currentRecord = null;
        }
      }

      record = getTransactionEntry();
      if (record != null)
        return record;
    }

    return null;
  }

  /**
   * Move the iterator to the begin of the range. If no range was specified move to the first record of the cluster.
   *
   * @return The object itself
   */  
  public ORecordIteratorClusterBinary begin() {
    browsedRecords = 0;

    updateRangesOnLiveUpdate();
    resetCurrentPosition();

    currentRecord = readCurrentRecord(getRecord(), +1);

    return this;
  }

  public boolean isIterateThroughTombstones() {
    return iterateThroughTombstones;
  }
  
  /**
   * Move the iterator to the end of the range. If no range was specified move to the last record of the cluster.
   *
   * @return The object itself
   */  
  public ORecordIteratorClusterBinary last() {
    browsedRecords = 0;

    updateRangesOnLiveUpdate();
    resetCurrentPosition();

    currentRecord = readCurrentRecord(getRecord(), -1);

    return this;
  }

  /**
   * Tell to the iterator that the upper limit must be checked at every cycle. Useful when concurrent deletes or additions change
   * the size of the cluster while you're browsing it. Default is false.
   *
   * @param iLiveUpdated
   *          True to activate it, otherwise false (default)
   * @see #isLiveUpdated()
   */  
  public ORecordIteratorClusterBinary setLiveUpdated(boolean iLiveUpdated) {
    this.liveUpdated = iLiveUpdated;    

    // SET THE RANGE LIMITS
    if (iLiveUpdated) {
      firstClusterEntry = 0L;
      lastClusterEntry = Long.MAX_VALUE;
    } else {
      final long[] range = database.getStorage().getClusterDataRange(current.getClusterId());
      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }

    totalAvailableRecords = database.countClusterElements(current.getClusterId(), isIterateThroughTombstones());

    return this;
  }

  private void updateRangesOnLiveUpdate() {
    if (liveUpdated) {
      final long[] range = database.getStorage().getClusterDataRange(current.getClusterId());

      firstClusterEntry = range[0];
      lastClusterEntry = range[1];
    }
  }
  
  protected boolean nextPosition() {
    if (positionsToProcess == null) {
      positionsToProcess = dbStorage.ceilingPhysicalPositions(current.getClusterId(), new OPhysicalPosition(firstClusterEntry));
      if (positionsToProcess == null)
        return false;
    } else {
      if (currentEntry >= lastClusterEntry)
        return false;
    }

    incrementEntreePosition();
    while (positionsToProcess.length > 0 && currentEntryPosition >= positionsToProcess.length) {
      positionsToProcess = dbStorage
          .higherPhysicalPositions(current.getClusterId(), positionsToProcess[positionsToProcess.length - 1]);

      currentEntryPosition = -1;
      incrementEntreePosition();
    }

    if (positionsToProcess.length == 0)
      return false;

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry > lastClusterEntry || currentEntry == ORID.CLUSTER_POS_INVALID)
      return false;

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected boolean checkCurrentPosition() {
    if (currentEntry == ORID.CLUSTER_POS_INVALID || firstClusterEntry > currentEntry || lastClusterEntry < currentEntry)
      return false;

    current.setClusterPosition(currentEntry);
    return true;
  }
  
  protected boolean prevPosition() {
    if (positionsToProcess == null) {
      positionsToProcess = dbStorage.floorPhysicalPositions(current.getClusterId(), new OPhysicalPosition(lastClusterEntry));
      if (positionsToProcess == null)
        return false;

      if (positionsToProcess.length == 0)
        return false;

      currentEntryPosition = positionsToProcess.length;
    } else {
      if (currentEntry < firstClusterEntry)
        return false;
    }

    decrementEntreePosition();

    while (positionsToProcess.length > 0 && currentEntryPosition < 0) {
      positionsToProcess = dbStorage.lowerPhysicalPositions(current.getClusterId(), positionsToProcess[0]);
      currentEntryPosition = positionsToProcess.length;

      decrementEntreePosition();
    }

    if (positionsToProcess.length == 0)
      return false;

    currentEntry = positionsToProcess[currentEntryPosition].clusterPosition;

    if (currentEntry < firstClusterEntry)
      return false;

    current.setClusterPosition(currentEntry);
    return true;
  }

  protected void resetCurrentPosition() {
    currentEntry = ORID.CLUSTER_POS_INVALID;
    positionsToProcess = null;
    currentEntryPosition = -1;
  }
  
  private void decrementEntreePosition() {
    if (positionsToProcess.length > 0)
      if (iterateThroughTombstones)
        currentEntryPosition--;
      else
        do {
          currentEntryPosition--;
        } while (currentEntryPosition >= 0 && ORecordVersionHelper
            .isTombstone(positionsToProcess[currentEntryPosition].recordVersion));
  }

  private void incrementEntreePosition() {
    if (positionsToProcess.length > 0)
      if (iterateThroughTombstones)
        currentEntryPosition++;
      else
        do {
          currentEntryPosition++;
        } while (currentEntryPosition < positionsToProcess.length && ORecordVersionHelper
            .isTombstone(positionsToProcess[currentEntryPosition].recordVersion));
  }
  
  protected OResultBinary readCurrentRecord(ORecord iRecord, final int iMovement) {
    if (limit > -1 && browsedRecords >= limit)
      // LIMIT REACHED
      return null;

    do {
      final boolean moveResult;
      switch (iMovement) {
      case 1:
        moveResult = nextPosition();
        break;
      case -1:
        moveResult = prevPosition();
        break;
      case 0:
        moveResult = checkCurrentPosition();
        break;
      default:
        throw new IllegalStateException("Invalid movement value : " + iMovement);
      }

      if (!moveResult)
        return null;

      OResultBinary res = null;
      try {        
        if (iRecord != null) {
          ORecordInternal.setIdentity(iRecord, new ORecordId(current.getClusterId(), current.getClusterPosition()));
          res = lowLevelDatabase.loadBInary(iRecord, fetchPlan, false, true, iterateThroughTombstones, lockingStrategy);
        } else
          res = lowLevelDatabase.loadBInary(current, fetchPlan, false, true, iterateThroughTombstones, lockingStrategy);
      } catch (ODatabaseException e) {
        if (Thread.interrupted() || lowLevelDatabase.isClosed())
          // THREAD INTERRUPTED: RETURN
          throw e;

        if (e.getCause() instanceof OSecurityException)
          throw e;

        brokenRIDs.add(current.copy());

        OLogManager.instance().error(this, "Error on fetching record during browsing. The record has been skipped", e);
      }

      if (iRecord != null) {
        browsedRecords++;
        return res;
      }
    } while (iMovement != 0);

    return null;
  }    
  
}
