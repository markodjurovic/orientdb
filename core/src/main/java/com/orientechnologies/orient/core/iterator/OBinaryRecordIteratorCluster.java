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
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.ORecordVersionHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OResultBinary;
import com.orientechnologies.orient.core.storage.OPhysicalPosition;
import com.orientechnologies.orient.core.storage.OStorage;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;


/**
 *
 * @author mdjurovi
 */
public class OBinaryRecordIteratorCluster implements Iterator<OResultBinary>{

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
  private final ORecord reusedRecord = null;                          // DEFAULT = NOT
  // REUSE IT
  private Boolean directionForward;
  private long                currentEntry         = ORID.CLUSTER_POS_INVALID;
  private int                 currentEntryPosition = -1;
  private OPhysicalPosition[] positionsToProcess   = null;
  
  final Set<ORID> brokenRIDs = new HashSet<>();
  
  public OBinaryRecordIteratorCluster(final ODatabaseDocumentInternal iDatabase, final ODatabaseDocumentInternal iLowLevelDatabase,
      final int iClusterId){
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.;
  }
  
   public OBinaryRecordIteratorCluster(final ODatabaseDocumentInternal iDatabase, final ODatabaseDocumentInternal iLowLevelDatabase,
      final int iClusterId, final long firstClusterEntry, final long lastClusterEntry) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.;
  }
  
  @Override
  public boolean hasNext() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  public boolean hasPrevious(){
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  @Override
  public OResultBinary next() {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  public OResultBinary previous(){
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
  public OResultBinary last(){
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
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
  
  protected boolean checkCurrentPosition() {
    if (currentEntry == ORID.CLUSTER_POS_INVALID || firstClusterEntry > currentEntry || lastClusterEntry < currentEntry)
      return false;

    current.setClusterPosition(currentEntry);
    return true;
  }
  
}
