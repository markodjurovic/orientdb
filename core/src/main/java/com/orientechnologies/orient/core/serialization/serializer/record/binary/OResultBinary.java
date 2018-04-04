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
package com.orientechnologies.orient.core.serialization.serializer.record.binary;

import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 *
 * @author mdjurovi
 */
public class OResultBinary implements OResult{

  private ORID id;
  private final byte[] bytes;
  private final int offset;
  private final int serializerVersion;
  private final int fieldLength;
  private final boolean embedded;
  
  private ODocument doc = null;
  
  public OResultBinary(byte[] bytes, int offset, int fieldLength, int serializerVersion, ORID rid,
          boolean embedded){
    this.bytes = bytes;
    this.serializerVersion = serializerVersion;
    this.offset = offset;
    this.fieldLength = fieldLength;
    this.id = rid;
    this.embedded = embedded;
  }

  public int getFieldLength() {
    return fieldLength;
  }

  public int getOffset() {
    return offset;
  }  
  
  public ORID getId() {
    return id;
  }

  public void setId(ORID id) {
    this.id = id;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getSerializerVersion() {
    return serializerVersion;
  }    
  
  @Override
  public <T> T getProperty(String name) {
    if (embedded)
      return ORecordSerializerBinary.INSTANCE.deserializeFieldFromEmbedded(bytes, offset, name, serializerVersion);
    else
      return ORecordSerializerBinary.INSTANCE.deserializeFieldFromRoot(bytes, name);
  }

  @Override
  public OElement getElementProperty(String name) {
    Object result = getProperty(name);
    
    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? (OElement) result : null;
  }

  @Override
  public OVertex getVertexProperty(String name) {
    OElement result = getElementProperty(name);
    
    return result instanceof OElement ? ((OElement) result).asVertex().orElse(null) : null;
  }

  @Override
  public OEdge getEdgeProperty(String name) {
    OElement result = getElementProperty(name);
    
    return result instanceof OElement ? ((OElement) result).asEdge().orElse(null) : null;
  }

  @Override
  public OBlob getBlobProperty(String name) {
    return null;
  }

  @Override
  public Set<String> getPropertyNames() {
    if (doc != null)
      return doc.getPropertyNames();
    
    String[] fields;
    if (embedded)
      fields = ORecordSerializerBinary.INSTANCE.getFieldNamesEmbedded(new ODocument(), bytes, offset, serializerVersion);
    else
      fields = ORecordSerializerBinary.INSTANCE.getFieldNamesRoot(new ODocument(), bytes);
    return new HashSet<>(Arrays.asList(fields));
  }

  @Override
  public Optional<ORID> getIdentity() {
    return Optional.of(id);
  }

  @Override
  public boolean isElement() {
    return true;
  }

  @Override
  public Optional<OElement> getElement() {
    return Optional.of(toDocument());
  }

  @Override
  public OElement toElement() {
    return toDocument();
  }

  @Override
  public boolean isBlob() {
    return false;
  }

  @Override
  public Optional<OBlob> getBlob() {
    return null;
  }

  @Override
  public Optional<ORecord> getRecord() {
    return Optional.of(toDocument());
  }

  @Override
  public boolean isProjection() {
    return false;
  }

  @Override
  public Object getMetadata(String key) {
    return null;
  }

  @Override
  public Set<String> getMetadataKeys() {
    return null;
  }

  @Override
  public boolean hasProperty(String varName) {
    //TODO make specialized method which will return on property name found
    if (doc != null){
      return doc.hasField(varName);
    }
    
    BytesContainer container = new BytesContainer(bytes, 1);
    return ORecordSerializerBinary.INSTANCE.getSerializer(bytes[0]).isContainField(container, varName, ORecordSerializerBinary.INSTANCE.getSerializer(bytes[0]).isSerializingClassNameByDefault());
  }
  
  private ODocument toDocument(){
    if (doc != null)
      return doc;
    
    doc = new ODocument(id);    
    doc.setInternalStatus(ORecordElement.STATUS.LOADED);
    ORecordSerializerBinary.INSTANCE.fromStream(bytes, doc, null);
    doc.setSource(bytes);
    return doc;
  }
  
}
