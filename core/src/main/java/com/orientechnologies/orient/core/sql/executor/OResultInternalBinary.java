package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.*;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OResultBinary;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class OResultInternalBinary implements OResult {
  protected Map<String, Object> content = new LinkedHashMap<>();
  protected Map<String, Object> metadata;
  protected OResultBinary       element;

  public OResultInternalBinary() {
  }

  public OResultInternalBinary(OResultBinary ident) {
    this.element = ident;
  }

  public void setProperty(String name, Object value) {
    if (value instanceof Optional) {
      value = ((Optional) value).orElse(null);
    }
    if (value instanceof OResult && ((OResult) value).isElement()) {
      content.put(name, ((OResult) value).getElement().get());
    } else {
      content.put(name, value);
    }
  }

  public void removeProperty(String name) {
    content.remove(name);
  }

  @Override
  public <T> T getProperty(String name) {
    T result = null;
    if (content.containsKey(name)) {
      result = (T) wrap(content.get(name));
    } else if (element != null) {
      result = element.getProperty(name);
    }
    if (result instanceof OIdentifiable && ((OIdentifiable) result).getIdentity().isPersistent()) {
      result = (T) ((OIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public OElement getElementProperty(String name) {
    Object result = null;
    if (content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = element.getElementProperty(name);
    }

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
    Object result = null;
    if (content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = element.getVertexProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? ((OElement) result).asVertex().orElse(null) : null;
  }

  @Override
  public OEdge getEdgeProperty(String name) {
    Object result = null;
    if (content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = element.getEdgeProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OElement ? ((OElement) result).asEdge().orElse(null) : null;
  }

  @Override
  public OBlob getBlobProperty(String name) {
    Object result = null;
    if (content.containsKey(name)) {
      result = content.get(name);
    } else if (element != null) {
      result = element.getBlobProperty(name);
    }

    if (result instanceof OResult) {
      result = ((OResult) result).getRecord().orElse(null);
    }

    if (result instanceof ORID) {
      result = ((ORID) result).getRecord();
    }

    return result instanceof OBlob ? (OBlob) result : null;
  }

  private Object wrap(Object input) {
    if (input instanceof OElement && !((OElement) input).getIdentity().isValid()) {
      OResultInternal result = new OResultInternal();
      OElement elem = (OElement) input;
      for (String prop : elem.getPropertyNames()) {
        result.setProperty(prop, elem.getProperty(prop));
      }
      elem.getSchemaType().ifPresent(x -> result.setProperty("@class", x.getName()));
      return result;
    } else if (isEmbeddedList(input)) {
      return ((List) input).stream().map(this::wrap).collect(Collectors.toList());
    } else if (isEmbeddedSet(input)) {
      return ((Set) input).stream().map(this::wrap).collect(Collectors.toSet());
    } else if (isEmbeddedMap(input)) {
      Map result = new HashMap();
      for (Map.Entry<Object, Object> o : ((Map<Object, Object>) input).entrySet()) {
        result.put(o.getKey(), wrap(o.getValue()));
      }
      return result;
    }
    return input;
  }

  private boolean isEmbeddedSet(Object input) {
    if (input instanceof Set) {
      for (Object o : (Set) input) {
        if (o instanceof OElement && !((OElement) o).getIdentity().isPersistent()) {
          return true;
        }
        if (isEmbeddedList(o)) {
          return true;
        }
        if (isEmbeddedSet(o)) {
          return true;
        }
        if (isEmbeddedMap(o)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isEmbeddedMap(Object input) {
    if (input instanceof Map) {
      for (Object o : ((Map) input).values()) {
        if (o instanceof OElement && !((OElement) o).getIdentity().isPersistent()) {
          return true;
        }
        if (isEmbeddedList(o)) {
          return true;
        }
        if (isEmbeddedSet(o)) {
          return true;
        }
        if (isEmbeddedMap(o)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean isEmbeddedList(Object input) {
    if (input instanceof List) {
      for (Object o : (List) input) {
        if (o instanceof OElement && !((OElement) o).getIdentity().isPersistent()) {
          return true;
        }
        if (isEmbeddedList(o)) {
          return true;
        }
        if (isEmbeddedSet(o)) {
          return true;
        }
        if (isEmbeddedMap(o)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Set<String> getPropertyNames() {
    Set<String> result = new LinkedHashSet<>();
    if (element != null) {      
      result.addAll(element.getPropertyNames());
    }
    result.addAll(content.keySet());
    return result;
  }

  @Override
  public boolean hasProperty(String propName) {
    if (element != null && element.hasProperty(propName)) {
      return true;
    }
    return content.keySet().contains(propName);
  }

  @Override
  public boolean isElement() {
    return this.element != null;
  }

  @Override
  public Optional<OElement> getElement() {
    if (element == null) {
      return Optional.ofNullable((OElement) element);
    }
    
    return element.getElement();    
  }

  @Override
  public OElement toElement() {
    if (isElement()) {
      return getElement().get();
    }
    ODocument doc = new ODocument();
    for (String s : getPropertyNames()) {
      if (s == null) {
        continue;
      } else if (s.equalsIgnoreCase("@rid")) {
        Object newRid = getProperty(s);
        if (newRid instanceof OIdentifiable) {
          newRid = ((OIdentifiable) newRid).getIdentity();
        } else {
          continue;
        }
        ORecordId oldId = (ORecordId) doc.getIdentity();
        oldId.setClusterId(((ORID) newRid).getClusterId());
        oldId.setClusterPosition(((ORID) newRid).getClusterPosition());
      } else if (s.equalsIgnoreCase("@version")) {
        Object v = getProperty(s);
        if (v instanceof Number) {
          ORecordInternal.setVersion(doc, ((Number) v).intValue());
        } else {
          continue;
        }
      } else if (s.equalsIgnoreCase("@class")) {
        doc.setClassName(getProperty(s));
      } else {
        doc.setProperty(s, convertToElement(getProperty(s)));
      }
    }
    return doc;
  }

  @Override
  public Optional<ORID> getIdentity() {
    if (element != null) {
      return element.getIdentity();
    }
    return Optional.empty();
  }

  @Override
  public boolean isProjection() {
    return this.element == null;
  }

  @Override
  public Optional<ORecord> getRecord() {
    if (this.element == null) {
      return Optional.empty();
    }
    return element.getRecord();
  }

  @Override
  public boolean isBlob() {
    return element.isBlob();    
  }

  @Override
  public Optional<OBlob> getBlob() {
    if (isBlob()) {
      return element.getBlob();
    }
    return Optional.empty();
  }

  @Override
  public Object getMetadata(String key) {
    if (key == null) {
      return null;
    }
    return metadata == null ? null : metadata.get(key);
  }

  public void setMetadata(String key, Object value) {
    if (key == null) {
      return;
    }
    if (metadata == null) {
      metadata = new HashMap<>();
    }
    metadata.put(key, value);
  }

  public void clearMetadata() {
    metadata = null;
  }

  public void removeMetadata(String key) {
    if (key == null || metadata == null) {
      return;
    }
    metadata.remove(key);
  }

  public void addMetadata(Map<String, Object> values) {
    if (values == null) {
      return;
    }
    if (this.metadata == null) {
      this.metadata = new HashMap<>();
    }
    this.metadata.putAll(values);
  }

  @Override
  public Set<String> getMetadataKeys() {
    return metadata == null ? Collections.emptySet() : metadata.keySet();
  }

  private Object convertToElement(Object property) {
    if (property instanceof OResult) {
      return ((OResult) property).toElement();
    }
    if (property instanceof List) {
      return ((List) property).stream().map(x -> convertToElement(x)).collect(Collectors.toList());
    }

    if (property instanceof Set) {
      return ((Set) property).stream().map(x -> convertToElement(x)).collect(Collectors.toSet());
    }

    if (property instanceof Map) {
      Map<Object, Object> result = new HashMap<>();
      Map<Object, Object> prop = ((Map) property);
      for (Map.Entry<Object, Object> o : prop.entrySet()) {
        result.put(o.getKey(), convertToElement(o.getValue()));
      }
    }

    return property;
  }

  public void setElementBinary(OIdentifiable element) {
    this.element = element.getRecordBinary();    
  }
  
//  public void setElement(OIdentifiable element) {
//    this.element = element.getRecordBinary();    
//  }

  @Override
  public String toString() {
    if (element != null) {
      return element.toString();
    }
    return "{\n" + content.entrySet().stream().map(x -> x.getKey() + ": " + x.getValue()).reduce("", (a, b) -> a + b + "\n")
        + "}\n";
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof OResultInternal)) {
      return false;
    }
    OResultInternal resultObj = (OResultInternal) obj;
    if (element != null) {
      if (!resultObj.getElement().isPresent()) {
        return false;
      }
      return element.equals(resultObj.getElement().get());
    } else {
      if (resultObj.getElement().isPresent()) {
        return false;
      }
      return this.content.equals(resultObj.content);
    }
  }

  @Override
  public int hashCode() {
    if (element != null) {
      return element.hashCode();
    }
    return content.hashCode();
  }

  public void bindToCache(ODatabaseDocumentInternal db) {
    //TODO turn this on, and check what to do with cache
//    if (isRecord()) {
//      Optional<ORecord> rec = element.getRecord();
//      ORecord cached = db.getLocalCache().findRecord(rec.get().getIdentity());
//      if (cached != null) {
//        if (!cached.isDirty()) {
//          //TODO return subset of array
//          cached.fromStream(rec.toStream());
//        }
//        element = cached;
//      } else {
//        db.getLocalCache().updateRecord(rec);
//      }
//    }
  }
}
