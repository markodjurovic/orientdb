/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ORecordCacheWeakRefs extends OAbstractMapCache<ORecord, WeakHashMap<ORID, WeakReference<ORecord>>> {

  public ORecordCacheWeakRefs() {
    super(new WeakHashMap<ORID, WeakReference<ORecord>>());
  }

  @Override
  public ORecord get(final ORID rid) {
    if (!isEnabled())
      return null;

    final WeakReference<ORecord> value;
    value = cache.get(rid);
    return get(value);
  }

  @Override
  public ORecord put(final ORecord record) {
    if (!isEnabled())
      return null;
    final WeakReference<ORecord> value;
    value = cache.put(record.getIdValue(), new WeakReference<ORecord>(record));
//    Orient.instance().getProfiler().updateCounter("INSERT_INTO_CACHE", "count of into cache insertions", 1);
    return get(value);
  }

  @Override
  public ORecord remove(final ORID rid) {
    if (!isEnabled())
      return null;
    final WeakReference<ORecord> value;
    value = cache.remove(rid);
    return get(value);
  }

  private ORecord get(WeakReference<ORecord> value) {
    if (value == null)
      return null;
    else
      return value.get();
  }

  @Override
  public void shutdown() {
    cache = new WeakHashMap<>();
  }

  @Override
  public void clear() {
    cache.clear();
  }
}
