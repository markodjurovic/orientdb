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

import com.orientechnologies.orient.core.db.record.OId;
import com.orientechnologies.orient.core.id.ORID;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 */
public class ORecordCacheWeakRefs<T extends OId> extends OAbstractMapCache<T, WeakHashMap<ORID, WeakReference<T>>> {

  public ORecordCacheWeakRefs() {
    super(new WeakHashMap<ORID, WeakReference<T>>());
  }

  @Override
  public T get(final ORID rid) {
    if (!isEnabled())
      return null;

    final WeakReference<T> value;
    value = cache.get(rid);
    return get(value);
  }

  @Override
  public T put(final T record) {
    if (!isEnabled())
      return null;
    final WeakReference<T> value;
    value = cache.put(record.getIdValue(), new WeakReference<T>(record));
    return get(value);
  }

  @Override
  public T remove(final ORID rid) {
    if (!isEnabled())
      return null;
    final WeakReference<T> value;
    value = cache.remove(rid);
    return get(value);
  }

  private T get(WeakReference<T> value) {
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
