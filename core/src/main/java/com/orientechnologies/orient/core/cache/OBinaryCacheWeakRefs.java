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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OResultBinary;
import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 *
 * @author mdjurovi
 */
/*public class OBinaryCacheWeakRefs extends OAbstractMapCache<WeakHashMap<ORID, WeakReference<OResultBinary>>> implements OCache<OResultBinary>{

  public OBinaryCacheWeakRefs(){
    super(new WeakHashMap<ORID, WeakReference<OResultBinary>>());
  }
  
  private OResultBinary get(WeakReference<OResultBinary> value) {
    if (value == null)
      return null;
    else
      return value.get();
  }
  
  @Override
  public ORecord get(ORID rid) {
    if (!isEnabled())
      return null;
    
    final WeakReference<OResultBinary> value = cache.get(rid);    
    return get(value).getRecord().get();
  }

  @Override
  public ORecord put(ORecord record) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public ORecord remove(ORID id) {
    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
  }
  
}*/
