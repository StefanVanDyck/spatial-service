/**************************************************************************
 * Copyright (C) 2010 Atlas of Living Australia
 * All Rights Reserved.
 * <p>
 * The contents of this file are subject to the Mozilla Public
 * License Version 1.1 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of
 * the License at http://www.mozilla.org/MPL/
 * <p>
 * Software distributed under the License is distributed on an "AS
 * IS" basis, WITHOUT WARRANTY OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * rights and limitations under the License.
 ***************************************************************************/
package au.org.ala.spatial.intersect

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

/**
 * @author Adam
 */

import groovy.util.logging.Slf4j

@Slf4j
class SimpleShapeFileCache {
    private final ConcurrentMap<String, SimpleShapeFile> cache = new ConcurrentHashMap<>()

    /**
     * Get a cached SimpleShapeFile (by filename+field) or create and cache it if absent.
     */
    SimpleShapeFile get(String filename, String fieldName) {
        String key = "${filename}::${fieldName}"
        return cache.computeIfAbsent(key) { new SimpleShapeFile(filename, fieldName) }
    }

}
