/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2012 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.undertow.server.handlers.file;

import static io.undertow.server.handlers.file.LimitedBufferSlicePool.PooledByteBuffer;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.xnio.BufferAllocator;


/**
 * @author Jason T. Greene
 */
public class DirectBufferCache {
    @SuppressWarnings("unchecked")
    private static final PooledByteBuffer[] EMPTY_BUFFERS = new PooledByteBuffer[0];

    private final LimitedBufferSlicePool pool;
    private final int max;
    private final int sliceSize;
    private final int segmentShift;
    private final Segment[] segments;

    public DirectBufferCache(int sliceSize, int max) {
        this(sliceSize, max, Runtime.getRuntime().availableProcessors());
    }

    public DirectBufferCache(int sliceSize, int max, int concurrency) {
        this.sliceSize = sliceSize;
        this.max = max;
        this.pool = new LimitedBufferSlicePool(BufferAllocator.DIRECT_BYTE_BUFFER_ALLOCATOR, sliceSize, max);
        int shift = 1;
        while (concurrency > (shift <<= 1)) {}
        segmentShift = 32 - shift;
        segments = new Segment[shift];
        for (int i = 0; i < segments.length; i++) {
            segments[i] = new Segment(shift);
        }
    }

    private static int hash(int h) {
        // Spread bits to regularize both segment and index locations,
        // using variant of single-word Wang/Jenkins hash.
        h += (h <<  15) ^ 0xffffcd7d;
        h ^= (h >>> 10);
        h += (h <<   3);
        h ^= (h >>>  6);
        h += (h <<   2) + (h << 14);
        return h ^ (h >>> 16);
    }

    public CacheEntry add(String path, int size) {
        Segment[] segments = this.segments;
        return segments[hash(path.hashCode()) >>> segmentShift & (segments.length - 1)].add(path, size);
    }

    public CacheEntry get(String path) {
        Segment[] segments = this.segments;
        return segments[hash(path.hashCode()) >>> segmentShift & (segments.length - 1)].get(path);
    }

    public void remove(String path) {
        Segment[] segments = this.segments;
        segments[hash(path.hashCode()) >>> segmentShift & (segments.length - 1)].remove(path);
    }

    private static class MaxLinkedMap<K, V> extends LinkedHashMap<K, V> {
        private int max;

        public MaxLinkedMap(int max) {
            this(max, false);
        }

        public MaxLinkedMap(int max, boolean access) {
            super(3, 0.66f, access);
            this.max = max;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > max;
        }
    }

    private static class CacheMap extends MaxLinkedMap<String, CacheEntry> {
        private CacheMap(int max) {
            super(max, true);
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, CacheEntry> eldest) {
            if (super.removeEldestEntry(eldest))  {
                CacheEntry value = eldest.getValue();
                value.dereference();
                return true;
            }

            return false;
        }
    }

    public static class CacheEntry {
        private static final AtomicIntegerFieldUpdater<CacheEntry> refsUpdater = AtomicIntegerFieldUpdater.newUpdater(CacheEntry.class, "refs");
        private static final AtomicIntegerFieldUpdater<CacheEntry> enabledUpdater = AtomicIntegerFieldUpdater.newUpdater(CacheEntry.class, "enabled");

        private final int size;
        private volatile PooledByteBuffer[] buffers;
        private volatile int enabled;
        private volatile int refs;

        public CacheEntry(int size, PooledByteBuffer[] buffers) {
            this.size = size;
            this.buffers = buffers;
        }

        public int size() {
            return size;
        }

        public PooledByteBuffer[] buffers() {
            return buffers;
        }

        public boolean enabled() {
            return enabled == 2;
        }

        public boolean claimEnable() {
            return enabledUpdater.compareAndSet(this, 0, 1);
        }

        public void enable() {
            this.enabled = 2;
        }

        public void disable() {
            this.enabled = 0;
        }

        public int reference() {
            for(;;) {
                int refs = this.refs;
                if (refs < 1) {
                    return refs; // destroying
                }

                if (refsUpdater.compareAndSet(this, refs++, refs)) {
                    return refs;
                }
            }
        }

        public int dereference() {
            for(;;) {
                int refs = this.refs;
                if (refs < 1) {
                    return refs;  // destroying
                }

                if (refsUpdater.compareAndSet(this, refs--, refs)) {
                    if (refs == 0) {
                        destroy();
                    }
                    return refs;
                }
            }
        }

        private void destroy() {
            buffers = EMPTY_BUFFERS;

            for (PooledByteBuffer buffer : buffers()) {
                buffer.free();
            }
        }
    }

    private class Segment {
        private final LinkedHashMap<String, CacheEntry> cache;
        private final LinkedHashMap<String, Integer> candidates;

        private Segment(int concurrency) {
            int limit = Math.max(100, max / sliceSize / concurrency);
            cache = new CacheMap(limit);
            candidates = new MaxLinkedMap<String, Integer>(limit);
        }

        public synchronized CacheEntry get(String path) {
            return cache.get(path);
        }

        public synchronized CacheEntry add(String path, int size) {
            CacheEntry entry = cache.get(path);
            if (entry != null)
                return null;

            Integer i = candidates.get(path);
            int count = i == null ? 0 : i.intValue();

            if (count > 5) {
                candidates.remove(path);
                entry = addCacheEntry(path, size);
            }  else {
                candidates.put(path, Integer.valueOf(++count));
            }

            return entry;
        }

        private CacheEntry addCacheEntry(String path, int size) {
            int reserveSize = size;
            int sliceSize = DirectBufferCache.this.sliceSize;
            int n = 0;
            while ((reserveSize -= sliceSize) > 0) {
                n++;
            }

            PooledByteBuffer[] buffers = allocate(n);
            if (buffers == null) {
                Iterator<CacheEntry> iterator = cache.values().iterator();
                int released = 0;
                while (released < size && iterator.hasNext()) {
                    CacheEntry value = iterator.next();
                    iterator.remove();
                    value.dereference();
                    released += value.size();
                }
                buffers = allocate(n);
            }

            if (buffers == null) {
                return null;
            }

            CacheEntry result = new CacheEntry(size, buffers);
            cache.put(path, result);

            return result;
        }

        private PooledByteBuffer[] allocate(int slices) {
            LimitedBufferSlicePool slicePool = pool;
            if (! slicePool.canAllocate(slices)) {
                return null;
            }

            PooledByteBuffer[] buffers = new PooledByteBuffer[slices];
            for (int i = 0; i < slices; i++) {
                PooledByteBuffer allocate = slicePool.allocate();
                if (allocate == null) {
                    while (--i >= 0) {
                        buffers[i].free();
                    }

                    return null;
                }

                buffers[i] = allocate;
            }
            return buffers;
        }

        public void remove(String path) {
            CacheEntry remove = cache.remove(path);
            if (remove != null)
                remove.dereference();
        }
    }
}
