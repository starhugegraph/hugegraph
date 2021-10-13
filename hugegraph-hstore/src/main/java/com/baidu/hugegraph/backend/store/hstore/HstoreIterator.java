package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.backend.store.BackendEntry;
import com.baidu.hugegraph.store.HgKvEntry;
import org.apache.commons.collections.CollectionUtils;

import java.util.Iterator;
import java.util.List;

public class HstoreIterator implements HstoreBackendIterator,
        BackendEntry.BackendIterator, Iterator {

    List<HgKvEntry> entries = null;
    int offset = 0;
    byte[] key;
    byte[] value;

    public HstoreIterator(List<HgKvEntry> hgKvEntries) {
        this.entries = hgKvEntries;
    }

    @Override
    public boolean hasNext() {
        return CollectionUtils.isEmpty(entries) ?
                false : offset < this.entries.size() ;
    }

    @Override
    public HstoreIterator next() {
        assert offset < this.entries.size() ;
        HgKvEntry entry = this.entries.get(offset++);
        this.key = entry.key();
        this.value = entry.value();
        return this;
    }

    @Override
    public byte[] key() {
        return key;
    }

    @Override
    public byte[] value() {
        return value;
    }

    @Override
    public void close() {
        this.entries=null;
    }

    @Override
    public byte[] position() {
        return this.key;
    }
}
