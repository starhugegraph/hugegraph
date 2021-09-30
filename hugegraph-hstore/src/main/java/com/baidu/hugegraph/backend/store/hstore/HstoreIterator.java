package com.baidu.hugegraph.backend.store.hstore;

import com.baidu.hugegraph.store.HgKvEntry;

import java.util.Iterator;
import java.util.List;

public class HstoreIterator implements HstoreBackendIterator, Iterator {

    List<HgKvEntry> entries = null;
    int offset = 0;
    byte[] key;
    byte[] value;

    public HstoreIterator(List<HgKvEntry> hgKvEntries) {
        this.entries = hgKvEntries;
    }

    @Override
    public boolean hasNext() {
        return this.entries == null ? false : offset < this.entries.size();
    }

    @Override
    public HstoreIterator next() {
        assert offset < this.entries.size();
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
}
