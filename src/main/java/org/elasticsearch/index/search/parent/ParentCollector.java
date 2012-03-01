package org.elasticsearch.index.search.parent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.FixedBitSet;

public class ParentCollector extends Collector {

    private final Map<Object, FixedBitSet> parentDocs;

    private FixedBitSet currentBitSet;

    public ParentCollector() {
        this.parentDocs = new HashMap<Object, FixedBitSet>();
    }

    public Map<Object, FixedBitSet> parentDocs() {
        return this.parentDocs;
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        currentBitSet.set(doc);
    }

    @Override
    public void setNextReader(IndexReader reader, int docBase)
            throws IOException {
        Object key = reader.getCoreCacheKey();
        FixedBitSet docIdSet = parentDocs.get(key);
        if (docIdSet == null) {
            docIdSet = new FixedBitSet(reader.maxDoc());
            parentDocs.put(key, docIdSet);
        }
        this.currentBitSet = docIdSet;
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }
}