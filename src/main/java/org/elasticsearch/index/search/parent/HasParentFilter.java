package org.elasticsearch.index.search.parent;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.search.internal.ScopePhase;

// Strategy: naively collect parent documents into a bunch of FixedBitSets.
// Then, at filter time, consult the FixedBitSet corresponding to the current
// index reader. Somehow create another FixedBitSet corresponding to the set
// of parents of the children in the current index reader and then compute the
// intersection of the two bitsets.

@SuppressWarnings("serial")
public class HasParentFilter extends Filter implements ScopePhase.CollectorPhase {

    private Query parentQuery;
    private String scope;
    private String childType;
    private String parentType;
    private Map<Object, FixedBitSet> parentDocs;

    public HasParentFilter(Query parentQuery, String scope, String childType, String parentType) {
        this.parentQuery = parentQuery;
        this.scope = scope;
        // TODO: use these two values to write a toString().
        this.childType = childType;
        this.parentType = parentType;
    }

    @Override
    public String scope() {
        return this.scope;
    }

    @Override
    public void clear() {
        this.parentDocs = null;
    }

    @Override
    public Query query() {
        // This is the query that the collector will collect over.
        return this.parentQuery;
    }

    @Override
    public boolean requiresProcessing() {
        return parentDocs == null;
    }

    @Override
    public Collector collector() {
        return new ParentCollector();
    }

    @Override
    public void processCollector(Collector collector) {
        this.parentDocs = ((ParentCollector) collector).parentDocs();
    }

    // So ends the implementation of the collector phase interface and begins
    // the implementation of the filter.

    @Override
    public DocIdSet getDocIdSet(IndexReader reader) throws IOException {
        // TODO: OK, IndexReader is conceptually a list of children doc IDs. We
        // need to massage it into a list of parent doc IDs, and then a bit
        // set. Then we'll have two bit sets and all we need to do is call AND.
        return null;
    }
}