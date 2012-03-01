package org.elasticsearch.index.search.parent;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.BytesWrap;
import org.elasticsearch.common.lucene.docset.GetDocSet;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.index.cache.id.IdReaderTypeCache;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.internal.ScopePhase;
import org.elasticsearch.search.internal.SearchContext;

// Strategy: naively collect parent documents into a bunch of FixedBitSets.
// Then, at filter time, consult the FixedBitSet corresponding to the current
// index reader. Somehow create another FixedBitSet corresponding to the set
// of parents of the children in the current index reader and then compute the
// intersection of the two bitsets.

@SuppressWarnings("serial")
public class HasParentFilter extends Filter implements ScopePhase.CollectorPhase {

    private Query parentQuery;
    private String scope;
    private String parentType;
    private Map<Object, FixedBitSet> parentDocs;
    private final SearchContext context;

    public HasParentFilter(Query parentQuery, String scope, String parentType, SearchContext context) {
        this.parentQuery = parentQuery;
        this.scope = scope;
        this.parentType = parentType;
        this.context = context;
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
        return new ChildDocSet(reader, context.idCache().reader(reader).type(parentType), parentDocs.get(reader.getCoreCacheKey()));
    }

    static class ChildDocSet extends GetDocSet {
        private final IndexReader reader;
        private final IdReaderTypeCache typeCache;
        private final FixedBitSet parentIds;

        public ChildDocSet(IndexReader reader, IdReaderTypeCache typeCache, FixedBitSet parentIds) {
            super(reader.maxDoc());
            this.reader = reader;
            this.typeCache = typeCache;
            this.parentIds = parentIds;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }

        @Override
        public boolean isCacheable() {
            // TODO?
            return false;
        }

        @Override
        public boolean get(int n) {
            BytesWrap parentId = typeCache.parentIdByDoc(n);
            int parentDocId = typeCache.docById(parentId);
            return parentDocId != -1 && !reader.isDeleted(parentDocId) && parentIds.get(parentDocId);
        }
    }
}