package org.elasticsearch.myterm;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;

import java.io.IOException;

/**
 * @Classname TermMatchesIterator
 * @Description TODO
 * @Date 2021/3/3 17:07
 * @Created by muhao
 */
class TermMatchesIterator implements MatchesIterator {

    private int upto;
    private int pos;
    private final PostingsEnum pe;
    private final Query query;

    TermMatchesIterator(Query query, PostingsEnum pe) throws IOException {
        this.pe = pe;
        this.query = query;
        this.upto = pe.freq();
    }

    @Override
    public boolean next() throws IOException {
        if (upto-- > 0) {
            pos = pe.nextPosition();
            return true;
        }
        return false;
    }

    @Override
    public int startPosition() {
        return pos;
    }

    @Override
    public int endPosition() {
        return pos;
    }

    @Override
    public int startOffset() throws IOException {
        return pe.startOffset();
    }

    @Override
    public int endOffset() throws IOException {
        return pe.endOffset();
    }

    @Override
    public MatchesIterator getSubMatches() throws IOException {
        return null;
    }

    @Override
    public Query getQuery() {
        return query;
    }
}

