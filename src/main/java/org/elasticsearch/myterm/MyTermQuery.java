package org.elasticsearch.myterm;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.similarity.MultistepSimilarity;
import org.elasticsearch.similarity.TermMyBM25Similarity;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;


/**
 * MyTermQuery 实现和 TermQuery 完全一样，只是在 TermWeight 时可以指定 具体的 similarity 的实现类，摆脱了在 field上上定义
 * similarity的方式，更加的灵活
 */
public class MyTermQuery extends Query {

    private final Term term;
    private final TermStates perReaderTermState;
    private final String similarity_type;

    final class TermWeight extends Weight {
        private final Similarity similarity;
        private final Similarity.SimScorer simScorer;
        private final TermStates termStates;
        private final ScoreMode scoreMode;

        public TermWeight(IndexSearcher searcher, ScoreMode scoreMode,
                          float boost, TermStates termStates) throws IOException {
            super(MyTermQuery.this);
            if (scoreMode.needsScores() && termStates == null) {
                throw new IllegalStateException("termStates are required when scores are needed");
            }
            this.scoreMode = scoreMode;
            this.termStates = termStates;
//            this.similarity = searcher.getSimilarity();
            this.similarity = getSimilarity(similarity_type);;

            final CollectionStatistics collectionStats;
            final TermStatistics termStats;
            if (scoreMode.needsScores()) {
                collectionStats = searcher.collectionStatistics(term.field());
                termStats = searcher.termStatistics(term, termStates);
            } else {
                // we do not need the actual stats, use fake stats with docFreq=maxDoc=ttf=1
                collectionStats = new CollectionStatistics(term.field(), 1, 1, 1, 1);
                termStats = new TermStatistics(term.bytes(), 1, 1);
            }

            if (termStats == null) {
                this.simScorer = null; // term doesn't exist in any segment, we won't use similarity at all
            } else {
                this.simScorer = similarity.scorer(boost, collectionStats, termStats);
            }
        }

        private Similarity getSimilarity(String similarity){
            if ("BM25".equalsIgnoreCase(similarity)){
                return new BM25Similarity();
            } else if ("class".equalsIgnoreCase(similarity)) {
                return new ClassicSimilarity();
            } else if ("custom".equalsIgnoreCase(similarity)) {
                return new TermMyBM25Similarity();
            } else if (similarity.startsWith("bucket-")) {
                String bucket = similarity.substring("bucket-".length());
                if (bucket.equalsIgnoreCase("e")) {
                    // 使用 自然数 E 来作为底数，也是BM25默认的使用底数
                    // 使用越大的底数，导致查询query term的权重越平滑，低频词的权重和高频词的权重差距越小。
                    return new MultistepSimilarity();
                }
                return new MultistepSimilarity(Double.parseDouble(bucket));
            }
            return new BM25Similarity();
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            terms.add(getTerm());
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            TermsEnum te = getTermsEnum(context);
            if (te == null) {
                return null;
            }
            if (context.reader().terms(term.field()).hasPositions() == false) {
                return super.matches(context, doc);
            }
            return MatchesUtils.forField(term.field(), () -> {
                PostingsEnum pe = te.postings(null, PostingsEnum.OFFSETS);
                if (pe.advance(doc) != doc) {
                    return null;
                }
                return new TermMatchesIterator(getQuery(), pe);
            });
        }

        @Override
        public String toString() {
            return "weight(" + MyTermQuery.this + ")";
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            assert termStates == null || termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) : "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);;
            final TermsEnum termsEnum = getTermsEnum(context);
            if (termsEnum == null) {
                return null;
            }
            LeafSimScorer scorer = new LeafSimScorer(simScorer, context.reader(), term.field(), scoreMode.needsScores());
            if (scoreMode == ScoreMode.TOP_SCORES) {
                return new TermScorer(this, termsEnum.impacts(PostingsEnum.FREQS), scorer);
            } else {
                return new TermScorer(this, termsEnum.postings(null, scoreMode.needsScores() ? PostingsEnum.FREQS : PostingsEnum.NONE), scorer);
            }
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return true;
        }

        /**
         * Returns a {@link TermsEnum} positioned at this weights Term or null if
         * the term does not exist in the given context
         */
        private TermsEnum getTermsEnum(LeafReaderContext context) throws IOException {
            assert termStates != null;
            assert termStates.wasBuiltFor(ReaderUtil.getTopLevelContext(context)) :
                    "The top-reader used to create Weight is not the same as the current reader's top-reader (" + ReaderUtil.getTopLevelContext(context);
            final TermState state = termStates.get(context);
            if (state == null) { // term is not present in that reader
                assert termNotInReader(context.reader(), term) : "no termstate found but term exists in reader term=" + term;
                return null;
            }
            final TermsEnum termsEnum = context.reader().terms(term.field()).iterator();
            termsEnum.seekExact(term.bytes(), state);
            return termsEnum;
        }

        private boolean termNotInReader(LeafReader reader, Term term) throws IOException {
            // only called from assert
            // System.out.println("TQ.termNotInReader reader=" + reader + " term=" +
            // field + ":" + bytes.utf8ToString());
            return reader.docFreq(term) == 0;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            TermScorer scorer = (TermScorer) scorer(context);
            if (scorer != null) {
                int newDoc = scorer.iterator().advance(doc);
                if (newDoc == doc) {
                    float freq = scorer.freq();
                    LeafSimScorer docScorer = new LeafSimScorer(simScorer, context.reader(), term.field(), true);
                    Explanation freqExplanation = Explanation.match(freq, "freq, occurrences of term within document");
                    Explanation scoreExplanation = docScorer.explain(doc, freqExplanation);
                    return Explanation.match(
                            scoreExplanation.getValue(),
                            "weight(" + getQuery() + " in " + doc + ") ["
                                    + similarity.getClass().getSimpleName() + "], result of:",
                            scoreExplanation);
                }
            }
            return Explanation.noMatch("no matching term");
        }
    }

    /** Constructs a query for the term <code>t</code>. */
    public MyTermQuery(Term t) {
        term = Objects.requireNonNull(t);
        perReaderTermState = null;
        similarity_type = "BM25";
    }

    /**
     * Expert: constructs a MyTermQuery that will use the provided docFreq instead
     * of looking up the docFreq against the searcher.
     */
    public MyTermQuery(Term t, String similarity_type) {
        term = Objects.requireNonNull(t);
        perReaderTermState = null;
        this.similarity_type = similarity_type;
    }

    /** Returns the term of this query. */
    public Term getTerm() {
        return term;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        final IndexReaderContext context = searcher.getTopReaderContext();
        final TermStates termState;
        if (perReaderTermState == null
                || perReaderTermState.wasBuiltFor(context) == false) {
            termState = TermStates.build(context, term, scoreMode.needsScores());
        } else {
            // PRTS was pre-build for this IS
            termState = this.perReaderTermState;
        }

        return new MyTermQuery.TermWeight(searcher, scoreMode, boost, termState);
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(term.field())) {
            visitor.consumeTerms(this, term);
        }
    }

    /** Prints a user-readable version of this query. */
    @Override
    public String toString(String field) {
        StringBuilder buffer = new StringBuilder();
        if (!term.field().equals(field)) {
            buffer.append(term.field());
            buffer.append(":");
        }
        buffer.append(term.text());
        return buffer.toString();
    }

    /** Returns the {@link TermStates} passed to the constructor, or null if it was not passed.
     *
     * @lucene.experimental */
    public TermStates getTermStates() {
        return perReaderTermState;
    }

    /** Returns true iff <code>other</code> is equal to <code>this</code>. */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) &&
                term.equals(((MyTermQuery) other).term);
    }

    @Override
    public int hashCode() {
        return classHash() ^ term.hashCode();
    }
}
