package org.elasticsearch.similarity;

import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SmallFloat;

import java.util.ArrayList;
import java.util.List;

/**
 * @Classname TermMyTermMyBM25Similarity
 * @Description 调整评分策略，得到粗粒度等分 (目标是学院排序，相关度排序和文档热度排序)
 * @Date 2021/2/21 16:10
 * @Created by muhao
 */

/**
 * 基于 BM25 然后自己改了一部分实现
 */
public class TermMyBM25Similarity extends Similarity {
    private final float k1;
    private final float b;

    /**
     * BM25 with the supplied parameter values.
     * @param k1 Controls non-linear term frequency normalization (saturation).
     * @param b Controls to what degree document length normalizes tf values.
     * @throws IllegalArgumentException if {@code k1} is infinite or negative, or if {@code b} is
     *         not within the range {@code [0..1]}
     */
    public TermMyBM25Similarity(float k1, float b) {
        if (Float.isFinite(k1) == false || k1 < 0) {
            throw new IllegalArgumentException("illegal k1 value: " + k1 + ", must be a non-negative finite value");
        }
        if (Float.isNaN(b) || b < 0 || b > 1) {
            throw new IllegalArgumentException("illegal b value: " + b + ", must be between 0 and 1");
        }
        this.k1 = k1;
        this.b  = b;
    }

    /** BM25 with these default values:
     * <ul>
     *   <li>{@code k1 = 1.2}</li>
     *   <li>{@code b = 0.75}</li>
     * </ul>
     */
    public TermMyBM25Similarity() {
        this(1.2f, 0.75f);
    }

    /** Implemented as <code>log(1 + (docCount - docFreq + 0.5)/(docFreq + 0.5))</code>. */
    protected float idf(long docFreq, long docCount) {
        return (float) Math.log(1 + (docCount - docFreq + 0.5D)/(docFreq + 0.5D));
    }

    /** The default implementation returns <code>1</code> */
    protected float scorePayload(int doc, int start, int end, BytesRef payload) {
        return 1;
    }

    /** The default implementation computes the average as <code>sumTotalTermFreq / docCount</code> */
    protected float avgFieldLength(CollectionStatistics collectionStats) {
        return (float) (collectionStats.sumTotalTermFreq() / (double) collectionStats.docCount());
    }

    /**
     * True if overlap tokens (tokens with a position of increment of zero) are
     * discounted from the document's length.
     */
    protected boolean discountOverlaps = true;

    /** Sets whether overlap tokens (Tokens with 0 position increment) are
     *  ignored when computing norm.  By default this is true, meaning overlap
     *  tokens do not count when computing norms. */
    public void setDiscountOverlaps(boolean v) {
        discountOverlaps = v;
    }

    /**
     * Returns true if overlap tokens are discounted from the document's length.
     * @see #setDiscountOverlaps
     */
    public boolean getDiscountOverlaps() {
        return discountOverlaps;
    }

    /** Cache of decoded bytes. */
    private static final float[] LENGTH_TABLE = new float[256];

    static {
        for (int i = 0; i < 256; i++) {
            LENGTH_TABLE[i] = SmallFloat.byte4ToInt((byte) i);
        }
    }


    @Override
    public final long computeNorm(FieldInvertState state) {
        final int numTerms;
        if (state.getIndexOptions() == IndexOptions.DOCS && state.getIndexCreatedVersionMajor() >= 8) {
            numTerms = state.getUniqueTermCount();
        } else if (discountOverlaps) {
            numTerms = state.getLength() - state.getNumOverlap();
        } else {
            numTerms = state.getLength();
        }
        return SmallFloat.intToByte4(numTerms);
    }


    /**
     * Computes a score factor for a simple term and returns an explanation
     * for that score factor.
     *
     * <p>
     * The default implementation uses:
     *
     * <pre class="prettyprint">
     * idf(docFreq, docCount);
     * </pre>
     *
     * Note that {@link CollectionStatistics#docCount()} is used instead of
     * {@link org.apache.lucene.index.IndexReader#numDocs() IndexReader#numDocs()} because also
     * {@link TermStatistics#docFreq()} is used, and when the latter
     * is inaccurate, so is {@link CollectionStatistics#docCount()}, and in the same direction.
     * In addition, {@link CollectionStatistics#docCount()} does not skew when fields are sparse.
     *
     * @param collectionStats collection-level statistics
     * @param termStats term-level statistics for the term
     * @return an Explain object that includes both an idf score factor
    and an explanation for the term.
     */
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats) {
        final long df = termStats.docFreq();
        final long docCount = collectionStats.docCount();
        final float idf = idf(df, docCount);
        return Explanation.match(idf, "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
                Explanation.match(df, "n, number of documents containing term"),
                Explanation.match(docCount, "N, total number of documents with field"));
    }

    /**
     * Computes a score factor for a phrase.
     *
     * <p>
     * The default implementation sums the idf factor for
     * each term in the phrase.
     *
     * @param collectionStats collection-level statistics
     * @param termStats term-level statistics for the terms in the phrase
     * @return an Explain object that includes both an idf
     *         score factor for the phrase and an explanation
     *         for each term.
     */
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics termStats[]) {
        double idf = 0d; // sum into a double before casting into a float
        List<Explanation> details = new ArrayList<>();
        for (final TermStatistics stat : termStats ) {
            Explanation idfExplain = idfExplain(collectionStats, stat);
            details.add(idfExplain);
            idf += idfExplain.getValue().floatValue();
        }
        return Explanation.match((float) idf, "idf, sum of:", details);
    }

    @Override
    public final SimScorer scorer(float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
        Explanation idf = termStats.length == 1 ? idfExplain(collectionStats, termStats[0]) : idfExplain(collectionStats, termStats);
        float avgdl = avgFieldLength(collectionStats);

        float[] cache = new float[256];
        for (int i = 0; i < cache.length; i++) {
            cache[i] = k1 * ((1 - b) + b * LENGTH_TABLE[i] / avgdl);
        }
        return new TermMyBM25Similarity.BM25Scorer(boost, k1, b, idf, avgdl, cache);
    }

    /** Collection statistics for the BM25 model. */
    private static class BM25Scorer extends SimScorer {
        /** query boost */
        private final float boost;
        /** k1 value for scale factor */
        private final float k1;
        /** b value for length normalization impact */
        private final float b;
        /** BM25's idf */
        private final Explanation idf;
        /** The average document length. */
        private final float avgdl;
        /** precomputed norm[256] with k1 * ((1 - b) + b * dl / avgdl) */
        private final float[] cache;
        /** weight (idf * boost) */
        private final float weight;

        BM25Scorer(float boost, float k1, float b, Explanation idf, float avgdl, float[] cache) {
            this.boost = boost;
            this.idf = idf;
            this.avgdl = avgdl;
            this.k1 = k1;
            this.b = b;
            this.cache = cache;
            this.weight = boost * idf.getValue().floatValue();
        }

        @Override
        public float score(float freq, long encodedNorm) {
            double norm = cache[((byte) encodedNorm) & 0xFF];
//            return weight * (float) (freq / (freq + norm));
            return weight;
        }

        @Override
        public Explanation explain(Explanation freq, long encodedNorm) {
            List<Explanation> subs = new ArrayList<>(explainConstantFactors());
            Explanation tfExpl = explainTF(freq, encodedNorm);
            subs.add(tfExpl);
            return Explanation.match(weight * tfExpl.getValue().floatValue(),
                    "score(freq="+freq.getValue()+"), product of:", subs);
        }

        private Explanation explainTF(Explanation freq, long norm) {
            List<Explanation> subs = new ArrayList<>();
            subs.add(freq);
            subs.add(Explanation.match(k1, "k1, term saturation parameter"));
            float doclen = LENGTH_TABLE[((byte) norm) & 0xff];
            subs.add(Explanation.match(b, "b, length normalization parameter"));
            if ((norm & 0xFF) > 39) {
                subs.add(Explanation.match(doclen, "dl, length of field (approximate)"));
            } else {
                subs.add(Explanation.match(doclen, "dl, length of field"));
            }
            subs.add(Explanation.match(avgdl, "avgdl, average length of field"));
            float normValue = k1 * ((1 - b) + b * doclen / avgdl);
            return Explanation.match(
                    (float) (freq.getValue().floatValue() / (freq.getValue().floatValue() + (double) normValue)),
                    "tf, computed as freq / (freq + k1 * (1 - b + b * dl / avgdl)) from:", subs);
        }

        private List<Explanation> explainConstantFactors() {
            List<Explanation> subs = new ArrayList<>();
            // query boost
            if (boost != 1.0f) {
                subs.add(Explanation.match(boost, "boost"));
            }
            // idf
            subs.add(idf);
            return subs;
        }
    }

    @Override
    public String toString() {
        return "BM25(k1=" + k1 + ",b=" + b + ")";
    }

    /**
     * Returns the <code>k1</code> parameter
     * @see #TermMyBM25Similarity(float, float)
     */
    public final float getK1() {
        return k1;
    }

    /**
     * Returns the <code>b</code> parameter
     * @see #TermMyBM25Similarity(float, float)
     */
    public final float getB() {
        return b;
    }
}
