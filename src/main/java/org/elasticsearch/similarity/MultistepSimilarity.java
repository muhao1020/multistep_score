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
 * @Classname MultistepSimilarity
 * @Description 在 BM25 算法基础上改造，增加控制 idf 的参数 base（合理范围是大于 1 的double类型数）， 如果 bucket=Math.E 则近似的退化成 BM25算法
 * 效果是将原本平滑的得分，经过 Math.round 变成整数，构造成类似阶梯的形状。方便为了后续的 热度标量 做二级排序准备
 * base 值越大， 阶梯宽度也越大
 * @Date 2021/7/1 19:42
 * @Created by muhao
 */
public class MultistepSimilarity extends Similarity {
    private final double base;

    public MultistepSimilarity(double base) {
        if (Double.isFinite(base) == false || base <= 1) {
            throw new IllegalArgumentException("illegal base value: " + base + ", must be a bigger than 1 finite value");
        }
        this.base = base;
    }

    /** BM25 with these default base number:
     * <ul>
     *   <li>{@code base = Math.E}</li>
     * </ul>
     */
    public MultistepSimilarity() {
        this(Math.E);
    }

    /** Implemented as <code>log(1 + (docCount - docFreq + 0.5)/(docFreq + 0.5))</code>. */
    protected float idf(long docFreq, long docCount) {
        float bm25_idf = (float) Math.log(1 + (docCount - docFreq + 0.5D)/(docFreq + 0.5D));
        float bucket_idf = (float) Math.log(base);
        return (float) Math.ceil(bm25_idf/bucket_idf);
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
//        final long df = termStats.docFreq();
//        final long docCount = collectionStats.docCount();
//        final float idf = idf(df, docCount);
//        return Explanation.match(idf, "idf, computed as log(1 + (N - n + 0.5) / (n + 0.5)) from:",
//                Explanation.match(df, "n, number of documents containing term"),
//                Explanation.match(docCount, "N, total number of documents with field"));
        final long df = termStats.docFreq();
        final long docCount = collectionStats.docCount();
        final float idf = idf(df, docCount);
        return Explanation.match(idf, "idf, computed as Math.ceil(log(1 + (N - n + 0.5) / (n + 0.5))),   from:",
                Explanation.match(df, "n, number of documents containing term"),
                Explanation.match(docCount, "N, total number of documents with field"),
                Explanation.match(base, "base number for log"));
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

        return new BM25Scorer(boost, idf, avgdl);
    }

    /** Collection statistics for the BM25 model. */
    private static class BM25Scorer extends SimScorer {
        /** query boost */
        private final float boost;
        /** BM25's idf */
        private final Explanation idf;
        /** The average document length. */
        private final float avgdl;
        /** weight (idf * boost) */
        private final float weight;

        BM25Scorer(float boost, Explanation idf, float avgdl) {
            this.boost = boost;
            this.idf = idf;
            this.avgdl = avgdl;
            this.weight = boost * idf.getValue().floatValue();
        }

        @Override
        public float score(float freq, long encodedNorm) {
//            double norm = cache[((byte) encodedNorm) & 0xFF];
            // current length of field
            int  doclen = ((byte) encodedNorm) & 0xFF;
            int docWeight = doclen < avgdl ? (int) freq : Math.round(freq/ Math.round(doclen / avgdl));
            return weight * docWeight;
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
            float doclen = LENGTH_TABLE[((byte) norm) & 0xff];
            if ((norm & 0xFF) > 39) {
                subs.add(Explanation.match(doclen, "dl, length of field (approximate)"));
            } else {
                subs.add(Explanation.match(doclen, "dl, length of field"));
            }
            subs.add(Explanation.match(avgdl, "avgdl, average length of field"));
            float normValue = doclen < avgdl ? 1 : Math.round(doclen / avgdl);
            return Explanation.match(
                    Math.round(freq.getValue().floatValue() / normValue),
                    "tf, computed as if dl < avgdl : return Math.round(freq/1); else return Math.round(freq/(Math.round(dl / avgdl))):", subs);
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
        return "BM25(base=" + base + ")";
    }

}
