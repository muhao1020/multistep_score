package org.elasticsearch.myterm;

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;

/**
 * @Classname TermQuery_V1
 * @Description 用于定义一个可以控制 similarity 的 TermQuery
 * @Date 2021/7/5 10:50
 * @Created by muhao
 */
public class TermQuery_V1 extends TermQuery {
    private Similarity similarity = new BM25Similarity();
    public TermQuery_V1(Term t) {
        super(t);
    }

    public TermQuery_V1(Term t, TermStates states) {
        super(t, states);
    }

    /**
     * 这是一个重要的要改的地方，因为这里的 similarity 将会被用于 doc 的 评分
     * @param similarity
     */
    public void setSimilarity(Similarity similarity) {
        this.similarity = similarity;
    }

    public Similarity getSimilarity() {
        return similarity;
    }

    /**
     * 这种方式要比之前的复制，然后在基础上修改要更加优雅
     * 官方文档： Once you have a new IndexReader, it's relatively cheap to create a new IndexSearcher from it.
     * 在一个现成的 IndexReader 上构造 IndexSearcher 是相对容易的(debug 进去发现没有进行底层的 IO 操作)。
     *
     * @param searcher
     * @param scoreMode
     * @param boost
     * @return
     * @throws IOException
     */
    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        // 如果 query 中的 similarity 和 IndexSearcher 中的 similarity 为不同的实例，则创建一个新的 IndexSearcher
        // TODO 这里有一个 bug，仅仅是考虑 两个 实例是否是同一个类的实例是不够的，其中的参数不通，评分结果也不同(比如 BM25 中的 k1 b 两个参数)。
        if (similarity.getClass().equals(searcher.getSimilarity().getClass()) == false){
            searcher = new IndexSearcher(searcher.getIndexReader());
            searcher.setSimilarity(similarity);
        }
        Weight weight = super.createWeight(searcher, scoreMode, boost);
        return weight;
    }
}
