package org.elasticsearch.mysynonym;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PackedTokenAttributeImpl;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.myterm.TermQuery_V1;
import org.elasticsearch.similarity.MultistepSimilarity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.lucene.search.Queries.newUnmappedFieldQuery;

/**
 * @Classname SynonymMatchQuery
 * @Description 在支持动态配置 similarity 的 Lucene query 前提下，实现上层的 ES query。
 * @Date 2021/7/05 19:39
 * @Created by muhao
 */
public class MultistepScoreQuery {


    public enum ZeroTermsQuery implements Writeable {
        NONE(0),
        ALL(1),
        // this is used internally to make sure that query_string and simple_query_string
        // ignores query part that removes all tokens.
        NULL(2);

        private final int ordinal;

        ZeroTermsQuery(int ordinal) {
            this.ordinal = ordinal;
        }

        public static ZeroTermsQuery readFromStream(StreamInput in) throws IOException {
            int ord = in.readVInt();
            for (ZeroTermsQuery zeroTermsQuery : ZeroTermsQuery.values()) {
                if (zeroTermsQuery.ordinal == ord) {
                    return zeroTermsQuery;
                }
            }
            throw new ElasticsearchException("unknown serialized type [" + ord + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }

    public static final ZeroTermsQuery DEFAULT_ZERO_TERMS_QUERY = ZeroTermsQuery.NONE;

    protected final QueryShardContext context;

    protected Analyzer analyzer;

    protected BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

    protected MultistepScoreQuery.ZeroTermsQuery zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;

    // 既然使用了此 query ，那么就是默认使用 阶梯 similarity
    private double base = Math.E;

    public double getBase() {
        return base;
    }

    public void setBase(double base) {
        this.base = base;
    }

    public MultistepScoreQuery(QueryShardContext context) {
        this.context = context;
    }

    public void setAnalyzer(String analyzerName) {
        this.analyzer = context.getMapperService().getIndexAnalyzers().get(analyzerName);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
        }
    }

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void setOccur(BooleanClause.Occur occur) {
        this.occur = occur;
    }

    public void setZeroTermsQuery(MultistepScoreQuery.ZeroTermsQuery zeroTermsQuery) {
        this.zeroTermsQuery = zeroTermsQuery;
    }

    public Query parse(String fieldName, Object value) throws IOException {
        final MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            return newUnmappedFieldQuery(fieldName);
        }

        if (this.analyzer == null) {
            Analyzer analyzer = getAnalyzer(fieldType);
            assert analyzer != null;
            this.analyzer = analyzer;
        }

        return parseInternal(fieldName, fieldType, value.toString());
    }

    protected final Query parseInternal(String fieldName, MappedFieldType fieldType, String queryText) throws IOException {
        final Query query;
        // Use the analyzer to get all the tokens, and then build an appropriate
        // query based on the analysis chain.
        try (TokenStream source = analyzer.tokenStream(fieldName, queryText)) {
            query = createFieldQuery(source, fieldType, fieldName);
        } catch (IOException e) {
            throw new RuntimeException("Error analyzing query text", e);
        }
        return query == null ? zeroTermsQuery() : query;
    }

    private Query createFieldQuery(TokenStream source, MappedFieldType fieldType, String field) {

        // 收集每一个 position 上的 term (如果有同义词 ，会有多个 term)
        List<TermType> positionTerms = new ArrayList<>();
        // Build an appropriate query based on the analysis chain.
        try (CachingTokenFilter stream = new CachingTokenFilter(source)) {

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
            PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

            if (termAtt == null) {
                return null;
            }

            // phase 1: read through the stream and assess the situation:
            // counting the number of tokens/positions and marking if we have any synonyms.


            stream.reset();
            // 执行 stream.incrementToken() 会产生一个 term
            // TODO 检查 stop 词的情况
            while (stream.incrementToken()) {
                int positionIncrement = posIncAtt.getPositionIncrement();
                // 分词出下一个 term
                PackedTokenAttributeImpl t = ((PackedTokenAttributeImpl) termAtt);
                String type = t.type();
                positionTerms.add(new TermType(field, type, t.getBytesRef()));
            }

            // phase 2: based on token count, presence of synonyms, and options
            // formulate a single term, boolean, or phrase.
            if (positionTerms.size() == 0) {
                return null;
            } else {
                return analyzeList(positionTerms);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error analyzing query text", e);
        }
    }

    private Query analyzeList(List<TermType> positionTerms) {
        if (positionTerms.size() == 1) {
            TermType termType = positionTerms.get(0);
            return newTermQuery(termType);
        } else {
            return newBooleanQuery(positionTerms);
        }
    }

    private Query newTermQuery(TermType termType){
        TermQuery_V1 query = new TermQuery_V1(new Term(termType.getField(), termType.getBytes()));
        query.setSimilarity(new MultistepSimilarity(base));
        return query;
    }


    private Query newBooleanQuery(List<TermType> positionTerms) {
        BooleanQuery.Builder q = new BooleanQuery.Builder();
        for (TermType t : positionTerms){
            if ("SYNONYM".equals(t.getType())) {
                // 同义词的召回得分贡献为 0
                TermQuery termQuery = new TermQuery(new Term(t.getField(), t.getBytes()));
                q.add(new BoostQuery(termQuery, 0f), BooleanClause.Occur.SHOULD);
            } else {
                q.add(newTermQuery(t), BooleanClause.Occur.SHOULD);
            }
        }
        return q.build();
    }


    protected Analyzer getAnalyzer(MappedFieldType fieldType) {
        if (analyzer == null) {
            return context.getSearchAnalyzer(fieldType);
        } else {
            return analyzer;
        }
    }

    protected Query zeroTermsQuery() {
        switch (zeroTermsQuery) {
            case NULL:
                return null;
            case NONE:
                return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
            case ALL:
                return Queries.newMatchAllQuery();
            default:
                throw new IllegalStateException("unknown zeroTermsQuery " + zeroTermsQuery);
        }
    }

}
