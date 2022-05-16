package org.elasticsearch.myterm;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.BaseTermQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;

/**
 * @Classname MyTermQueryBuilder
 * @Description 在 term 的基础上，实现查询时的指定 similarity（这里的海鸥） 功能
 * @Date 2021/3/13 19:11
 * @Created by muhao
 */
public class MyTermQueryBuilder extends BaseTermQueryBuilder<MyTermQueryBuilder> {
    public static final String NAME = "custom_similarity_term";

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField SIMILARITY_FIELD = new ParseField("similarity");
    private String similarity_type;


    public static QueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String similarity_type = null;
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, currentFieldName);
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token.isValue()) {
                        if (QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            value = parser.objectText();
                        } else if (SIMILARITY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            similarity_type = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }
        MyTermQueryBuilder termQueryBuilder = new MyTermQueryBuilder(fieldName, value, similarity_type);
        termQueryBuilder.boost(boost);
        return termQueryBuilder;
    }

    public MyTermQueryBuilder(String fieldName, Object value, String similarity_type) {
        super(fieldName, value);
        this.similarity_type = similarity_type;
        System.out.println("AAAAAA:"+similarity_type);
    }

    public MyTermQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        return new MyTermQuery(new Term(fieldName, (BytesRef) value),similarity_type);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
