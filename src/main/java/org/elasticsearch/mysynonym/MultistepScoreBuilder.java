package org.elasticsearch.mysynonym;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;

import java.io.IOException;
import java.util.Objects;

/**
 * @Classname MultistepScoreBuilder
 * @Description TODO
 * @Date 2021/7/6 16:28
 * @Created by muhao
 */
public class MultistepScoreBuilder extends AbstractQueryBuilder<MultistepScoreBuilder> {

    /** The name for the match query */
    public static final String NAME = "multistep_score";

    public static final ParseField QUERY_FIELD = new ParseField("query");
    public static final ParseField ANALYZER_FIELD = new ParseField("analyzer");
    public static final ParseField BASE_FIELD = new ParseField("base");
    public static final ParseField ZERO_TERMS_QUERY_FIELD = new ParseField("zero_terms_query");

    private final String fieldName;
    private final Object value;
    private String analyzer;
    // 这里使用 base 是否为 null 判断， 是否有参数传递进来
    private Double base;
    protected MultistepScoreQuery.ZeroTermsQuery zeroTermsQuery = MultistepScoreQuery.DEFAULT_ZERO_TERMS_QUERY;


    /**
     * Constructs a new match query.
     */
    public MultistepScoreBuilder(String fieldName, Object value) {
        if (fieldName == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires fieldName");
        }
        if (value == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires query value");
        }
        this.fieldName = fieldName;
        this.value = value;
    }

    /**
     * read from stream
     * @param in
     * @throws IOException
     */
    public MultistepScoreBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        value = in.readGenericValue();
        zeroTermsQuery = MultistepScoreQuery.ZeroTermsQuery.readFromStream(in);
        // optional fields
        analyzer = in.readOptionalString();
        base = in.readOptionalDouble();
    }

    /**
     * write to stream
     * @param out
     * @throws IOException
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeGenericValue(value);
        zeroTermsQuery.writeTo(out);
        // optional fields
        out.writeOptionalString(analyzer);
        out.writeDouble(base);
    }

    /** Returns the field name used in this query. */
    public String fieldName() {
        return this.fieldName;
    }

    /** Returns the value used in this query. */
    public Object value() {
        return this.value;
    }

    public MultistepScoreBuilder zeroTermsQuery(MultistepScoreQuery.ZeroTermsQuery zeroTermsQuery) {
        if (zeroTermsQuery == null) {
            throw new IllegalArgumentException("[" + NAME + "] requires zeroTermsQuery to be non-null");
        }
        this.zeroTermsQuery = zeroTermsQuery;
        return this;
    }

    /**
     * Returns the setting for handling zero terms queries.
     */
    public MultistepScoreQuery.ZeroTermsQuery zeroTermsQuery() {
        return this.zeroTermsQuery;
    }


    public MultistepScoreBuilder analyzer(String analyzer) {
        this.analyzer = analyzer;
        return this;
    }

    /** Get the analyzer to use, if previously set, otherwise {@code null} */
    public String analyzer() {
        return this.analyzer;
    }

    public MultistepScoreBuilder base(double base) {
        this.base = base;
        return this;
    }

    public double base() {
        return base;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.startObject(fieldName);
        builder.field(QUERY_FIELD.getPreferredName(), value);
        if (analyzer != null) {
            builder.field(ANALYZER_FIELD.getPreferredName(), analyzer);
        }
        if (base != null) {
            builder.field(BASE_FIELD.getPreferredName(), base);
        }
        builder.field(ZERO_TERMS_QUERY_FIELD.getPreferredName(), zeroTermsQuery.toString());
        printBoostAndQueryName(builder);
        builder.endObject();
        builder.endObject();
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        // validate context specific fields
        if (analyzer != null && context.getIndexAnalyzers().get(analyzer) == null) {
            throw new QueryShardException(context, "[" + NAME + "] analyzer [" + analyzer + "] not found");
        }

        MultistepScoreQuery multistepScoreQuery = new MultistepScoreQuery(context);
        if (analyzer != null) {
            multistepScoreQuery.setAnalyzer(analyzer);
        }
        if (base != null) {
            if (base <= 1) {
                throw new IllegalArgumentException("[" + NAME + "] requires base must bigger than 1, but got " + base);
            }
            multistepScoreQuery.setBase(base);
        }
        multistepScoreQuery.setZeroTermsQuery(zeroTermsQuery);
        return multistepScoreQuery.parse(fieldName, value);
    }

    @Override
    protected boolean doEquals(MultistepScoreBuilder other) {
        return Objects.equals(fieldName, other.fieldName) &&
                Objects.equals(value, other.value) &&
                Objects.equals(analyzer, other.analyzer) &&
                Objects.equals(zeroTermsQuery, other.zeroTermsQuery) &&
                Objects.equals(base, other.base);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, value, analyzer, zeroTermsQuery, base);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public static MultistepScoreBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        Object value = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        Double base = null;
        String analyzer = null;
        MultistepScoreQuery.ZeroTermsQuery zeroTermsQuery = MultistepScoreQuery.DEFAULT_ZERO_TERMS_QUERY;
        String queryName = null;
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
                        } else if (ANALYZER_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            analyzer = parser.text();
                        } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            boost = parser.floatValue();
                        } else if (BASE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            base = parser.doubleValue();
                        } else if (ZERO_TERMS_QUERY_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            String zeroTermsValue = parser.text();
                            if ("none".equalsIgnoreCase(zeroTermsValue)) {
                                zeroTermsQuery = MultistepScoreQuery.ZeroTermsQuery.NONE;
                            } else if ("all".equalsIgnoreCase(zeroTermsValue)) {
                                zeroTermsQuery = MultistepScoreQuery.ZeroTermsQuery.ALL;
                            } else {
                                throw new ParsingException(parser.getTokenLocation(),
                                        "Unsupported zero_terms_query value [" + zeroTermsValue + "]");
                            }
                        } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            queryName = parser.text();
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[" + NAME + "] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[" + NAME + "] unknown token [" + token + "] after [" + currentFieldName + "]");
                    }
                }
            } else {
                throwParsingExceptionOnMultipleFields(NAME, parser.getTokenLocation(), fieldName, parser.currentName());
                fieldName = parser.currentName();
                value = parser.objectText();
            }
        }
        if (value == null) {
            throw new ParsingException(parser.getTokenLocation(), "No text specified for text query");
        }
        MultistepScoreBuilder multistepScoreBuilder = new MultistepScoreBuilder(fieldName, value);
        multistepScoreBuilder.analyzer(analyzer);
        multistepScoreBuilder.zeroTermsQuery(zeroTermsQuery);
        multistepScoreBuilder.queryName(queryName);
        multistepScoreBuilder.boost(boost);
        if (base != null) {
            multistepScoreBuilder.base(base);
        }
        return multistepScoreBuilder;
    }
}
