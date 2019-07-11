package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.Query;

public class QueryFactory {

    private static String CLASS = "queryClass";
    private static String DEFAULT_CLASS = "MatchAllQuery";

    /**
     * 构建Query
     * <p>
     * todo 构建过程可使用类路劲加载，避免switch硬编码
     *
     * @param param
     * @param allowNull 是否允许空对象
     * @return
     * @throws Exception
     */
    public static Query build(Configuration param, boolean allowNull) throws Exception {

        String aClass = null;

        if (null == param) {

            if (allowNull) {
                return null;
            }

            aClass = DEFAULT_CLASS;
        } else {
            aClass = param.getString(CLASS);

            if (null == aClass) {
                aClass = DEFAULT_CLASS;
            }
        }

        QueryBuilder builder = null;

        switch (aClass.toUpperCase()) {
            case "TERMQUERY":
                builder = new TermQueryBuilder();
                break;
            case "MATCHALLQUERY":
                builder = new MatchAllQueryBuilder();
                break;
            case "TERMSQUERY":
                builder = new TermsQueryBuilder();
                break;
            case "BOOLQUERY":
                builder = new BoolQueryBuilder();
                break;
            case "FUNCTIONSCOREQuERY":
                builder = new FunctionScoreQueryBuilder();
                break;
            case "GEOBOUNDINGBOXQUERY":
                builder = new GeoBoundingBoxQueryBuilder();
                break;
            case "GeoDistanceQuery":
                builder = new GeoDistanceQueryBuilder();
                break;
            case "MATCHPHRASEQUERY":
                builder = new MatchPhraseQueryBuilder();
                break;
            case "GEOPOLYGONQUERY":
                builder = new GeoPolygonQueryBuilder();
                break;
            case "MATCHQUERY":
                builder = new MatchQueryBuilder();
                break;
            case "CONSTSCOREQUERY":
                builder = new ConstScoreQueryBuilder();
                break;
            case "PREFIXQUERY":
                builder = new PrefixQueryBuilder();
                break;
            case "WILDCARDQUERY":
                builder = new WildcardQueryBuilder();
                break;
            case "NESTEDQUERY":
                builder = new NestedQueryBuilder();
                break;
            case "RANGEQUERY":
                builder = new RangeQueryBuilder();
                break;
        }

        if (null == builder) {
            throw new NoQueryBuilderFoundException("class :" + aClass + "not found");
        }

        return builder.build(param);
    }

    public static void main(String[] args) throws Exception {

        String config = "{\"query\": {\n" +
                "                            \"fieldName\": \"okok\",\n" +
                "                            \"queryClass\": \"TermQuery\",\n" +
                "                            \"term\": {\n" +
                "                                \"type\": \"STRING\",\n" +
                "                                \"value\": \"value\"\n" +
                "                            }\n" +
                "                        }}";

        config = "{\"query\": {\n" +
                "                            \"shouldQueries\": [\n" +
                "                                {\n" +
                "                                    \"fieldName\": \"okok\",\n" +
                "                                    \"term\": {\n" +
                "                                        \"type\": \"STRING\",\n" +
                "                                        \"value\": \"value\"\n" +
                "                                    },\n" +
                "                                    \"queryClass\": \"TermQuery\"\n" +
                "                                }\n" +
                "                            ],\n" +
                "                            \"queryClass\": \"BoolQuery\",\n" +
                "                            \"minimumShouldMatch\": 1\n" +
                "                        }}";
        Query build = build(Configuration.from(config).getConfiguration("query"), true);

        System.out.println(build);



    }
}
