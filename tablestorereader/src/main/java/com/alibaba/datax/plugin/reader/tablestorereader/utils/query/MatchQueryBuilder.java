package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.MatchQuery;
import com.alicloud.openservices.tablestore.model.search.query.QueryOperator;

public class MatchQueryBuilder implements QueryBuilder<MatchQuery> {
    private static String fieldName = "fieldName";
    private static String text = "text";
    private static String minimumShouldMatch = "minimumShouldMatch";
    private static String operator = "operator";

    @Override
    public MatchQuery build(Configuration configuration) throws Exception {

        MatchQuery matchPhraseQuery = new MatchQuery();

        matchPhraseQuery.setFieldName(configuration.getString(fieldName));
        matchPhraseQuery.setText(configuration.getString(text));
        matchPhraseQuery.setMinimumShouldMatch(configuration.getInt(minimumShouldMatch));

        QueryOperator queryOperator = TableStoreDataTypeUtil.queryOperatorType(configuration.getString(operator));

        if (null == queryOperator) {
            throw new NoColumnTypeFoundException();
        }

        matchPhraseQuery.setOperator(queryOperator);

        return matchPhraseQuery;
    }
}
