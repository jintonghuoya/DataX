package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.MatchPhraseQuery;

public class MatchPhraseQueryBuilder implements QueryBuilder<MatchPhraseQuery> {
    private static String fieldName = "fieldName";
    private static String text = "text";

    @Override
    public MatchPhraseQuery build(Configuration configuration) throws Exception {

        MatchPhraseQuery matchPhraseQuery = new MatchPhraseQuery();
        matchPhraseQuery.setFieldName(configuration.getString(fieldName));
        matchPhraseQuery.setText(configuration.getString(text));

        return matchPhraseQuery;
    }
}
