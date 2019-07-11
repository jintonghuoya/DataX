package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.NestedQuery;
import com.alicloud.openservices.tablestore.model.search.query.ScoreMode;

public class NestedQueryBuilder implements QueryBuilder<NestedQuery> {

    private static String path = "path";
    private static String query = "query";
    private static String scoreMode = "scoreMode";

    @Override
    public NestedQuery build(Configuration configuration) throws Exception {

        NestedQuery nestedQuery = new NestedQuery();

        nestedQuery.setPath(configuration.getString(path));
        nestedQuery.setQuery(QueryFactory.build(configuration.getConfiguration(query), true));

        ScoreMode scoreMode = TableStoreDataTypeUtil.scoreModeType(configuration.getString(NestedQueryBuilder.scoreMode));

        nestedQuery.setScoreMode(scoreMode);

        return nestedQuery;
    }
}
