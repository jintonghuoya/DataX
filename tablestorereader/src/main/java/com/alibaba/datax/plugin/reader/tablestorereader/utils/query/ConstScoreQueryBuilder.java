package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.ConstScoreQuery;

public class ConstScoreQueryBuilder implements QueryBuilder<ConstScoreQuery> {

    private static String FILTER = "filter";

    @Override
    public ConstScoreQuery build(Configuration configuration) throws Exception {

        ConstScoreQuery constScoreQuery = new ConstScoreQuery();

        Configuration filter = configuration.getConfiguration(FILTER);

        constScoreQuery.setFilter(QueryFactory.build(filter, true));

        return constScoreQuery;
    }
}
