package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.WildcardQuery;

public class WildcardQueryBuilder implements QueryBuilder<WildcardQuery> {
    private static String fieldName = "fieldName";
    private static String value = "value";

    @Override
    public WildcardQuery build(Configuration configuration) throws Exception {

        WildcardQuery wildcardQuery = new WildcardQuery();

        wildcardQuery.setFieldName(configuration.getString(fieldName));
        wildcardQuery.setValue(configuration.getString(value));

        return wildcardQuery;
    }
}
