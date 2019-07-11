package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.PrefixQuery;

public class PrefixQueryBuilder implements QueryBuilder<PrefixQuery> {
    private static String fieldName = "fieldName";
    private static String prefix = "prefix";

    @Override
    public PrefixQuery build(Configuration configuration) throws Exception {

        PrefixQuery prefixQuery = new PrefixQuery();

        prefixQuery.setFieldName(configuration.getString(fieldName));
        prefixQuery.setPrefix(configuration.getString(prefix));

        return prefixQuery;
    }
}
