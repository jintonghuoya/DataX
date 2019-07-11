package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;

public class GeoBoundingBoxQueryBuilder implements QueryBuilder<GeoBoundingBoxQuery>  {

    private static String fieldName = "fieldName";
    private static String topLeft = "topLeft";
    private static String bottomRight = "bottomRight";

    @Override
    public GeoBoundingBoxQuery build(Configuration configuration) throws Exception {
        GeoBoundingBoxQuery geoBoundingBoxQuery = new GeoBoundingBoxQuery();
        geoBoundingBoxQuery.setBottomRight(configuration.getString(bottomRight));
        geoBoundingBoxQuery.setFieldName(configuration.getString(fieldName));
        geoBoundingBoxQuery.setTopLeft(configuration.getString(topLeft));

        return geoBoundingBoxQuery;
    }
}
