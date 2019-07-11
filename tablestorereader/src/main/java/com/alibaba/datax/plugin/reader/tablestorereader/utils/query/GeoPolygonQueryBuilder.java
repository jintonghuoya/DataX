package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.GeoPolygonQuery;

public class GeoPolygonQueryBuilder implements QueryBuilder<GeoPolygonQuery> {

    private static String fieldName = "fieldName";
    private static String points = "points";

    @Override
    public GeoPolygonQuery build(Configuration configuration) throws Exception {
        GeoPolygonQuery geoBoundingBoxQuery = new GeoPolygonQuery();
        geoBoundingBoxQuery.setFieldName(configuration.getString(fieldName));
        geoBoundingBoxQuery.setPoints(configuration.getList(points, String.class));

        return geoBoundingBoxQuery;
    }
}
