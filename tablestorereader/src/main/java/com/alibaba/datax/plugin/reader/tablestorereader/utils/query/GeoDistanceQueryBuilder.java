package com.alibaba.datax.plugin.reader.tablestorereader.utils.query;

import com.alibaba.datax.common.util.Configuration;
import com.alicloud.openservices.tablestore.model.search.query.GeoBoundingBoxQuery;
import com.alicloud.openservices.tablestore.model.search.query.GeoDistanceQuery;

public class GeoDistanceQueryBuilder implements QueryBuilder<GeoDistanceQuery>  {

    private static String fieldName = "fieldName";
    private static String distanceInMeter = "distanceInMeter";
    private static String centerPoint = "centerPoint";

    @Override
    public GeoDistanceQuery build(Configuration configuration) throws Exception {
        GeoDistanceQuery geoBoundingBoxQuery = new GeoDistanceQuery();
        geoBoundingBoxQuery.setCenterPoint(configuration.getString(centerPoint));
        geoBoundingBoxQuery.setFieldName(configuration.getString(fieldName));
        geoBoundingBoxQuery.setDistanceInMeter(configuration.getDouble(distanceInMeter));

        return geoBoundingBoxQuery;
    }
}
