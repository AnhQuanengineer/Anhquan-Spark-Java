package org.datapipeline.Spark_sql.ultis;

import org.apache.spark.sql.api.java.UDF1;

public class Udf implements UDF1<Integer, Integer> {
    @Override
    public Integer call(Integer data) throws Exception {
        if(data == null){
            return null;
        }
        return data + 1;
    }
}
