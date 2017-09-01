package com.jd.etl.consts;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by root on 17-8-14.
 */
public interface ETLConst {
    public static final String ETL_THREADS = "etl.threads.num";
    public static final String ETL_THREADS_DEFAUL_VALUE = "8";
    public static final String ETL_TARGET_TABLE = "etl.target.table";
    public static final String ETL_TARGET_PARTITION = "etl.target.partition";
    public static final String ETL_TARGET_PATH = "etl.target.path";
    public static final String ETL_COALESCE = "etl.orc.coalesce";
    public static final String ETL_COALESCE_DEFAULT_VALUE = "0";

    public static final String ETL_PROPERTIES_FILE = "/etl.properties";
    public static final String ETL_TABLELIST_FILE = "/tables.list";
    public static final String ETL_SCHEMALIST_FILE = "/schemas.list";
    public static final String ETL_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /*public static final String ETL_SAMPLE_TABLE = "etl.sample.table";
    public static final String ETL_TEMP_DATABASE = "etl.tmp.db";
    public static final String ETL_TEMP_PATH = "etl.tmp.path";

    public static final String ETL_SQL_COLUMNS = "etl.sql.columns";*/

    public static  Set<String> COL_TYPE_NUMBER = new HashSet<String>() {{
        add("INT");
        add("BIGINT");
        add("LONG");
    }};

}
