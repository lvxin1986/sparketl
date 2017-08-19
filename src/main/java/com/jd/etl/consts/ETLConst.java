package com.jd.etl.consts;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by root on 17-8-14.
 */
public interface ETLConst {
    public static final String ETL_THREADS = "etl.mysql2orc.threads.num";
    public static final String ETL_THREADS_DEFAUL_VALUE = "8";
    public static final String ETL_TARGET_TABLE = "etl.target.table";
    public static final String ETL_TARGET_PARTITION = "etl.target.partition";
    public static final String ETL_TARGET_PATH = "etl.target.path";
    public static final String ETL_COALESCE = "etl.mysql2orc.orc.coalesce";
    public static final String ETL_COALESCE_DEFAULT_VALUE = "0";

    public static  Set<String> COL_TYPE_NUMBER = new HashSet<String>() {{
        add("INT");
        add("BIGINT");
        add("LONG");
    }};

}
