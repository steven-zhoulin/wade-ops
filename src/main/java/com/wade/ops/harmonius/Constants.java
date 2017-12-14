package com.wade.ops.harmonius;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public interface Constants {
    String HT_TRACE = "trace";
    String HT_TRACE_MENU = "trace_menu";
    String HT_TRACE_OPERID = "trace_operid";
    String HT_TRACE_SN = "trace_sn";
    String HT_TRACE_SERVICE = "trace_service";
    String HT_SERVICE_MAP = "service_map";

    String dependService = "dependService|";
    String beDependService = "beDependService|";
    String beDependMenuId = "beDependMenuId|";

    byte[] NULL_BYTES = Bytes.toBytes("");
}
