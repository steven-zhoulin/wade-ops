package com.wade.ops.harmonius;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class RelationBuf {

    private Map<String, AtomicLong> dependService = new HashMap<>();
    private Map<String, AtomicLong> beDependService = new HashMap<>();
    private Map<String, AtomicLong> beDependMenuId = new HashMap<>();

    public Map<String, AtomicLong> getDependService() {
        return dependService;
    }

    public Map<String, AtomicLong> getBeDependService() {
        return beDependService;
    }

    public Map<String, AtomicLong> getBeDependMenuId() {
        return beDependMenuId;
    }

}
