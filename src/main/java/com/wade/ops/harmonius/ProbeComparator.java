package com.wade.ops.harmonius;

import java.util.Comparator;
import java.util.HashMap;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc: 探针比较器
 * @auth: steven.zhou
 * @date: 2017/09/14
 */
public final class ProbeComparator implements Comparator {

    @Override
    public int compare(Object o1, Object o2) {
        HashMap<String, Object> m1 = (HashMap<String, Object>) o1;
        HashMap<String, Object> m2 = (HashMap<String, Object>) o2;
        String starttime1 = (String) m1.get("starttime");
        String starttime2 = (String) m2.get("starttime");
        return starttime1.compareTo(starttime2);
    }

}
