package com.wade.ops.harmonius;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/04
 */
public class Relation {

    /**
     * 依赖的服务 K:依赖发生的时间, V: 依赖的服务名
     */
    private Map<String, String> dependService;

    /**
     * 被什么服务依赖 K:被依赖的时间, V: 被依赖的服务名
     */
    private Map<String, String> beDependService;

    /**
     * 被什么菜单依赖 K:被依赖的时间, V: 被依赖的菜单名
     */
    private Map<String, String> beDependMenuId;

    public Relation() {
        dependService = new HashMap<>();
        beDependService = new HashMap<>();
        beDependMenuId = new HashMap<>();
    }

    /**
     * 获取依赖的服务
     *
     * @return
     */
    public Map<String, String> getDependService() {
        return dependService;
    }

    /**
     * 获取被依赖的服务
     *
     * @return
     */
    public Map<String, String> getBeDependService() {
        return beDependService;
    }

    /**
     * 获取被依赖的菜单
     *
     * @return
     */
    public Map<String, String> getBeDependMenuId() {
        return beDependMenuId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(2000);
        sb.append("dependService: " + dependService + "\n");
        sb.append("beDependService " + beDependService + "\n");
        sb.append("beDependMenuId: " + beDependMenuId + "\n");
        return sb.toString();
    }

}
