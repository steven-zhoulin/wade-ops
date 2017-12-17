package com.wade.ops.harmonius;

import java.util.ArrayList;
import java.util.List;
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
     * 依赖的服务
     *
     * K1:'dependService', V1: '依赖的服务名^时间' |  K2:'count', V2:'依赖次数'
     *
     */
    private List<Map<String, String>> dependService;

    /**
     * 被什么服务依赖
     *
     * K1:'beDependService', V1: '被依赖的服务名^[时间|mainservice]' |  K2:'count', V2:'被依赖次数'
     *
     */
    private List<Map<String, String>> beDependService;

    /**
     * 被什么菜单依赖
     *
     * K1:'beDependMenuId', V1: '依赖的菜单名^时间' |  K2:'count', V2:'被依赖次数'
     *
     */
    private List<Map<String, String>> beDependMenuId;

    public Relation() {
        dependService = new ArrayList<>();
        beDependService = new ArrayList<>();
        beDependMenuId = new ArrayList<>();
    }

    /**
     * 获取依赖的服务
     *
     * @return
     */
    public List<Map<String, String>> getDependService() {
        return dependService;
    }

    /**
     * 获取被依赖的服务
     *
     * @return
     */
    public List<Map<String, String>> getBeDependService() {
        return beDependService;
    }

    /**
     * 获取被依赖的菜单
     *
     * @return
     */
    public List<Map<String, String>> getBeDependMenuId() {
        return beDependMenuId;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(2000);
        sb.append(dependService + "\n");
        sb.append(beDependService + "\n");
        sb.append(beDependMenuId + "\n");
        return sb.toString();
    }

}
