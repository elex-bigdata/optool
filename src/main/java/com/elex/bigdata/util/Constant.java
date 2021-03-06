package com.elex.bigdata.util;

import java.text.SimpleDateFormat;

import com.google.gson.Gson;

/**
 * Author: liqiang
 * Date: 14-4-29
 * Time: 下午4:37
 */
public interface Constant {
    public static final String[] xa_cluster = {"node0","node1","node2","node3","node4","node5","node6","node7",
                                                "node8","node9","node10","node11","node12","node13","node14","node15"};

    public static SimpleDateFormat dfmt = new SimpleDateFormat("yyyyMMddHHmmss");
    public static Gson gson = new Gson();
}
