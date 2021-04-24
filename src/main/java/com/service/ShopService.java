package com.service;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.trident.kfkConfig.TableAndColumn;
import com.trident.kfkDRPC.DrpcService;
import com.trident.kfkTridentUtil.KfkUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2018/6/20.
 */
public class ShopService {


    /**
     * 店铺排行
     *
     * @return
     */
    public static Object getShop_ranking() {

        List<Map> resultList = new ArrayList<Map>();
        Object jsonString = null;
        try {
            String result = DrpcService.getDRPCClient().execute(TableAndColumn.DRPCTxid_shop_ranking, "top10_fromAll");

            JSONArray jsonObject = JSONArray.parseArray(result);

            for (int i = 0; i < jsonObject.size(); i++) {
                List list = (List) jsonObject.get(i);
                Map<String, Object> map = (Map<String, Object>) list.get(2);
                resultList.add(map);
            }

            jsonString = JSONObject.toJSONString(resultList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    /**
     * 汇总量
     *
     * @return
     */
    public static Object getAll_amt() {

        List<Map> resultList = new ArrayList<Map>();
        Object jsonString = null;
        try {
            String result = DrpcService.getDRPCClient().execute(TableAndColumn.DRPCTxid_all_Amt, TableAndColumn._column_all_amt_key);

            //String result = "[[\"kfk-all\",\"kfk-all\",29949510]]";
            JSONArray jsonObject = JSONArray.parseArray(result);

            if (jsonObject.size() > 0) {
                List valueList = (List) jsonObject.get(0);
                jsonString = (valueList.size() > 0) ? valueList.get(2) : "";
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    /**
     * 曲线轴汇总量
     *
     * @return
     */
    public static Object getXtime_amt() {

        List<Map> resultList = new ArrayList<Map>();
        Object jsonString = null;
        try {


            String lazyTime = KfkUtil.getLazyCurrTime(2000);

            System.out.println("xtime 查询的时间："+lazyTime);
            //String result = " [[\"20180622-23:45:13\",\"20180622-23:45:13\",{\"shop_amtSum\":\"29977990\",\"x_time\":\"20180622-23:45:13\"}]]";
            String result = DrpcService.getDRPCClient().execute(TableAndColumn.DRPCTxid_xtime_Amt, lazyTime);
            System.out.println("xtime 查询的时间："+result);
            JSONArray jsonObject = JSONArray.parseArray(result);

            for (int i = 0; i < jsonObject.size(); i++) {

                List list = (List) jsonObject.get(0);
                Map<String, Object> map = (Map<String, Object>) list.get(2);
                if (!"null".equals(String.valueOf(map.get("x_time")))) {
                    CacheXTime.cacheTime.add(map);
                }else{
                    return JSONObject.toJSONString(CacheXTime.cacheTime);
                }
                resultList.add(map);
            }

            jsonString = JSONObject.toJSONString(resultList);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jsonString;
    }

    public static void main(String[] args) {
        getXtime_amt();
    }

}
