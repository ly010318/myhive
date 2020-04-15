package com.foo.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;

public class BaseFieldUDF extends UDF {
    public String evaluate(String line, String jsonkeysString) {
        StringBuilder sb = new StringBuilder();

        // 1 切割 jsonkeys mid uid vc vn l sr os ar md
        String[] jsonkeys = jsonkeysString.split(",");
        // 2 处理 line 服务器时间 | json
        String[] longContents = line.split("\\|");
        //3 合法性校验
        if (longContents.length != 2 || StringUtils.isBlank(longContents[1])) {
            return "";
        }
        // 4 开始处理 json
        try {
            JSONObject jsonObject = new JSONObject(longContents[1]);
            // 获取 cm 里面的对象
            JSONObject base = jsonObject.getJSONObject("cm");
            // 循环遍历取值
            for (int i = 0; i < jsonkeys.length; i++) {
                String filedName = jsonkeys[i].trim();
                if (base.has(filedName)) {
                    sb.append(base.getString(filedName)).append("\t");
                } else {
                    sb.append("\t");
                }
            }
            sb.append(jsonObject.getString("et")).append("\t");
            sb.append(longContents[0]).append("\t");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        String line = "1541217850324|{\\\"cm\\\":{\\\"mid\\\":\\\"m7856\\\",\\\"uid\\\":\\\"u8739\\\",\\\"ln\\\":\\\"-\n" +
                "74.8\\\",\\\"sv\\\":\\\"V2.2.2\\\",\\\"os\\\":\\\"8.1.3\\\",\\\"g\\\":\\\"P7XC9126@gmail.com\\\",\\\"nw\\\":\\\"3G\\\n" +
                "\",\\\"l\\\":\\\"es\\\",\\\"vc\\\":\\\"6\\\",\\\"hw\\\":\\\"640*960\\\",\\\"ar\\\":\\\"MX\\\",\\\"t\\\":\\\"1541204134250\\\n" +
                "\",\\\"la\\\":\\\"-31.7\\\",\\\"md\\\":\\\"huawei-\n" +
                " 17\\\",\\\"vn\\\":\\\"1.1.2\\\",\\\"sr\\\":\\\"O\\\",\\\"ba\\\":\\\"Huawei\\\"},\\\"ap\\\":\\\"weather\\\",\\\"et\\\":[{\\\n" +
                " \"ett\\\":\\\"1541146624055\\\",\\\"en\\\":\\\"display\\\",\\\"kv\\\":{\\\"goodsid\\\":\\\"n4195\\\",\\\"copyrig\n" +
                " ht\\\":\\\"ESPN\\\",\\\"content_provider\\\":\\\"CNN\\\",\\\"extend2\\\":\\\"5\\\",\\\"action\\\":\\\"2\\\",\\\"ext\n" +
                "  end1\\\":\\\"2\\\",\\\"place\\\":\\\"3\\\",\\\"showtype\\\":\\\"2\\\",\\\"category\\\":\\\"72\\\",\\\"newstype\\\":\\\"\n" +
                " 5\\\"}},{\\\"ett\\\":\\\"1541213331817\\\",\\\"en\\\":\\\"loading\\\",\\\"kv\\\":{\\\"extend2\\\":\\\"\\\",\\\"load\n" +
                " ing_time\\\":\\\"15\\\",\\\"action\\\":\\\"3\\\",\\\"extend1\\\":\\\"\\\",\\\"type1\\\":\\\"\\\",\\\"type\\\":\\\"3\\\",\\\n" +
                "\"loading_way\\\":\\\"1\\\"}},{\\\"ett\\\":\\\"1541126195645\\\",\\\"en\\\":\\\"ad\\\",\\\"kv\\\":{\\\"entry\\\":\\\n" +
                "\"3\\\",\\\"show_style\\\":\\\"0\\\",\\\"action\\\":\\\"2\\\",\\\"detail\\\":\\\"325\\\",\\\"source\\\":\\\"4\\\",\\\"behavior\\\":\\\"2\\\",\\\"content\\\":\\\"1\\\",\\\"newstype\\\":\\\"5\\\"}},{\\\"ett\\\":\\\"1541202678812\\\",\\\"\n" +
                " en\\\":\\\"notification\\\",\\\"kv\\\":{\\\"ap_time\\\":\\\"1541184614380\\\",\\\"action\\\":\\\"3\\\",\\\"type\n" +
                "\\\":\\\"4\\\",\\\"content\\\":\\\"\\\"}},{\\\"ett\\\":\\\"1541194686688\\\",\\\"en\\\":\\\"active_background\\\"\n" +
                ",\\\"kv\\\":{\\\"active_source\\\":\\\"3\\\"}}]};";
        String x = new BaseFieldUDF().evaluate(line,
                "mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,nw,ln,la,t");
        System.out.println(x);
    }
}

