package com.elex.bigdata.servlet;

import com.elex.bigdata.service.OpService;
import com.elex.bigdata.uid.HbaseMysqlUIDTruncator;
import com.elex.bigdata.uid.StreamLogUidTransformer;
import com.elex.bigdata.util.Constant;
import com.google.gson.Gson;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Author: liqiang
 * Date: 14-4-29
 * Time: 下午4:33
 */
public class OpServlet extends HttpServlet {
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        if ("dload".equals(req.getParameter("action"))) {
            drill(req, resp);
        }else if("drc".equals(req.getParameter("action"))){

        }else if("drs".equals(req.getParameter("action"))){

        }else if("tuid".equals(req.getParameter("action"))){
            transformUID(req, resp);
        }else if("convertTime".equals(req.getParameter("action"))){
            convertTime(req, resp);
        }

    }

    private void drill(HttpServletRequest req, HttpServletResponse resp) {

        OpService service = new OpService();

        String json = "";
        try {
            List<Map<String,String>> result = new ArrayList<Map<String, String>>();
            Map<String, String> status = service.getDrillStatus();
            Map<String, String> line = null;
            for(String node : Constant.xa_cluster){
                String value = status.get(node);
                if(status.get(node) == null){
                    value = "-1";
                }
                line = new HashMap<String, String>();
                line.put("name",node);
                line.put("status",value);
                result.add(line);
            }
            //json = "{'success':'true','result':" + new Gson().toJson(result) + "}";
            json = "{\"success\":\"true\",\"result\" :" + new Gson().toJson(result)+ "}";
        } catch (Exception e) {
            json = "{\"success\":\"false\",\"result\" :\"" + e.getMessage() + "\"}";
            e.printStackTrace();
        }

        try {
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(json);
            System.out.println(json);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void transformUID(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String[] uids = req.getParameter("uids").trim().split(",");
        String project = req.getParameter("project");
        String idType = req.getParameter("idtype");
        Map<Long, String> results = null;
        try {
            StreamLogUidTransformer transformer = StreamLogUidTransformer.INSTANCE;
            if("hash".equals(idType)){
                List<Long> longUid = HbaseMysqlUIDTruncator.truncate(Arrays.asList(uids));
                System.out.println("LongUIDS : " + longUid);

                results = transformer.transform(project,longUid);
            }else{
                results = transformer.transform(project,uids);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage(),e.getCause());
        }

        try {
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(new Gson().toJson(results));
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void convertTime(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Map<String,String> result = new HashMap<String, String>();
        try{
            long time = Long.parseLong(req.getParameter("time"));

            Date d = new Date(time);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String formatDate = sdf.format(d);
            result.put("success","true");
            result.put("result",formatDate);
        }catch(Exception e){
            result.put("success","false");
            result.put("msg",e.getMessage());
        }


        try {
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(new Gson().toJson(result));
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
