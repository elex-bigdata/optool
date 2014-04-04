package com.elex.bigdata.servlet;

import com.elex.bigdata.service.ADSearchService;
import com.elex.bigdata.util.MetricMapping;
import com.google.gson.Gson;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Author: liqiang
 * Date: 14-4-3
 * Time: 下午5:54
 */
public class ADSearchServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {

        if("hit".equals(req.getParameter("action"))){
            countHit(req,resp);
        }else if("prj".equals(req.getParameter("action"))){
            getProject(resp);
        }

    }

    private void getProject(HttpServletResponse resp){
        Map<String,Byte> projects = MetricMapping.getInstance().getAllProjectShortNameMapping();
        String result = new Gson().toJson(projects);
        try {
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(result);
            pw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void countHit(HttpServletRequest req, HttpServletResponse resp){

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

        String startTime = req.getParameter("startTime");
        String endTime = req.getParameter("endTime");
        String nation = req.getParameter("nation");
        String pid = req.getParameter("pid");

        String tableName = "ad_all_log";

        System.out.println(startTime + " : " + endTime + " : " + nation + " : " + pid);

        long start = 0;
        long end = 0;
        try {
            start = sdf.parse(startTime).getTime();
            end = sdf.parse(endTime).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }


        ADSearchService service = new ADSearchService();
        try {
            String result = service.countHit(tableName,Integer.parseInt(pid),start,end,nation);
            int count = service.count(tableName,Integer.parseInt(pid),start,end,nation);
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(result + ",total:" + count);
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
