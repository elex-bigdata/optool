package com.elex.bigdata.servlet;

import com.elex.bigdata.service.ADSearchService;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

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
        }

    }

    private void countHit(HttpServletRequest req, HttpServletResponse resp){
        String startTime = req.getParameter("startTime");
        String endTime = req.getParameter("endTime");
        String nation = req.getParameter("nation");
        String pid = req.getParameter("pid");

        String tableName = "ad_all_log";

        System.out.println(startTime + " : " + endTime + " : " + nation + " : " + pid);

        ADSearchService service = new ADSearchService();
        try {
            String result = service.countHit(tableName,Integer.parseInt(pid),Long.parseLong(startTime),Long.parseLong(endTime),nation.toUpperCase());
            int count = service.count(tableName,Integer.parseInt(pid),Long.parseLong(startTime),Long.parseLong(endTime),nation.toUpperCase());
            PrintWriter pw = new PrintWriter(resp.getOutputStream());
            pw.write(result + ",total:" + count);
            pw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
