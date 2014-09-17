package com.elex.ba.main;

import com.elex.ba.job.*;
import com.elex.ba.util.Utils;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * 离线计算月活跃/周活跃
 * Date: 14-7-23
 * Time: 下午6:43
 */
public class Main {

    public static void  main(String[] args) throws Exception {

        System.out.println(args.length + " - " + args[0]);

        if("yac".equals(args[0]) ){
            new LoadYacURLJob().run();
        }

 /*       if(args.length != 2){
            System.err.println("Usage : date type");
            System.exit(-1);
        }

        String date =  args[0];
        String type = args[1];

        long begin = System.currentTimeMillis();
        System.out.println("begin analyze");
        //获取项目列表
        File file = new File(Main.class.getClassLoader().getResource("projects").getFile());
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        String json = "";

        while ((line = reader.readLine()) != null) {
            json += line;
        }

        Map<String,String> pjson = new Gson().fromJson(json, Map.class);
        Map<String,Set<String>> projects = new HashMap<String,Set<String>>();
        Set<String> allpids = new HashSet<String>();
        for(Map.Entry<String,String> kv : pjson.entrySet()){
            Set<String> pids = new HashSet<String>();
            String[] p = kv.getValue().split(",");
            for(String pid : p){
                pids.add(pid.trim());
            }
            projects.put(kv.getKey(), pids);
            allpids.addAll(pids);
        }

        if("hbase".equals(type)){
            //load UID
            loadHBaseUID(date, allpids, 1);
            //combie UID
            uidCombine(date, allpids);
            //combie Project
            projectCombine(date, projects);
        }else if("trans".equals(type)){
            //combie UID
            uidCombine(date, allpids);
        }else if("comb".equals(type)){
            //combie Project
            projectCombine(date, projects);
        }else if("wau".equals(type)){ //周活跃
            loadHBaseUID(date, allpids, 1);
            uidCombine(date, allpids);
            projectCombine(date, projects);
            projectCount(date, projects.keySet(), 7);
        }else if("mau".equals(type)){ //月活跃
            loadHBaseUID(date, allpids, 1);
            uidCombine(date, allpids);
            projectCombine(date, projects);
            projectCount(date, projects.keySet(), 30);
        }else if("batchload".equals(type)){
            //首次运行离线导出30天的UID，按天按大项目的类别进行存储
            for(int i=0; i<30; i++){
                if(i >0){
                    date = Utils.getLastDate(date);
                }
                loadHBaseUID(date, allpids, 1);
                uidCombine(date, allpids);
                projectCombine(date, projects);
            }
        }else if("registuid".equals(type)){
            transRegistUID(date, allpids);
        }else if("registcombine".equals(type)){
            registUserCombine(date, projects);
        }

        System.out.println("End analyze , spend " + (System.currentTimeMillis() - begin) );*/

    }

    public static void loadHBaseUID(String date,Set<String> projects,int offset) throws ParseException {
        ExecutorService service = new ThreadPoolExecutor(16,16,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        String[] days = Utils.getLastDate(date,30);
        for(String pid : projects){
            for(int i =0;i<16; i++){
                tasks.add(service.submit(new LoadHBaseUIDJob(days, "node" + i, pid)));
            }
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("loadHBaseUID finished");
    }

    public static void uidCombine(String date, Set<String> projects) throws ParseException {
        ExecutorService service = new ThreadPoolExecutor(25,40,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
//        String[] days = Utils.getLastDate(date,30);
//        for(String d : days){
            for(String project : projects){
                tasks.add(service.submit(new UIDCombineJob(date,project)));
            }
//        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("uidCombine finished");
    }

    public static void transRegistUID(String date, Set<String> projects) throws ParseException {
        ExecutorService service = new ThreadPoolExecutor(25,40,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();

        for(String project : projects){
            tasks.add(service.submit(new RegistUserJob(date,project)));
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("uidCombine finished");
    }

    public static void projectCombine(String date, Map<String,Set<String>> projects) throws ParseException {
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        String[] days = Utils.getLastDate(date,3);
        for(String p : projects.keySet()){
            tasks.add(service.submit(new ProjectCombineJob(days, p, projects.get(p))));
        }


        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("projectCombine finished");
    }

    public static void registUserCombine(String date, Map<String,Set<String>> projects) throws ParseException {
        ExecutorService service = new ThreadPoolExecutor(20,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String p : projects.keySet()){
            tasks.add(service.submit(new RegistUserCombineJob(date, p, projects.get(p))));
        }
        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("registUserCombine finished");
    }

    public static void projectCount(String date, Set<String> projects, int range){
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String p : projects){
            tasks.add(service.submit(new ProjectCountJob(date,p, range)));
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("projectCount finished");
    }
}
