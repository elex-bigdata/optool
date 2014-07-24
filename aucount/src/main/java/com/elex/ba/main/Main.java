package com.elex.ba.main;

import com.elex.ba.job.LoadHBaseUIDJob;
import com.elex.ba.job.ProjectCombineJob;
import com.elex.ba.job.ProjectCountJob;
import com.elex.ba.job.UIDCombineJob;
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
        if(args.length != 1){
            System.err.println("Usage : date");
            System.exit(-1);
        }

        String date =  args[0];

        long begin = System.currentTimeMillis();
        System.out.println("begin analyze");
        File file = new File(Main.class.getClassLoader().getResource("projects").getFile());
//        File pfile = new File(fpath);
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
        //load UID
        loadHBaseUID(date, allpids);

        //combie UID
        uidCombine(date, allpids);

        //combie Project
        projectCombine(date, projects);

        //count
        projectCount(date, projects.keySet());
        System.out.println("End analyze , spend " + (System.currentTimeMillis() - begin) );

    }

    public static void loadHBaseUID(String date,Set<String> projects) throws ParseException {
        ExecutorService service = new ThreadPoolExecutor(16,16,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        String[] timeRange = Utils.getDateRange(date,30);
        for(int i =0;i<16; i++){
            tasks.add(service.submit(new LoadHBaseUIDJob(date, timeRange, "node" + i, projects)));
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("count finished");
    }

    public static void uidCombine(String date, Set<String> projects){
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String project : projects){
            tasks.add(service.submit(new UIDCombineJob(date,project)));
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("count finished");
    }

    public static void projectCombine(String date, Map<String,Set<String>> projects){
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String p : projects.keySet()){
            tasks.add(service.submit(new ProjectCombineJob(date, p, projects.get(p))));
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("count finished");
    }

    public static void projectCount(String date, Set<String> projects){
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String p : projects){
            tasks.add(service.submit(new ProjectCountJob(date,p)));
        }

        for(Future f : tasks){
            try {
                f.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        service.shutdown();
        System.out.println("count finished");
    }
}
