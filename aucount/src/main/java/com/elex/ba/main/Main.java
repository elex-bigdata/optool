package com.elex.ba.main;

import com.elex.ba.job.LoadHBaseUIDJob;
import com.elex.ba.job.ProjectCombineJob;
import com.elex.ba.job.UIDCombineJob;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午6:43
 */
public class Main {

    public static void  main(String[] args) throws Exception {
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
//        loadHBaseUID(allpids);

        //combie UID
//        new UIDCombineJob(allpids).call();
//        uidCombine(allpids);

        //combie Project
//        new ProjectCombineJob(projects);
        projectCombine(projects);

        System.out.println("end analyze , spend " + (System.currentTimeMillis() - begin) );

    }

    public static void loadHBaseUID(Set<String> projects){
        ExecutorService service = new ThreadPoolExecutor(16,16,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(int i =0;i<16; i++){
            String nodename = "node" + i;

            tasks.add(service.submit(new LoadHBaseUIDJob(nodename, projects)));
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

    public static void uidCombine(Set<String> projects){
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String project : projects){
            tasks.add(service.submit(new UIDCombineJob(project)));
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

    public static void projectCombine(Map<String,Set<String>> projects){
        ExecutorService service = new ThreadPoolExecutor(16,20,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
        List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
        for(String p : projects.keySet()){
            tasks.add(service.submit(new ProjectCombineJob(p,projects.get(p))));
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
