package com.elex.bigdata.main;

import com.elex.bigdata.job.CombinerJob;
import com.elex.bigdata.job.QuartorJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: liqiang
 * Date: 14-6-7
 * Time: 上午11:31
 */
public class Main {

    public static void main(String[] args) throws Exception {

        System.out.println(args.length + " - " + args[0]);
        if(args.length != 2){
            System.err.println("Usage : filename");
            System.exit(-1);
        }

        String fpath = args[0];
        String type = args[1];
        File pfile = new File(fpath);
        FileReader fr = new FileReader(pfile);
        BufferedReader reader = new BufferedReader(fr);
        String p = null;
        List<String> pjs = new ArrayList<String>();
        while((p = reader.readLine() )!=null){
            pjs.add(p.trim());
        }
        reader.close();

        if("count".equals(type)){
            ExecutorService service = new ThreadPoolExecutor(16,16,60, TimeUnit.MILLISECONDS,new LinkedBlockingDeque<Runnable>());
            List<Future<Integer>> tasks = new ArrayList<Future<Integer>>();
            for(int i =0;i<16; i++){
                String nodename = "node" + i;

                tasks.add(service.submit(new QuartorJob(nodename, pjs)));
            }

            for(Future f : tasks){
                try {
                    f.get();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

            service.shutdown();
            System.out.println("count finished");
        }else if("combine".equals(type)){
            System.out.println("begin combine");
            new CombinerJob(pjs).call();
        }else if("sum".equals(type)){

            FileSystem fs = FileSystem.get(new Configuration());
            Map<String,Map<String,String>> result = new LinkedHashMap<String,Map<String,String>>();
            for(String pj : pjs){
                Path path = new Path("/user/hadoop/quartorcombine/" + pj + "/part-r-00000");
                if(fs.exists(path)){
                    InputStream is = fs.open(path);
                    BufferedReader br = new BufferedReader(new InputStreamReader(is));
                    Map<String,String> q = new LinkedHashMap<String, String>();
                    q.put("201301","0");
                    q.put("201302","0");
                    q.put("201303","0");
                    q.put("201304","0");
                    q.put("201305","0");
                    q.put("201306","0");
                    q.put("201307","0");
                    q.put("201308","0");
                    q.put("201309","0");
                    q.put("201310","0");
                    q.put("201311","0");
                    q.put("201312","0");
                    String value = null;
                    while((value = br.readLine() )!=null){
                        String[] qv = value.split("\t");
                        q.put(qv[0],qv[1]);
                    }
                    result.put(pj, q);
                }
            }


            for(Map.Entry<String,Map<String,String>> pv : result.entrySet()){
                String line = pv.getKey() + ",";
                for(Map.Entry<String,String> qv : pv.getValue().entrySet()){
                    line += qv.getValue() + ",";
                }
                System.out.println(line);
            }

        }

    }
}
