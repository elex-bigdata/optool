package com.elex.bigdata.service;

import com.elex.bigdata.util.SSHUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: liqiang
 * Date: 14-4-29
 * Time: 下午5:09
 */
public class OpService {

    public Map<String,String> getDrillStatus() throws Exception{
        SSHUtil ssh = SSHUtil.getXAConnection();
        try {
            Map<String,String> status = new HashMap<String,String>();
            //node10:Drillbit is running
            ssh.execCommand("perl /home/hadoop/Drill/script/service.pl status");
            while (true) {
                String line = ssh.readLine();

                if (line == null ) {
                    break;
                }
                if(line.startsWith("node")){
                    String node = line.split(":")[0];
                    if(line.endsWith("running.")){
                        status.put(node,"0");
                    }else{
                        status.put(node,"-1");
                    }
                }
            }
            return status;
        } catch (IOException e) {
            throw e;
        } finally{
            ssh.close();
        }
    }

    public boolean opDrill(){

        return false;
    }
}
