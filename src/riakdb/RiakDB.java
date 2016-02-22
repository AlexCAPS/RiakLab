/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package riakdb;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.api.commands.buckets.StoreBucketProperties;
import com.basho.riak.client.api.commands.search.StoreIndex;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.search.YokozunaIndex;
import com.basho.riak.client.core.util.BinaryValue;
import com.basho.riak.client.api.commands.search.Search;
import com.basho.riak.client.api.commands.search.StoreSchema;
import com.basho.riak.client.core.query.search.YokozunaSchema;
import java.io.File;

import java.net.UnknownHostException;
import java.util.concurrent.ExecutionException;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

/**
 *
 * @author alex
 */
public class RiakDB {

    /**
     * @param args the command line arguments
     */
    static RiakClient client;
    public static void main(String[] args) throws UnknownHostException, ExecutionException, InterruptedException, IOException, ParseException {

        client = RiakClient.newClient(8087, "127.0.0.1");

        
        YokozunaIndex subIndex = new YokozunaIndex("subInd");
        YokozunaIndex objIndex = new YokozunaIndex("objInd");
        
        StoreIndex subStoreIndex = new StoreIndex.Builder(subIndex).build();
        StoreIndex objStoreIndex = new StoreIndex.Builder(objIndex).build();
        
        client.execute(subStoreIndex);
        client.execute(objStoreIndex);
        
        Namespace subNamespace = new Namespace("sub");
        Namespace objNamespace = new Namespace("obj");
        
        StoreBucketProperties subProp = new StoreBucketProperties.Builder(subNamespace).withSearchIndex("subInd").build();
        
        client.execute(subProp);

        StoreBucketProperties objProp = new StoreBucketProperties.Builder(objNamespace).withSearchIndex("objInd").build();
        client.execute(objProp);
        
        //http://docs.basho.com/riak/latest/dev/using/search/
        
        int id = 0;
        String filePath = args[0];                      //"subjects";
        FileReader reader = new FileReader(filePath);
        JSONParser jsonParser = new JSONParser();
        Object jsonObject = jsonParser.parse(reader);
        JSONArray sub = (JSONArray) jsonObject;
        Iterator i = sub.iterator();

        while (i.hasNext()) {

            RiakObject object = new RiakObject().
                    setContentType("application/json").
                    setValue(BinaryValue.create(i.next().toString()));
            Location location = new Location(subNamespace, Integer.toString(id));

            StoreValue sv = new StoreValue.Builder(object).withLocation(location).build();
            client.execute(sv);
            id = id + 1;
        }

        String filePath2 = args[1]; //"obj";
        FileReader reader2 = new FileReader(filePath2);
        JSONParser jsonParser2 = new JSONParser();
        Object jsonObject2 = jsonParser2.parse(reader2);
        JSONArray obj = (JSONArray) jsonObject2;
        Iterator j = obj.iterator();

        while (j.hasNext()) {

            RiakObject object = new RiakObject().
                    setContentType("application/json").
                    setValue(BinaryValue.create(j.next().toString())); // значение
                                                            //id - псевдо-уникальный ключ
            Location location = new Location(objNamespace, Integer.toString(id));

            StoreValue sv = new StoreValue.Builder(object).withLocation(location).build();
            client.execute(sv);
            id = id + 1;
        }
        
        String username = "alex";
        String path = "/home/alex/file3";
        String right = "rw";
        boolean status = CanDoIn(username, path, right);
        System.out.println("CanDoIt say " + status);

        client.shutdown();
    }

    private static boolean CanDoIn(String username, String path, String right) {
        int usrlvl = GetUserLevel(username);
        String querystring = GetQueryString(path);
        List<Integer> alllvlList = GetAllLvlParents(querystring);
        
        boolean status = false;
        switch(right) {
            case "r" : status = NoReadUp(usrlvl, alllvlList); break;
            case "w" : status = NoWriteDown(usrlvl, alllvlList); break;
            case "rw": status = CanRW(usrlvl, alllvlList); break;
        }
        
        return status;
    }

    private static int GetUserLevel(String username){
        try {
            List<Map<String, List<String>>> res;
            Search s = new Search.Builder("subInd", "name_s:" + username).build();
            res = client.execute(s).getAllResults();
            String strlvl = res.get(0).get("lvl_i").get(0);
            int lvl = Integer.parseInt(strlvl);
            System.out.println(username + "lvl = " + lvl);
            return lvl;
        } catch (ExecutionException ex) {
            Logger.getLogger(RiakDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RiakDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -127;
    }

    private static String GetQueryString(String path) {
        String[] parents = path.split("/");
        
//        String tmpname = "\\/";
        String tmpname = "";
        List<String> fullparents = new ArrayList<>();
//        fullparents.add(tmpname);
        for (String parent : parents) {
            if(!parent.equals("")) {
            tmpname += ("\\/" + parent);
            fullparents.add(tmpname);
            System.out.println("parent = " + tmpname);
            }
        }
        
        String query = "";
        for(int i=0; i< fullparents.size() - 1; i++) {
            query += ("path_s: " + fullparents.get(i) + " OR ");
        }
        query += "path_s: " + fullparents.get(fullparents.size()-1);
        System.out.println(query);
        return query;
    }

    private static List<Integer> GetAllLvlParents(String querystring) {
        try {
            Search s = new Search.Builder("objInd", querystring).build();
            List<Map<String, List<String>>> res = client.execute(s).getAllResults();
            List<Integer> lvls = new ArrayList<>();
            for (Iterator<Map<String, List<String>>> iterator = res.iterator(); iterator.hasNext();) {
                Map<String, List<String>> next = iterator.next();
                lvls.add(Integer.parseInt(next.get("lvl_i").get(0)));
            }
            return lvls;
        } catch (ExecutionException ex) {
            Logger.getLogger(RiakDB.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(RiakDB.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private static boolean NoReadUp(int usrlvl, List<Integer> alllvlList) {
        for (Integer integer : alllvlList) {
            if(usrlvl < integer) {
                return false;
            }
        }
        return true;
    }
    private static boolean NoWriteDown(int usrlvl, List<Integer> alllvlList) {
        for (Integer integer : alllvlList) {
            if(usrlvl > integer) {
                return false;
            }
        }
        return true;
    }

    private static boolean CanRW(int usrlvl, List<Integer> alllvlList) {
        for (Integer integer : alllvlList) {
            if(usrlvl != integer) {
                return false;
            }
        }
        return true;
    }
}
