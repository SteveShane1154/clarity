package com.shane;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import java.io.FileWriter;
import java.io.IOException;

public class JsonSimpleWriteExample {

    /*
    public static void main(String[] args) {

        //json-simple-1.1.1.jar


        JSONObject obj = new JSONObject();
        obj.put("name", "Steve");
        obj.put("age", new Integer(51));

        JSONArray list = new JSONArray();
        list.add("Kathy");
        list.add("Bryan");
        list.add("Becky");

        obj.put("messages", list);

        try (FileWriter file = new FileWriter("c:\\steve\\test.json")) {

            file.write(obj.toJSONString());
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print(obj);

    }
*/

    public void fn_steve()
    {
        org.json.simple.JSONObject obj = new JSONObject();
        obj.put("name", "Steve");
        obj.put("age", new Integer(51));

        JSONArray list = new JSONArray();
        list.add("Kathy");
        list.add("Bryan");
        list.add("Becky");

        obj.put("messages", list);

        try (FileWriter file = new FileWriter("c:\\steve\\test.json", true)) {

            file.write(obj.toJSONString());
            file.flush();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.print(obj);
    }

}
