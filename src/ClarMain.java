
package com.shane;

import com.shane.Alarms;
import com.shane.ConvertToAvro;

public class ClarMain {


    public static void main(String[] args) {


        System.out.println("Starting Program");

        //-- Pass in the SQL & Location of the FileName
        Alarms a = new Alarms();
        //30 Seconds per 400K Rows from SQL.
        // 200K Rows in Json is 62mb
        // 200K Rows in Plain Avro is 16mb
        // 200K Rows in Avro/Snappy is 3mb

        //--a.fn_writeJson("SELECT TOP 100 * FROM alarms", "c:\\steve\\test.json");



        //--System.out.println("Program Finished Reading SQL and Writing to Json");


        ConvertToAvro1 ca = new ConvertToAvro1();

        //-- Test Wades file
        ca.fn_ConvertJsonToAvro("c:\\steve\\avro\\ss.json","c:\\steve\\avro\\sss_avro_no_snappy.avro", "");
        System.out.println("Program Finished Reading Json and Converting to Avro/Snappy");





    }
}



