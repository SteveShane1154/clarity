package com.shane;

import java.io.File;
import java.io.IOException;

import org.apache.avro.file.Codec;
import org.apache.avro.file.CodecFactory;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.FileNotFoundException;
import java.io.FileReader;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.xerial.snappy.SnappyCodec;

import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;




/**
 * <a>
 * Created by shanesrg on 2/1/2017.
 *
 *  Need to generate the Avro Schema File.  I generated it alarms.avsc
 *{
 "namespace":"com.shane.models",
 "type":"record",
 "name":"AlarmModel",
 "fields":[
 {"name":"bed", "type":"string"},
 {"name":"txt", "type":"string"},
 {"name":"unit", "type":"string"},
 {"name":"channel", "type":"string"},
 {"name":"msh_ts", "type":"string"},
 {"name":"source", "type":"string"},
 {"name":"alarm_ts", "type":"string"}
 ]
 }
 *
 * Then you have to generate a Class
 * java -jar <path/to/avro-tools-1.7.7.jar> compile schema <path/to/schema-file> <destination-folder>
 *
 * Next Compile the Schema (Done in IntelliJ
 *  java -jar /home/Hadoop/Avro_work/jars/avro-tools-1.7.7.jar compile schema /home/Hadoop/Avro_work/schema/emp.avsc /home/Hadoop/Avro/with_code_gen
 *
 *  </a>
 *
 */
public class ConvertToAvro1 {

    //private static final String FILENAME = "c:\\\\steve\\\\test.json";

    /**
     * Converts a Json File to Avro
     *
     * <p>
     * This method always returns immediately, whether or not the pgm yields.
     *
     * @param  sourcefilename  Path and Name to Json File that will be converted
     * @param  destfilename    Path and Name of Avro file to be converted.
     * @param  xcodec          Codec "snappy" or blank for default
     * @return      Nada
     *
     */
    public void fn_ConvertJsonToAvro(String sourcefilename, String destfilename, String xcodec){
        //-- Read .Json File
        //-- Load into Avro and Write new Avro File
        JSONParser parser = new JSONParser();
        BufferedReader br = null;
        FileReader fr = null;

        try {

            com.shane.ObxAvroModel ss = new com.shane.ObxAvroModel();

            DatumWriter<ObxAvroModel> AlarmsDatumWriter = new SpecificDatumWriter<ObxAvroModel>(ObxAvroModel.class);
            DataFileWriter<ObxAvroModel> AlarmsFileWriter = new DataFileWriter<ObxAvroModel>(AlarmsDatumWriter);
            //-- Snappy Compression
              if (xcodec == "snappy")
            {
                AlarmsFileWriter.setCodec(CodecFactory.snappyCodec());
            }
            else
            {
                AlarmsFileWriter.setCodec(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL));
             }

            AlarmsFileWriter.create(ss.getSchema(), new File(destfilename));

            String sCurrentLine;


            br = new BufferedReader(new FileReader(sourcefilename));

            while ((sCurrentLine = br.readLine()) != null) {
                Object obj = parser.parse(sCurrentLine);
                JSONObject jsonObject = (JSONObject) obj;

                ss.setBed((String) jsonObject.get("bed"));
                ss.setText((String) jsonObject.get("text"));
                ss.setUnit((String) jsonObject.get("unit"));
                ss.setChannel((String) jsonObject.get("channel"));
                ss.setMshTs((Long) jsonObject.get("msh_ts"));
                ss.setSource((String) jsonObject.get("source"));
                ss.setAlarmTs((Long) jsonObject.get("alarm_ts"));

                AlarmsFileWriter.append(ss);


            }

            //--Cleanup
            AlarmsFileWriter.close();

            System.out.println("data successfully serialized");


        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }



    }

    public void fnCreateAvroFile() throws IOException
    {
        com.shane.ObxAvroModel ss = new com.shane.ObxAvroModel();
        DatumWriter<ObxAvroModel> AlarmsDatumWriter = new SpecificDatumWriter<ObxAvroModel>(ObxAvroModel.class);
        DataFileWriter<ObxAvroModel> AlarmsFileWriter = new DataFileWriter<ObxAvroModel>(AlarmsDatumWriter);
        AlarmsFileWriter.create(ss.getSchema(), new File("c://steve/JavaAlarms.avro"));
    }

    public void fnAppendAvroFile(com.shane.ObxAvroModel ss1) throws IOException {
//        com.shane.ObxAvroModel ss1 = new com.shane.ObxAvroModel();
        DatumWriter<ObxAvroModel> AlarmsDatumWriter = new SpecificDatumWriter<ObxAvroModel>(ObxAvroModel.class);
        DataFileWriter<ObxAvroModel> AlarmsFileWriter = new DataFileWriter<ObxAvroModel>(AlarmsDatumWriter);
        AlarmsFileWriter.append(ss1);

    }



    public static CodecFactory getCodec(String name) {
        if (name == null || name.equalsIgnoreCase("null")) {
            return CodecFactory.nullCodec();
        }
        CodecFactory codecFactory;
        switch (name) {
            case "snappy":
                codecFactory = CodecFactory.snappyCodec();
                break;
            case "deflate":
                codecFactory = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
                break;
            case "bzip2":
                codecFactory = CodecFactory.bzip2Codec();
                break;
            case "xz":
                codecFactory = CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL);
                break;
            default:
                codecFactory = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL);
                break;
        }
        return codecFactory;
    }



}

