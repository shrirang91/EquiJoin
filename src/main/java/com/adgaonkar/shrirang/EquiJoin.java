package com.adgaonkar.shrirang;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by shrirang on 11/19/16.
 * Map Reduce program for Equal Join
 */

public class EquiJoin {

    /**
     * This class handles the Mapper function of the hadoop framework.
     * For the equal-join operation assuming non-unique tuple,
     * this function reads every line and then transform that to a Key Value pair.
     */
    private static class MapperForEquiJoin extends Mapper<Object, Text, DoubleWritable, Text>  {

        private DoubleWritable joinAttribute = new DoubleWritable();

        /**
         * This function maps the rows by using their join column attribute and stores that
         * in an Attributes String array. The merges everything else verbose.
         * @param joinColumn the join attribute.
         * @param tuple the entire row value.
         * @param context the application context for Mapper.
         * @throws IOException
         */
        public void map(Object joinColumn, Text tuple, Context context) throws IOException {

            try {
                if (tuple != null) {
                    String row = tuple.toString();
                    String[] attributes = row.split(",");

                    joinAttribute.set(Double.parseDouble(attributes[1].trim()));
                    context.write(joinAttribute, tuple);
                }
            } catch (Exception ex) {
                System.out.println("Error in mapper method : " + ex.getMessage());
            }

        }
    }

    /**
     * This class handles the Reducer function of the hadoop framework.
     * For the equal-join operation assuming non-unique tuple,
     * this function takes all the mapped key value pairs and then reduces them based on the key value.
     */
    private static class ReducerForEquiJoin extends Reducer<DoubleWritable, Text, Text, Text> {

        /**
         * This functions takes in the key value pair and then merges them based
         * on the join column which is the attribute variable.
         * @param joinColumn the join attribute.
         * @param tuples the entire row value.
         * @param context the application context for Reducer.
         * @throws IOException
         */
        public void reduce(DoubleWritable joinColumn, Iterable<Text> tuples, Context context) throws IOException {

            try{
                Iterator<Text> tupleIterator = tuples.iterator();
                Map<String, List<Text>> mapperValues = new HashMap<String , List<Text>>();
                List<String> arrayList = new ArrayList<String>();

                while (tupleIterator.hasNext()) {
                    Text row = tupleIterator.next();
                    String[] attributes = row.toString().split(",");
                    String attribute = attributes[0].trim();

                    if (mapperValues.containsKey(attribute)) {
                        List<Text> tuple = mapperValues.get(attribute);
                        tuple.add(new Text(row));
                        mapperValues.put(attribute, tuple);
                    } else {
                        List<Text> tuple = new ArrayList<Text>();
                        tuple.add(new Text(row));
                        arrayList.add(attribute);
                        mapperValues.put(attribute, tuple);
                    }
                }

                int len = arrayList.size();
                String[] tupleRow = new String[len];
                tupleRow = arrayList.toArray(tupleRow);
                Arrays.sort(tupleRow);

                if (mapperValues.size() == 2) {

                    List<Text> firstTable = mapperValues.get(tupleRow[0]);
                    List<Text> secondTable = mapperValues.get(tupleRow[1]);
                    int firstTableSize = firstTable.size() - 1;
                    int secondTableSize = secondTable.size() - 1;

                    for (int i = firstTableSize; i > -1 ; i--) {
                        Text firstTuple = firstTable.get(i);
                        for (int j = secondTableSize; j > -1 ; j--) {
                            Text secondTuple = secondTable.get(j);
                            String joinResult = firstTuple.toString() + ", " + secondTuple.toString();
                            context.write(new Text(joinResult), new Text(""));
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error in reduce method : " + e.getMessage());
            }
        }
    }

    public static void main(String args[]) throws Exception {
        Configuration configuration = new Configuration();
        Job jobObject = Job.getInstance(configuration, "EquiJoin");
        jobObject.setJarByClass(EquiJoin.class);


        jobObject.setMapperClass(MapperForEquiJoin.class);
        jobObject.setMapOutputKeyClass(DoubleWritable.class);
        jobObject.setMapOutputValueClass(Text.class);


        jobObject.setReducerClass(ReducerForEquiJoin.class);
        jobObject.setOutputKeyClass(Text.class);
        jobObject.setOutputValueClass(Text.class);

        try {
            FileSystem fileSystem = FileSystem.get(configuration);
            if (fileSystem.exists(new Path(args[1]))) {
                fileSystem.delete(new Path(args[1]), true);
            }
        }catch(Exception e){
            System.out.println("Error in main method : " + e.getMessage());
        }

        FileInputFormat.addInputPath(jobObject, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobObject, new Path(args[1]));
        if(jobObject.waitForCompletion(true)) {
            System.exit(0);
        } else {
            System.exit(1);
        }
    }

}
