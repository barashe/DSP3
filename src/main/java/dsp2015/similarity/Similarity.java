package dsp2015.similarity;
import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import dsp2015.total_count.StatComp;
import dsp2015.types.PathFeatValue;
import dsp2015.types.PathKey;
import dsp2015.types.PathValue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.ListPathsServlet;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
* Created by ran on 13/08/15.
*/
public class Similarity {

    public static class MapClass extends Mapper<PathKey, PathFeatValue, PathKey, PathFeatValue> {

        private Hashtable<String, List<String>> joinData = new Hashtable<String, List<String>>();

        @Override
        public void setup(Context context) {
            try {
                Path[] localPaths = new Path[2];
                localPaths[0]= new Path(context.getConfiguration().get("positiveTestSet"));
                localPaths[1]= new Path(context.getConfiguration().get("negativeTestSet"));
                if (localPaths != null && localPaths.length == 2 ) {
                    String line;
                    String[] tokens;
                    for (int i=0 ; i<2 ; i++) {
                        BufferedReader joinReader = new BufferedReader(new FileReader(localPaths[i].toString()));
                        try {
                            while ((line = joinReader.readLine()) != null) {
                                tokens = line.split("\t", 2);
                                putData(tokens[0],tokens[1]);
                                putData(tokens[1],tokens[0]);
                            }
                        } finally {
                            joinReader.close();
                        }
                    }
                }
            } catch(IOException e) {
                System.err.println("Exception reading DistributedCache: " + e);
            }
        }

        private void putData(String key, String value) {
            if (joinData.containsKey(key)){
                joinData.get(key).add(value);
            }
                else{
                    List<String> tmpList = new ArrayList<String>();
                    tmpList.add(value);
                    joinData.put(key, tmpList);
                }

        }

        @Override
        public void map(PathKey key, PathFeatValue value, Context context) throws IOException, InterruptedException {
            List<String> joinValues = joinData.get(key.getPath().toString().substring(1));
            if (joinValues != null) {
                for (String joinValue : joinValues) {
                    key.setSimKey(key.getPath().toString() + "\t" + joinValue);
                    value.setWord(key.getWord());
                    context.write(key,value);
                }
                //context.write(key,new Text(value.toString() + "," + joinValue));
            }
        }
    }

    public static class ReduceClass extends Reducer<PathKey,PathFeatValue,Text,Text> {

        private Text currentSimKey;
        private boolean isP1 = false;
        private SimComp simCompX , simCompY;
        private Map<String , PathFeatValue> XfeatureTable;
        private Map<String , PathFeatValue> YfeatureTable;

        @Override
        protected void reduce(PathKey key, Iterable<PathFeatValue> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (PathFeatValue value : values) {

                //init
                Text tmpSimKey = key.getSimKey();
                if (currentSimKey ==null || !currentSimKey.toString().equals(tmpSimKey.toString())){
                    currentSimKey = tmpSimKey;
                    if (currentSimKey!=null){
                        Text newValue = new Text("simSlotX:\tsim: "+simCompX.compSim()+"\tcosine: "+simCompX.compCosine()+"\tdice-cover: "+simCompX.compCover()+"simSlotY:\tsim: "+simCompY.compSim()+"\tcosine: "+simCompY.compCosine()+"\tdice-cover: "+simCompY.compCover());
                        context.write(key.getSimKey() , newValue);
                    }
                    init();
                }

                //add to feature table (after checking appropriate slot)
                Map<String,PathFeatValue> tableToUpdate;
                SimComp simToUpdate;
                if (key.getSlot().get()){
                    tableToUpdate = XfeatureTable;
                    simToUpdate = simCompX;
                }
                else{
                    tableToUpdate = YfeatureTable;
                    simToUpdate = simCompY;
                }
                if (key.getSimKey().toString().split("\t")[0].equals(key.getPath().toString()))
                    isP1=true;
                else
                    isP1 = false;
                update(value,tableToUpdate,simToUpdate,isP1);
            }
        }

        private void update(PathFeatValue value, Map<String, PathFeatValue> tableToUpdate, SimComp simToUpdate,boolean isP1) {
            Text word = value.getWord();
            PathFeatValue valueToUpdate = tableToUpdate.get(word);
            if (valueToUpdate!=null){
                if (isP1) {
                    simToUpdate.updateIntersect(value, valueToUpdate);
                }
                else{
                    simToUpdate.updateIntersect(valueToUpdate , value);
                }
            }
            else{
                tableToUpdate.put(word.toString(),value);
            }
            simToUpdate.update(value,isP1);
        }


        private void init() {
            simCompX = new SimComp();
            XfeatureTable = new HashMap<String, PathFeatValue>();
            simCompY = new SimComp();
            YfeatureTable = new HashMap<String, PathFeatValue>();
        }
    }


/*    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "DataJoin with DistributedCache");
        conf.set("key.value.separator.in.input.line", ",");
        DistributedCache.addCacheFile(new URI(args[0]), conf);
        job.setJarByClass(MapperSideJoinWithDistributedCache.class);
        job.setMapperClass(MapClass.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setInputFormatClass(KeyTaggedValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }*/
}

