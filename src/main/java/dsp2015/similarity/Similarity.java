package dsp2015.similarity;
import java.io.*;

import java.net.URI;
import java.util.*;

import dsp2015.total_count.StatComp;
import dsp2015.types.PathFeatValue;
import dsp2015.types.PathKey;
import dsp2015.types.PathValue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

    private static final Log LOG = LogFactory.getLog(Similarity.class);

    public static class MapClass extends Mapper<PathKey, PathFeatValue, PathKey, PathFeatValue> {

        private Map<String, List<String>> joinData = new HashMap<String, List<String>>();
        private Map<String, List<String>> joinDataReverse = new HashMap<String, List<String>>();


        @Override
        public void setup(Context context) {
            try {
                Path[] localPaths = new Path[2];
                //localPaths[0]= new Path(context.getConfiguration().get("positiveTestSet"));
                //localPaths[1]= new Path(context.getConfiguration().get("negativeTestSet"));
                localPaths[0]= new Path("positive-preds.txt");
                localPaths[1]= new Path("negative-preds.txt");

                LOG.warn("sanity1");
                if (localPaths != null && localPaths.length == 2 ) {
                    String line;
                    String[] tokens;
                    LOG.warn("sanity2");
                    for (int i=0 ; i<2 ; i++) {
                        InputStream is = getClass().getClassLoader().getResourceAsStream(localPaths[i].toString());
                        BufferedReader joinReader=new BufferedReader(new InputStreamReader(is));

//                        FileReader fr = new FileReader(localPaths[i].toString());
//                        BufferedReader joinReader = new BufferedReader(fr);
                        try {
                            LOG.warn("before reading test sets , path"+i+" = "+ localPaths[i].toString());
                            while ((line = joinReader.readLine()) != null) {
                                LOG.warn("reading test set line = " + line);
                                tokens = line.split("\t", 2);
                                putData(tokens[0],tokens[1] , joinData);
                                putData(tokens[1],tokens[0] , joinDataReverse);
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

        private void putData(String key, String value , Map<String, List<String>> target) {
            if (target.containsKey(key)){
                target.get(key).add(value);
            }
                else{
                    List<String> tmpList = new ArrayList<String>();
                    tmpList.add(value);
                    target.put(key, tmpList);
                }

        }

        @Override
        public void map(PathKey key, PathFeatValue value, Context context) throws IOException, InterruptedException {
            List<String> joinValues = joinData.get(key.getPath().toString());
            List<String> joinValuesReversed = joinDataReverse.get(key.getPath().toString());

            write(key , value , joinValues , false , context);
            write(key, value, joinValuesReversed, true, context);

        }

        private void write(PathKey key, PathFeatValue value, List<String> joinValuesCandidate, boolean isReversed, Context context) {
            if (joinValuesCandidate != null) {
                for (String joinValue : joinValuesCandidate) {
                    try {
                        Text currentPath = key.getPath();
                        if(!isReversed)
                            key.setSimKey(currentPath + "\t" + joinValue);
                        else
                            key.setSimKey(joinValue + "\t" + currentPath);
                        value.setWord(key.getWord());
                        value.setSlot(key.getSlot());
                        context.write(key, value);
                    } catch (IOException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                //context.write(key,new Text(value.toString() + "," + joinValue));
            }
        }
    }

    public static class ReduceClass extends Reducer<PathKey,PathFeatValue,Text,Text> {

        //private Text currentSimKey;
        private boolean isP1 = false;
        private SimComp simCompX , simCompY;
        private Map<String , PathFeatValue> XfeatureTable;
        private Map<String , PathFeatValue> YfeatureTable;

        @Override
        protected void reduce(PathKey key, Iterable<PathFeatValue> values, Context context) throws IOException, InterruptedException {
            init();
            for (PathFeatValue value : values) {

                //currentSimKey = key.getSimKey();
                 //add to feature table (after checking appropriate slot)
                Map<String,PathFeatValue> tableToUpdate;
                SimComp simToUpdate;
                if (value.getSlot().get()){
                    tableToUpdate = XfeatureTable;
                    simToUpdate = simCompX;
                }
                else{
                    tableToUpdate = YfeatureTable;
                    simToUpdate = simCompY;
                }
                //check if current value is p1 or p2 in the simKey
                if (key.getSimKey().toString().split("\t")[0].equals(key.getPath().toString()))
                    isP1=true;
                else
                    isP1 = false;
                update(value, tableToUpdate,simToUpdate,isP1);
            }

                Text newValue = new Text("simSlotX:\tsim: "+simCompX.compSim()+"\tcosine: "+simCompX.compCosine()+"\tdice-cover: "+simCompX.compCover()+"\tsimSlotY:\tsim: "+simCompY.compSim()+"\tcosine: "+simCompY.compCosine()+"\tdice-cover: "+simCompY.compCover());
                context.write(key.getSimKey() , newValue);
        }


        private void update(PathFeatValue value, Map<String, PathFeatValue> tableToUpdate, SimComp simToUpdate,boolean isP1) {
            Text word = value.getWord();
            PathFeatValue valueToUpdate = tableToUpdate.get(word.toString());
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

