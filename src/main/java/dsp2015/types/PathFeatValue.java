package dsp2015.types;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by barashe on 8/12/15.
 */
public class PathFeatValue implements Writable, WritableComparable<PathFeatValue> {

    private Text path;
    private Text word;
    private IntWritable count;
    private DoubleWritable mi;
    private DoubleWritable tfidf;
    private DoubleWritable dice;
    private BooleanWritable isFirst;
    private BooleanWritable slot;

    public PathFeatValue(){
        path = new Text();
        word = new Text();
        count = new IntWritable();
        mi = new DoubleWritable();
        tfidf = new DoubleWritable();
        dice = new DoubleWritable();
        isFirst = new BooleanWritable();
        slot = new BooleanWritable();
    }

    public void set(PathValue pv){
        path.set(pv.getPath());
        count.set(pv.getCount().get());
    }

    public PathFeatValue(PathFeatValue pv){
        path = pv.getPath();
        word = pv.getWord();
        count = pv.getCount();
        mi = pv.getMi();
        tfidf = pv.getTfidf();
        dice = pv.getDice();
        isFirst = pv.getIsFirst();
        slot = pv.getSlot();
    }

    public void setStat(double mi, double tfidf, double dice){
        this.mi.set(mi);
        this.tfidf.set(tfidf);
        this.dice.set(dice);
    }

    public void setWord(Text word) {
        this.word.set(word);
    }

    public void setSlot(BooleanWritable slot) {

        this.slot.set(slot.get());
    }

    public Text getWord() {
        return new Text(word);
    }

    public BooleanWritable getSlot() {
        return new BooleanWritable(slot.get());
    }

    public void setFirst(boolean isFirst){
        this.isFirst.set(isFirst);
    }

    public Text getPath() {
        return new Text(path);
    }

    public IntWritable getCount() {
        return new IntWritable(count.get());
    }

    public DoubleWritable getMi() {
        return new DoubleWritable(mi.get());
    }

    public DoubleWritable getTfidf() {
        return new DoubleWritable(tfidf.get());
    }

    public DoubleWritable getDice() {
        return new DoubleWritable(dice.get());
    }

    public BooleanWritable getIsFirst() {
        return new BooleanWritable(isFirst.get());
    }

    public int compareTo(PathFeatValue pathFeatValue) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        path.write(dataOutput);
        word.write(dataOutput);
        count.write(dataOutput);
        mi.write(dataOutput);
        tfidf.write(dataOutput);
        dice.write(dataOutput);
        isFirst.write(dataOutput);
        slot.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        path.readFields(dataInput);
        word.readFields(dataInput);
        count.readFields(dataInput);
        mi.readFields(dataInput);
        tfidf.readFields(dataInput);
        dice.readFields(dataInput);
        isFirst.readFields(dataInput);
        slot.readFields(dataInput);

    }

    @Override
    public String toString() {
        //return super.toString();
        return "path:" + path.toString() + ", word:"+ word.toString() +", count:"+count + ", mi:"+ mi +", tfidf:" + tfidf +", dice:"+dice+ ", isFirst:"+ isFirst;
    }
}
