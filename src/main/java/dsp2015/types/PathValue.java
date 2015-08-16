package dsp2015.types;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by barashe on 8/6/15.
 */
public class PathValue implements Writable, WritableComparable<PathValue>{

    private Text path;
    private Text word;
    private IntWritable count;
    private IntWritable totalCount;
    private IntWritable totalPathSlotCount;
    private IntWritable wordSlotCount;

    private BooleanWritable isFirst;

    public PathValue() {
        path = new Text();
        word = new Text();
        count = new IntWritable();
        totalCount = new IntWritable();
        totalPathSlotCount = new IntWritable();
        wordSlotCount = new IntWritable();

        isFirst = new BooleanWritable();

    }

    public PathValue(Text path, IntWritable count, IntWritable totalCount, IntWritable totalPathSlotCount, IntWritable wordSlotCount, BooleanWritable isFirst) {
        this.path = path;
        this.word = new Text();
        this.count = count;
        this.totalCount = totalCount;
        this.totalPathSlotCount = totalPathSlotCount;
        this.wordSlotCount = wordSlotCount;
        this.isFirst = isFirst;
    }

    public PathValue(String path, int count, int totalCount, int totalPathSlotCount, int wordSlotCount, boolean isFirst) {
        this(new Text(path), new IntWritable(count), new IntWritable(totalCount), new IntWritable(totalPathSlotCount), new IntWritable(wordSlotCount), new BooleanWritable(isFirst));
    }

    public PathValue(String path, int count, boolean isFirst) {
        this(path, count, 0, 0, 0, isFirst);

    }

    /**
     * Deep copies PathValue 'other' to a new PathValue
     * @param other - a PathValue to copy
     */
    public PathValue(PathValue other){
        this(other.getPath(), other.getCount(), other.getTotalCount(), other.getTotalPathSlotCount(), other.getWordSlotCount(), other.getIsFirst());
    }

    public void set(int count, int totalCount, int totalPathSlotCount, int wordSlotCount, boolean isFirst){
        this.count.set(count);
        this.totalCount.set(totalCount);
        this.totalPathSlotCount.set(totalPathSlotCount);
        this.wordSlotCount.set(wordSlotCount);
        this.isFirst.set(isFirst);
    }

    public void set(String path, int count){
        this.path.set(path);
        this.count.set(count);
    }

    public void setWord(Text word){
        this.word.set(word);
    }

    public Text getWord(){
        return new Text(word);
    }

    public void setTotalCount(int totalCount) {
        this.totalCount.set(totalCount);
    }

    public void setTotalPathSlotCount(int totalPathSlotCount) {
        this.totalPathSlotCount.set(totalPathSlotCount);
    }

    public void setCount(int count) {
        this.count.set(count);
    }

    public void setWordSlotCount(int wordSlotCount) {
        this.wordSlotCount.set(wordSlotCount);
    }

    public void setFirst(boolean isFirst){
        this.isFirst.set(isFirst);
    }

    public IntWritable getCount() {
        return new IntWritable(count.get());
    }

    public IntWritable getTotalCount() {
        return new IntWritable(totalCount.get());
    }

    public IntWritable getTotalPathSlotCount() {
        return new IntWritable(totalPathSlotCount.get());
    }


    public IntWritable getWordSlotCount() {
        return new IntWritable(wordSlotCount.get());
    }

    public BooleanWritable getIsFirst() {
        return new BooleanWritable(isFirst.get());
    }

    public int compareTo(PathValue pathValue) {
        return 0;
    }


    public void write(DataOutput dataOutput) throws IOException {
        path.write(dataOutput);
        word.write(dataOutput);
        count.write(dataOutput);
        totalCount.write(dataOutput);
        totalPathSlotCount.write(dataOutput);
        wordSlotCount.write(dataOutput);

        isFirst.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        path.readFields(dataInput);
        word.readFields(dataInput);
        count.readFields(dataInput);
        totalCount.readFields(dataInput);
        totalPathSlotCount.readFields(dataInput);
        wordSlotCount.readFields(dataInput);
        isFirst.readFields(dataInput);
    }

    public Text getPath() {
        return new Text(path);
    }
}
