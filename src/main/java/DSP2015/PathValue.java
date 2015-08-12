package DSP2015;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by barashe on 8/6/15.
 */
public class PathValue implements Writable, WritableComparable<PathValue>{

    private Text path;
    private IntWritable count;
    private IntWritable totalCount;
    private IntWritable totalPathSlotXCount;
    private IntWritable totalPathSlotYCount;
    private IntWritable wordSlotCount;

    private BooleanWritable isFirst;

    public PathValue() {
        path = new Text();
        count = new IntWritable();
        totalCount = new IntWritable();
        totalPathSlotXCount = new IntWritable();
        totalPathSlotYCount = new IntWritable();
        wordSlotCount = new IntWritable();

        isFirst = new BooleanWritable();

    }

    public PathValue(Text path, IntWritable count, IntWritable totalCount, IntWritable totalPathSlotXCount, IntWritable totalPathSlotYCount, IntWritable wordSlotCount, BooleanWritable isFirst) {
        this.path = path;
        this.count = count;
        this.totalCount = totalCount;
        this.totalPathSlotXCount = totalPathSlotXCount;
        this.totalPathSlotYCount = totalPathSlotYCount;
        this.wordSlotCount = wordSlotCount;
        this.isFirst = isFirst;
    }

    public PathValue(String path, int count, int totalCount, int totalPathSlotXCount, int totalPathSlotYCount, int wordSlotCount, boolean isFirst) {
        this(new Text(path), new IntWritable(count), new IntWritable(totalCount), new IntWritable(totalPathSlotXCount), new IntWritable(totalPathSlotYCount), new IntWritable(wordSlotCount), new BooleanWritable(isFirst));
    }

    public PathValue(String path, int count, boolean isFirst) {
        this(path, count, 0, 0, 0, 0, isFirst);

    }

    /**
     * Deep copies PathValue 'other' to a new PathValue
     * @param other - a PathValue to copy
     */
    public PathValue(PathValue other){
        this(other.getPath(), other.getCount(), other.getTotalCount(), other.getTotalPathSlotXCount(), other.getTotalPathSlotYCount(), other.getWordSlotCount(), other.getIsFirst());
    }

    public void set(int count, int totalCount, int totalPathSlotXCount, int totalPathSlotYCount, int wordSlotCount, boolean isFirst){
        this.count.set(count);
        this.totalCount.set(totalCount);
        this.totalPathSlotXCount.set(totalPathSlotXCount);
        this.totalPathSlotYCount.set(totalPathSlotYCount);
        this.wordSlotCount.set(wordSlotCount);
        this.isFirst.set(isFirst);
    }

    public void set(String path, int count){
        this.path.set(path);
        this.count.set(count);
    }

    public void setTotalCount(int totalCount) {
        this.totalCount.set(totalCount);
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

    public IntWritable getTotalPathSlotXCount() {
        return new IntWritable(totalPathSlotXCount.get());
    }

    public IntWritable getTotalPathSlotYCount() {
        return new IntWritable(totalPathSlotYCount.get());
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
        count.write(dataOutput);
        totalCount.write(dataOutput);
        totalPathSlotXCount.write(dataOutput);
        totalPathSlotYCount.write(dataOutput);
        wordSlotCount.write(dataOutput);

        isFirst.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        path.readFields(dataInput);
        count.readFields(dataInput);
        totalCount.readFields(dataInput);
        totalPathSlotXCount.readFields(dataInput);
        totalPathSlotYCount.readFields(dataInput);
        wordSlotCount.readFields(dataInput);
        isFirst.readFields(dataInput);
    }

    public Text getPath() {
        return new Text(path);
    }
}
