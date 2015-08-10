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
    private IntWritable wordSlotXCount;
    private IntWritable wordSlotYcount;
    private BooleanWritable isFirst;

    public PathValue() {
        path = new Text();
        count = new IntWritable();
        totalCount = new IntWritable();
        totalPathSlotXCount = new IntWritable();
        totalPathSlotYCount = new IntWritable();
        wordSlotXCount = new IntWritable();
        wordSlotYcount = new IntWritable();
        isFirst = new BooleanWritable();

    }

    public PathValue(Text path, IntWritable count, IntWritable totalCount, IntWritable totalPathSlotXCount, IntWritable totalPathSlotYCount, IntWritable wordSlotXCount, IntWritable wordSlotYcount, BooleanWritable isFirst) {
        this.path = path;
        this.count = count;
        this.totalCount = totalCount;
        this.totalPathSlotXCount = totalPathSlotXCount;
        this.totalPathSlotYCount = totalPathSlotYCount;
        this.wordSlotXCount = wordSlotXCount;
        this.wordSlotYcount = wordSlotYcount;
        this.isFirst = isFirst;
    }

    public PathValue(String path, int count, int totalCount, int totalPathSlotXCount, int totalPathSlotYCount, int wordSlotXCount, int wordSlotYCount, boolean isFirst) {
        this(new Text(path), new IntWritable(count), new IntWritable(totalCount), new IntWritable(totalPathSlotXCount), new IntWritable(totalPathSlotYCount), new IntWritable(wordSlotXCount), new IntWritable(wordSlotYCount), new BooleanWritable(isFirst));
    }

    public PathValue(String path, int count, boolean isFirst) {
        this(path, count, 0, 0, 0, 0, 0, isFirst);

    }

    /**
     * Deep copies PathValue 'other' to a new PathValue
     * @param other - a PathValue to copy
     */
    public PathValue(PathValue other){
        this(other.getPath(), other.getCount(), other.getTotalCount(), other.getTotalPathSlotXCount(), other.getTotalPathSlotYCount(), other.getWordSlotXCount(), other.getWordSlotYcount(), other.getIsFirst());
    }

    public void set(int count, int totalCount, int totalPathSlotXCount, int totalPathSlotYCount, int wordSlotXCount, int wordSlotYCount, boolean isFirst){
        this.count.set(count);
        this.totalCount.set(totalCount);
        this.totalPathSlotXCount.set(totalPathSlotXCount);
        this.totalPathSlotYCount.set(totalPathSlotYCount);
        this.wordSlotXCount.set(wordSlotXCount);
        this.wordSlotYcount.set(wordSlotYCount);
        this.isFirst.set(isFirst);
    }

    public void setCount(int count){
        this.count.set(count);
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

    public IntWritable getWordSlotXCount() {
        return new IntWritable(wordSlotXCount.get());
    }

    public IntWritable getWordSlotYcount() {
        return new IntWritable(wordSlotYcount.get());
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
        wordSlotXCount.write(dataOutput);
        wordSlotYcount.write(dataOutput);
        isFirst.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        path.readFields(dataInput);
        count.readFields(dataInput);
        totalCount.readFields(dataInput);
        totalPathSlotXCount.readFields(dataInput);
        totalPathSlotYCount.readFields(dataInput);
        wordSlotXCount.readFields(dataInput);
        wordSlotYcount.readFields(dataInput);
        isFirst.readFields(dataInput);
    }

    public Text getPath() {
        return new Text(path);
    }
}
