package DSP2015;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by barashe on 8/6/15.
 */
public class PathKey implements Writable, WritableComparable<PathKey> {

    private Text path;
    private Text word;
    private BooleanWritable slot; //if true slot = left, if false slot = right
    private BooleanWritable isFirst;

    public PathKey() {
        path = new Text();
        word = new Text();
        slot = new BooleanWritable();
        isFirst = new BooleanWritable();
    }

    public PathKey(Text path, Text word, BooleanWritable slot) {
        this.path = path;
        this.word = word;
        this.slot = slot;
    }

    public PathKey(String path, String word, boolean slot) {
        this(new Text(path), new Text(word), new BooleanWritable(slot));
    }

    public void set(PathKey other){
        path.set(other.getPath());
        word.set(other.getWord());
        slot.set(other.getSlot().get());
        isFirst.set(other.getIsFirst().get());

    }

    public void set(String path, String word, boolean slot, boolean isFirst){
        this.path.set(path);
        this.word.set(word);
        this.slot.set(slot);
        this.isFirst.set(isFirst);
    }

    public void setFirst(boolean isFirst){
        this.isFirst.set(isFirst);
    }

    public Text getPath() {
        return new Text(path);
    }

    public Text getWord() {
        return new Text(word);
    }

    public BooleanWritable getSlot() {
        return new BooleanWritable(slot.get());
    }

    public BooleanWritable getIsFirst() {
        return new BooleanWritable(isFirst.get());
    }

    public int compareTo(PathKey pathKey) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        path.write(dataOutput);
        word.write(dataOutput);
        slot.write(dataOutput);
        isFirst.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        path.readFields(dataInput);
        word.readFields(dataInput);
        slot.readFields(dataInput);
        isFirst.readFields(dataInput);
    }
}
