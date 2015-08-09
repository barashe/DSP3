package DSP2015;

/**
 * Created by barashe on 8/9/15.
 */
public class TripletParser {

    public TripletParser() {
        count = 0;
        path = "";
    }

    public int getCount() {
        return count;
    }

    public String getPath() {
        return path;
    }

    private String path;
    private int count;

    public void parse(String line){
        String[] tmp = line.split("\t");
        path = tmp[1];
        count = Integer.parseInt(tmp[2]);
    }
}
