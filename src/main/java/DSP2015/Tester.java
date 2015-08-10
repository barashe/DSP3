package DSP2015;

import java.io.*;

/**
 * Created by barashe on 8/10/15.
 */
public class Tester {

    public static void main(String[] args){
        TripletParser p = new TripletParser();
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream("/home/barashe/IdeaProjects/DSP3/src/main/java/DSP2015/out3");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

        String strLine;

        //Read File Line By Line
        try {
            while ((strLine = br.readLine()) != null)   {
                // Print the content on the console
                p.parse(strLine);
                if(p.getPath() != null){
                    System.out.println(p.getPath().get(0) + "\n\tX: " + p.getW1() + " Y: " + p.getW2() + " Count: " + p.getCount());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //Close the input stream
        try {
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
