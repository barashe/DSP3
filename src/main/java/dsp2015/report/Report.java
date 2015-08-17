package dsp2015.report;

import org.apache.commons.io.FileUtils;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.data.xy.DefaultTableXYDataset;
import org.jfree.data.xy.XYSeries;

import java.io.*;
import java.util.*;

/**
 * Created by ran on 12/08/15.
 */
public class Report {
    static String negativeSetPath = "/home/ran/Documents/DSP3/negative-preds.txt";
    static String positiveSetPath = "/home/ran/Documents/DSP3/positive-preds.txt";
    static Map<String, Boolean> negativeTable;
    static Map<String, Boolean> positiveTable;
    /*args[0]=input
    *
    * */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("args problem");
        }
        Report report = new Report();
        negativeTable = report.readTestSet(negativeSetPath);
        positiveTable = report.readTestSet(positiveSetPath);
        Map<String,Double[]> input = new HashMap<String, Double[]>();
        try {
            input = report.parse(FileUtils.readLines(new File(args[0])));
            report.normalize(input);
        } catch (IOException e) {
            e.printStackTrace();
        }

        List<List<Integer[]>> tpFnTnFp = report.generateCurves(input);
        double[][] precisions = calculatePrecs(tpFnTnFp);
        double[][] recalls = calculateRecalls(tpFnTnFp);
        XYSeries xySeries;
        DefaultTableXYDataset dataSet = new DefaultTableXYDataset();
        for (int j = 0; j < 3 ; j++){
            String simParameter="";
            switch (j){
                case(0):
                    simParameter = "PMI-LIN";
                    break;
                case(1):
                    simParameter = "TFIDF-COSINE";
                    break;
                case(2):
                    simParameter = "DICE-COVER";
                    break;
            }
            xySeries = new XYSeries(simParameter , false , false);
            for (int i = 0 ; i<11 ; i++){
                xySeries.addOrUpdate(recalls[i][j], precisions[i][j]);
            }
            dataSet.addSeries(xySeries);
        }
        String newFileOutPath = "/home/ran/Documents/DSP3/PrecisionRecallCurve.jpeg";
        JFreeChart chart = ChartFactory.createXYLineChart("PrecisionRecall curves", "Recall", "Precision", dataSet);
        try {
            ChartUtilities.saveChartAsJPEG(new File(newFileOutPath), chart, 1000, 1000);
        } catch (IOException e) {
            System.err.println("Problem occurred creating chart.");
        }



    }

    private static double[][] calculatePrecs(List<List<Integer[]>> tpFnTnFp) {
        double [][] res = new double[11][3];
        int i = 0;
        for (List<Integer[]> sims : tpFnTnFp) {
            int j =0;
            for (Integer[] sim : sims) {
                res[i][j] = sim[0]/(double)(sim[0]+sim[3]);
                j++;
            }
            i++;
        }
        return res;
    }
    private static double[][] calculateRecalls(List<List<Integer[]>> tpFnTnFp) {
        double [][] res = new double[11][3];
        int i = 0;
        for (List<Integer[]> sims : tpFnTnFp) {
            int j =0;
            for (Integer[] sim : sims) {
                res[i][j] = sim[0]/(double)(sim[0]+sim[1]);
                j++;
            }
            i++;
        }
        return res;
    }

    private List<List<Integer[]>> generateCurves(Map<String, Double[]> input) {
        List<List<Integer[]>> res = new ArrayList<List<Integer[]>>();
        for (double threshold = 0; threshold < 0.5 ; threshold+=0.05) {
            Integer[] tmpSetMi = {0,0,0,0};
            Integer[] tmpSetTfidf = {0,0,0,0};
            Integer[] tmpSetCosine = {0,0,0,0};
            for (Map.Entry<String, Double[]> entry : input.entrySet()) {
                boolean overThresh1 = entry.getValue()[0]>=threshold;
                boolean overThresh2 = entry.getValue()[1]>=threshold;
                boolean overThresh3 = entry.getValue()[2]>=threshold;
                boolean isPositive = positiveTable.containsKey(entry.getKey());
                boolean isNegative = negativeTable.containsKey(entry.getKey());

                if (isPositive){
                    if (overThresh1) { tmpSetMi[0]++;}
                    if (overThresh2) { tmpSetTfidf[0]++;}
                    if (overThresh3) { tmpSetCosine[0]++;}
                }
                if (isNegative){
                    if (!overThresh1) { tmpSetMi[2]++;}
                    if (!overThresh2) { tmpSetTfidf[2]++;}
                    if (!overThresh3) { tmpSetCosine[2]++;}
                }
            }
            int positiveSize = positiveTable.size();
            int negativeSize = negativeTable.size();
            tmpSetMi[1] = positiveSize - tmpSetMi[0];
            tmpSetMi[3] = negativeSize - tmpSetMi[2];
            tmpSetTfidf[1] = positiveSize - tmpSetTfidf[0];
            tmpSetTfidf[3] = negativeSize - tmpSetTfidf[2];
            tmpSetCosine[1] = positiveSize - tmpSetCosine[0];
            tmpSetCosine[3] = negativeSize - tmpSetCosine[2];
            List<Integer[]> tmpList = new ArrayList<Integer[]>();
            tmpList.add(tmpSetMi);
            tmpList.add(tmpSetTfidf);
            tmpList.add(tmpSetCosine);
            res.add(tmpList);
        }
        return res;
    }

    private Map<String, Double[]> parse(List<String> toParse) {
        Map<String,Double[]> res = new HashMap<String, Double[]>();
        if (toParse==null){
            return null;
        }
        for (String line : toParse) {
            Double[] values = new Double[3];
            String[] splitted = line.split("\t");
            res.put(splitted[0]+"\t"+splitted[1], new Double[]{Double.valueOf(splitted[3]), Double.valueOf(splitted[5]), Double.valueOf(splitted[7])});
        }
        return res;
    }

    public Map<String, Boolean> readTestSet(String path) {
        List<String> lines;
        Map<String, Boolean> res = new HashMap<String, Boolean>();
        try {
            lines = FileUtils.readLines(new File(path));
            if (lines != null) {
                for (String line : lines) {
                    res.put(line, false);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return res;
    }

    public void normalize(Map<String, Double[]> input){
        double[][] minMax = findMinMax(input);
        for (Double[] values : input.values()) {
            for (int i = 0; i <3 ; i++) {
                values[i] = values[i]/minMax[1][i];
            }
        }
    }

    private double[][] findMinMax(Map<String, Double[]> input) {
        double[][] minMaxMatrix = {{Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY,Double.POSITIVE_INFINITY},
                                    {Double.NEGATIVE_INFINITY,Double.NEGATIVE_INFINITY,Double.NEGATIVE_INFINITY}};
        for (Double[] values : input.values()) {
            for (int i = 0; i <3 ; i++) {
                if (values[i]<minMaxMatrix[0][i])
                    minMaxMatrix[0][i] = values[i];
                if (values[i]>minMaxMatrix[1][i])
                    minMaxMatrix[1][i] = values[i];
            }
        }
        return minMaxMatrix;
    }
}
