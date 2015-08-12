package dsp2015.path_slot_count;

import dsp2015.types.PathFeatValue;
import dsp2015.types.PathValue;

/**
 * Created by barashe on 8/12/15.
 */
public class StatComp {

    private double mi;
    private double tfidf;
    private double dice;

    public StatComp() {

    }

    private double compMi(double slotCount, double count, double wordCount, double pathCount){
        return Math.log(slotCount) + Math.log(count) - Math.log(wordCount) - Math.log(pathCount);
    }

    private double compTfidf(double count, double wordCount){
        return (count)/(1 + Math.log(wordCount));
    }
    
    private double compDice(double count, double pathCount, double wordCount){
        return (2*count)/(pathCount + wordCount);
    }

    public void comp(PathValue p){
        double slotCount = p.getTotalCount().get();
        double count = p.getCount().get();
        double wordCount = p.getWordSlotCount().get();
        double pathCount = p.getTotalPathSlotCount().get();
        mi = compMi(slotCount, count, wordCount, pathCount);
        tfidf = compTfidf(count, wordCount);
        dice = compDice(count, pathCount, wordCount);
        
    }

    public double getMi() {
        return mi;
    }

    public double getTfidf() {
        return tfidf;
    }

    public double getDice() {
        return dice;
    }
}
