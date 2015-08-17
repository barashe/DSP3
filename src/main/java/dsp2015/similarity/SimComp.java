package dsp2015.similarity;

import dsp2015.types.PathFeatValue;

/**
 * Created by ran on 14/08/15.
 */
public class SimComp {
    private double sumMiP1 , sumMiP2 , sumMiP1P2,
            sumTfidfP1 , sumTfidfP2 , sumTfidfP1P2,
            sumDiceP1 ,  sumDiceP1P2;

    public SimComp(){
        sumMiP1 = 0;
        sumMiP2 = 0;
        sumMiP1P2 = 0;
        sumTfidfP1 = 0;
        sumTfidfP2 = 0;
        sumTfidfP1P2 = 0;
        sumDiceP1 = 0;
        sumDiceP1P2 = 0;
    }

    public void reset(){
        sumMiP1 = 0;
        sumMiP2 = 0;
        sumMiP1P2 = 0;
        sumTfidfP1 = 0;
        sumTfidfP2 = 0;
        sumTfidfP1P2 = 0;
        sumDiceP1 = 0;
        sumDiceP1P2 = 0;
    }

    public double compSim(){
        return sumMiP1P2/(sumMiP1+sumMiP2);
    }

    public double compCosine(){
        return sumTfidfP1P2/(Math.sqrt(sumTfidfP1)*Math.sqrt(sumTfidfP2));
    }

    public double compCover(){
        return sumDiceP1P2/sumDiceP1;
    }


    public void updateIntersect(PathFeatValue p1Value , PathFeatValue p2Value ){
        sumMiP1P2 += p1Value.getMi().get() + p2Value.getMi().get();
        sumTfidfP1P2 += p1Value.getTfidf().get() * p2Value.getTfidf().get();
        sumDiceP1P2 += p1Value.getDice().get();
    }

    public void update(PathFeatValue value, boolean isP1) {
        if (isP1){
            sumMiP1 += value.getMi().get();
            sumTfidfP1 += Math.pow(value.getTfidf().get(), 2);
            sumDiceP1 += value.getDice().get();
        }
        else {
            sumMiP2 += value.getMi().get();
            sumTfidfP2 += value.getTfidf().get();
        }
    }

    public static double sComp(double simX, double simY){
        return Math.sqrt(simX * simY);
    }
}

