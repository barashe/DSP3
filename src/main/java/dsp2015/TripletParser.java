package dsp2015;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by barashe on 8/9/15.
 */
public class TripletParser {

    private class Word{
        private String word;
        private String type;
        private int parent;

        public Word(String word, String type, int parent) {
            this.word = word;
            this.type = type;
            this.parent = parent;
        }

        public String getWord() {
            return word;
        }

        public String getType() {
            return type;
        }

        public int getParent() {
            return parent;
        }

        public boolean isRoot(){
            return (parent ==0);
        }

        public boolean isNoun(){
            return (type.charAt(0) == 'N' || type.equals("PRP"));
        }

        public boolean isVerb(){
            return (type.charAt(0) == 'V');
        }



    }

    private class ParsedLine{
        private List<Word> words;
        private int count;
        private boolean isNull = false;

        public boolean isNull(){
            return isNull;
        }

        public ParsedLine(String line){
            words = new ArrayList<Word>();
            String[] tmp = line.split("\t");
            count = Integer.parseInt(tmp[2]);
            String[] wordsToParse = tmp[1].split(" ");
            for(int i = 0; i < wordsToParse.length; i++){
                String[] data = wordsToParse[i].split("/");
                if (data.length!=4){
                    isNull = true;
                    break;
                }
                Word word = new Word(data[0], data[1], Integer.parseInt(data[data.length-1]));
                words.add(word);
            }
        }



        public Word getWord(int index){
            return words.get(index);
        }

        public Word getParent(Word word){
            if(word.getParent() == 0)
                return null;
            return words.get(word.getParent() - 1);
        }

        public int getCount() {
            return count;
        }

        public int size(){
            return words.size();
        }
    }

    private String[] paths;
    private int count;
    private String w1, w2;

    public TripletParser() {
        count = 0;
        paths = null;
    }

    public int getCount() {
        return count;
    }

    public String[] getPath() {
        return paths;
    }

    public String getW1() {
        return w1;
    }

    public String getW2() {
        return w2;
    }

    private boolean isValid(ParsedLine pl, Word noun, Word otherNoun){
        Word w = pl.getParent(noun);
        while(w != null){
            if(w.equals(otherNoun))
                return false;
            w = pl.getParent(w);
        }
        return true;
    }

    private String extractReversePath(ParsedLine pl, Word w, boolean root, String xy){
        if(w.isRoot()){
            if(root)
                return w.getWord();
            else
                return "";
        }
        String toWrite = (w.isNoun()? xy: w.getWord());
        return extractReversePath(pl, pl.getParent(w), root, xy) +" "+ toWrite;
    }

    private String extractPath(ParsedLine pl, Word firstNoun, Word secondNoun, boolean forward){
        if(forward)
            return extractReversePath(pl, firstNoun, false, "X") + " " + extractReversePath(pl, secondNoun, true, "Y");
        return extractReversePath(pl, firstNoun, false, "Y") + " " + extractReversePath(pl, secondNoun, true, "X");


    }

    public void parse(String line) {

        ParsedLine pl = new ParsedLine(line);
        Word word;
        paths = null;
        if(pl.isNull())
            return;
        Word firstNoun = null, secondNoun= null;
        for(int i = 0; i < pl.size(); i++){
            word = pl.getWord(i);
            if(word.isRoot()) {
                if (!word.isVerb()) {
                    return;
                }

            }
            else {
                if (word.isNoun()) {
                    if(firstNoun == null) {
                        firstNoun = word;
                        w1 = word.getWord();
                    }
                    else {
                        secondNoun = word;

                        w2 = word.getWord();
                    }
                }

            }
        }

        if(firstNoun != null && secondNoun != null){
            if(isValid(pl, firstNoun, secondNoun) && isValid(pl, secondNoun, firstNoun)) {
                paths = new String[2];
                paths[0] = extractPath(pl, firstNoun, secondNoun, true).substring(1);
                paths[1] = extractPath(pl, secondNoun, firstNoun, false).substring(1);
            }
        }
        count = pl.getCount();

    }
}
