package DSP2015;

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
        private int index;

        public Word(String word, String type, int parent, int index) {
            this.word = word;
            this.type = type;
            this.parent = parent;
            this.index = index;
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Word word = (Word) o;

            return index == word.index;

        }

    }

    private class ParsedLine{
        private List<Word> words;
        private int count;

        public ParsedLine(String line) {
            words = new ArrayList<Word>();
            String[] tmp = line.split("\t");
            count = Integer.parseInt(tmp[2]);
            String[] wordsToParse = tmp[1].split(" ");
            for(int i = 0; i < wordsToParse.length; i++){
                String[] data = wordsToParse[i].split("/");
                Word word = new Word(data[0], data[1], Integer.parseInt(data[3]), i);
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

    private List<String> paths;
    private int count;

    public TripletParser() {
        count = 0;
        paths = null;
    }

    public int getCount() {
        return count;
    }

    public List<String> getPath() {
        return paths;
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

    public void parse(String line){

        ParsedLine pl = new ParsedLine(line);
        Word word;
        String path ="";
        paths = null;
        Word firstNoun = null, secondNoun= null;
        for(int i = 0; i < pl.size(); i++){
            word = pl.getWord(i);
            if(i>0)
                path += " ";
            if(word.isRoot()) {
                if (!word.isVerb()) {
                    return;
                }
                path += word.getWord();
            }
            else {
                if (word.isNoun()) {
                    if(firstNoun == null) {
                        firstNoun = word;
                        path += "X";
                    }
                    else {
                        secondNoun = word;
                        path += "Y";
                    }
                }
                else
                    path += word.getWord();

            }
        }

        if(firstNoun != null && secondNoun != null){
            if(isValid(pl, firstNoun, secondNoun) && isValid(pl, secondNoun, firstNoun)) {
                paths = new ArrayList<String>();
                paths.add(path);
            }
        }
        count = pl.getCount();

    }
}
