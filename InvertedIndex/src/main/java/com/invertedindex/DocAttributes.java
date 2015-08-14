package com.invertedindex;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Created by vikram on 8/6/15.
 */
public class DocAttributes implements Writable {
    private Integer docNum;
    private String word;
    private Integer docFrequency;
    private Integer termFrequency;
    public DocAttributes(){}
    public DocAttributes(Integer docNum,String word,Integer docFrequency,Integer termFrequency){
        this.docNum = docNum;
        this.word = word;
        this.docFrequency = docFrequency;
        this.termFrequency = termFrequency;
    }
    public DocAttributes(DocAttributes docAttributes){
        this.docNum = docAttributes.getDocNum();
        this.docFrequency = docAttributes.getDocFrequency();
        this.word = docAttributes.getWord();
        this.termFrequency = docAttributes.getTermFrequency();
    }
    public Integer getDocNum() {
        return docNum;
    }

    public void setDocNum(Integer docNum) {
        this.docNum = docNum;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public Integer getDocFrequency() {
        return docFrequency;
    }

    public void setDocFrequency(Integer docFrequency) {
        this.docFrequency = docFrequency;
    }

    public Integer getTermFrequency() {
        return termFrequency;
    }

    public void setTermFrequency(Integer termFrequency) {
        this.termFrequency = termFrequency;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.docNum);
        dataOutput.writeUTF(this.word);
        dataOutput.writeInt(this.docFrequency);
        dataOutput.writeInt(this.termFrequency);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.docNum = dataInput.readInt();
        this.word = dataInput.readUTF();
        this.docFrequency = dataInput.readInt();
        this.termFrequency= dataInput.readInt();
    }
    @Override
    public String toString(){
        return this.word+"\t"+":"+this.docFrequency+":"+"("+this.docNum+","+this.termFrequency+")";
    }
}
