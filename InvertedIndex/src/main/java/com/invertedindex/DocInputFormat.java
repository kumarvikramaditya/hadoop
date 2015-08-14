package com.invertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by vikram on 9/6/15.
 */
public class DocInputFormat implements WritableComparable<DocInputFormat> {
    String mapInputKey;
    String mapInputValue;

    public DocInputFormat(String mapIn, String mapVal){
        this.mapInputKey = mapIn;
        this.mapInputValue = mapVal;
    }
    public String getMapInputKey() {
        return mapInputKey;
    }

    public void setMapInputKey(String mapInputKey) {
        this.mapInputKey = mapInputKey;
    }

    public String getMapInputValue() {
        return mapInputValue;
    }

    public void setMapInputValue(String mapInputValue) {
        this.mapInputValue = mapInputValue;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(mapInputKey);
        out.writeUTF(mapInputValue);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        mapInputKey = in.readUTF();
        mapInputValue = in.readUTF();

    }

    @Override
    public int compareTo(DocInputFormat o) {
        return mapInputKey.compareTo(o.getMapInputKey());
    }
    }
