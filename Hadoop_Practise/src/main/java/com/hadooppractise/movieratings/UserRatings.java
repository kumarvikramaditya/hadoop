package com.hadooppractise.movieratings;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;


public class UserRatings implements Writable{
  //  private Integer userId;
    private Integer movieId;
    private Integer rating;
    private Integer time;

    public UserRatings(){}
    public UserRatings(Integer movieId, Integer rating, Integer time){
       // this.userId= userId;
        this.movieId= movieId;
        this.rating = rating;
        this.time = time;
    }
   /* public Integer getUserId() {
        return userId;
    }

    public void setUserId(Integer userId) {
        this.userId = userId;
    }*/

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }

    public Integer getTime() {
        return time;
    }

    public void setTime(Integer time) {
        this.time = time;
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
      //  dataOutput.writeInt(userId);
        dataOutput.writeInt(movieId);
        dataOutput.writeInt(rating);
        dataOutput.writeInt(time);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       // userId.intValue();
        this.movieId = dataInput.readInt();
        this.rating = dataInput.readInt();
        this.time = dataInput.readInt();
    }
    @Override
    public String toString(){
        return Integer.toString(movieId)+"\t"+Integer.toString(rating)+"\t"+Integer.toString(time);
    }

//    @Override
//    public boolean hasNext() {
//        return false;
//    }
//
//    @Override
//    public UserRatings next() {
//        return null;
//    }
//
//    @Override
//    public void remove() {
//
//    }
}
