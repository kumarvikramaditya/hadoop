/*
package com.invertedindex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

*/
/**
 * Created by vikram on 11/6/15.
 *//*

public class TestClass {

    List<DocAttributes> list = new ArrayList<DocAttributes>();
    DocAttributes doc;

    public DocAttributes setDoc(){
        doc = new DocAttributes();
        doc.setDocFrequency(2);
        doc.setTermFrequency(2);
        doc.setWord("my");
        doc.setDocNum(1);
        list.add(doc);
        doc = new DocAttributes();
        doc.setDocFrequency(3);
        doc.setTermFrequency(3);
        doc.setWord("Goal");
        doc.setDocNum(2);
        list.add(doc);
        doc = new DocAttributes();
        doc.setDocFrequency(1);
        doc.setTermFrequency(1);
        doc.setWord("my");
        doc.setDocNum(3);
        list.add(doc);
        return doc;
    }

    public void tst(Iterator<DocAttributes> value){
        List<DocAttributes> allList = new ArrayList<DocAttributes>();
        DocAttributes docAttributes = new DocAttributes();
        while(value.hasNext()){
            docAttributes = value.next();
            allList.add(docAttributes);
        }
        for(int i=0;i<allList.size();i++){

            for(int j=i+1;j<allList.size();j++){
                System.out.println("COMPARING->"+allList.get(i).getWord()+"###"+allList.get(j).getWord());
                if(allList.get(i).getWord().matches(allList.get(j).getWord())){
                    allList.get(i).setDocFrequency(allList.get(i).getDocFrequency()+allList.get(j).getDocFrequency());
                    allList.remove(j);
                }
            }
        }
        System.out.println("List->"+allList.toString());
    }

    public static void main(String[] args) {
        TestClass t = new TestClass();
        t.setDoc();
        Iterator<DocAttributes> iterator = t.list.iterator();
        t.tst(iterator);
    }
}
*/
