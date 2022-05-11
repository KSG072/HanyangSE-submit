package edu.hanyang.submit;
import org.apache.commons.lang3.tuple.MutableTriple;

import java.io.DataInputStream;
import java.io.IOException;

class DataManager {
    public boolean isEOF = false;
    private DataInputStream dis = null;
    public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);

    public DataManager(DataInputStream dis) throws IOException {
        this.dis = dis;
        tuple.setLeft(this.dis.readInt());
        tuple.setMiddle(this.dis.readInt());
        tuple.setRight(this.dis.readInt());
    }

    private boolean readNext() throws IOException {
        try {
            tuple.setLeft(dis.readInt());
            tuple.setMiddle(dis.readInt());
            tuple.setRight(dis.readInt());
            return true;
        } catch ( Exception e ) {
            return false;
        }
    }

    public void getTuple(MutableTriple<Integer, Integer, Integer> ret) throws IOException {
        ret.setLeft(tuple.getLeft());
        ret.setMiddle(tuple.getMiddle());
        ret.setRight(tuple.getRight());
        isEOF = (!readNext());
    }

//    public MutableTriple<Integer, Integer, Integer> getTuple() throws IOException {
//        if (!isEOF) {
//            MutableTriple<Integer, Integer, Integer> tmp = tuple;
//            isEOF = (!readNext());
//            return tmp;
//        }
//        else
//            return null;
//    }

    public int compare(DataManager o1, DataManager o2) {
        return o1.tuple.compareTo(o2.tuple);
    }
}
