package edu.hanyang.submit;

import org.apache.commons.lang3.tuple.MutableTriple;

import java.io.DataInputStream;
import java.io.IOException;

class DataManager {
    public boolean isEOF = false;
    private DataInputStream dis = null;
    public MutableTriple<Integer,Integer,Integer> tuple = new MutableTriple<Integer,Integer,Integer>(0,0,0);

    public DataManager (DataInputStream dis) throws IOException {...}

    private boolean readNext() throws IOException {
        if (isEOF) return false;
        tuple.setLeft(dis.readInt()); tuple.setMiddle(dis.readInt()); tuple.setRight(dis.readInt());
        return true;
    }

    public void getTuple(MutableTriple<Integer, Integer, Integer> ret) throws IOException {
        ret.setLeft(tuple.getLeft());
        ret.setMiddle(tuple.getMiddle());
        ret.setRight(tuple.getRight());
        isEOF = (!readNext());
    }
}

