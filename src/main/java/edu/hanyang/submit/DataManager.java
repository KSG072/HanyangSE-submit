package edu.hanyang.submit;
import org.apache.commons.lang3.tuple.MutableTriple;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.util.ArrayList;

class DataManager {
    public boolean isEOF = false;
    private byte[] buffer = null;
    private BufferedInputStream dis = null;
    private DataInputStream inputStream = null;
    public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>(0, 0, 0);

    public DataManager(BufferedInputStream dis, int blocksize) throws IOException {
        this.dis = dis;
        this.buffer = new byte[blocksize];
        DataInputStream inputStream = new DataInputStream(dis);
        inputStream.read(buffer);
    }
    private int nextRead() throws IOException{
        return this.dis.read(buffer);
    }

    private boolean readNext() throws IOException {
        if (isEOF) {
            if(nextRead() != -1)

        }
        tuple.setLeft(inputStream.readInt());
        tuple.setMiddle(inputStream.readInt());
        tuple.setRight(inputStream.readInt());
        return true;
    }

    public void getTuple(MutableTriple<Integer, Integer, Integer> ret) throws IOException {
        ret.setLeft(tuple.getLeft());
        ret.setMiddle(tuple.getMiddle());
        ret.setRight(tuple.getRight());
        isEOF = (!readNext());
    }
}
