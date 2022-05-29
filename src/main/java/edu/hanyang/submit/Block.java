package edu.hanyang.submit;

public class Block {
    public int my_pos=0;
    public int type = 0; //1 is non-leaf, 0 is leaf
    public int nkeys = 0;
    public int[] keys;
    public int[] vals;

    public Block(int maxKeys){
        this.keys = new int[maxKeys];
        this.vals = new int[maxKeys+1];
    }
}