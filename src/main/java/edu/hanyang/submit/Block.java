package edu.hanyang.submit;

public class Block {
    public int parent=0;
    public int type = 0; //1 is non-leaf, 0 is leaf
    public int nkeys = 0;
    public int[] keys;
    public int[] vals;

    public Block(int maxKeys){
        this.keys = new int[maxKeys];
        this.vals = new int[maxKeys+1];
    }
    public Block(int p, int t, int n, int maxKeys){
        this.parent = p;
        this.type = t;
        this.nkeys = n;
        this.keys = new int[maxKeys];
        this.vals = new int[maxKeys+1];
    }
}