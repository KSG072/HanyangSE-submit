package edu.hanyang.submit;

public class Block {
    public int parent=0;
    public int type = 0; //1 is non-leaf, 0 is leaf
    public int nkeys = 0;
    private int max;
    public int[] keys;
    public int[] vals;

    public Block(int maxKeys){
        this.max = maxKeys;
        this.keys = new int[maxKeys];
        this.vals = new int[maxKeys+1];
    }
    public Block(int p, int t, int n, int maxKeys){
        this.parent = p;
        this.type = t;
        this.nkeys = n;
        this.max = maxKeys;
        this.keys = new int[maxKeys];
        this.vals = new int[maxKeys+1];
    }
    public int addKey(int key, int value){
        if(this.max == this.nkeys) return -1;
        this.vals = insertVal(insertKey(key), value);
        nkeys++;
        return 1;
    }
    private int insertKey(int key){
        int[] arr = new int[this.max];
        int pos;
        for(pos=0; pos<this.nkeys; pos++){
            if(key < this.keys[pos]){
                for(int j=0; j<pos; j++){
                    arr[j] = this.keys[j];
                }
                arr[pos] = key;
                for(int j=pos+1; j<this.nkeys+1; j++){
                    arr[j] = this.keys[j-1];
                }
                break;
            }
        }
        this.keys = arr;
        return pos;
    }
    private int[] insertVal(int pos, int val){
        int[] values = new int[this.max];
        for(int i=0; i<pos; i++){
            values[i] = this.vals[i];
        }
        values[pos] = val;
        for(int i=pos+1; i<nkeys+1; i++){
            values[i] = this.vals[i-1];
        }
        return values;
    }
}