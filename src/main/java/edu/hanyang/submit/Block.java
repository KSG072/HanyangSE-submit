package edu.hanyang.submit;

public class Block {
    public int parent=0;
    public int type = 0; //1 is non-leaf, 0 is leaf
    public int nkeys = 0;
    public int max;
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
                System.arraycopy(this.keys, 0, arr, 0, pos);
                arr[pos] = key;
                if (this.nkeys + 1 - (pos + 1) >= 0)
                    System.arraycopy(this.keys, pos + 1 - 1, arr, pos + 1, this.nkeys + 1 - (pos + 1));
                break;
            }
        }
        this.keys = arr;
        return pos;
    }
    private int[] insertVal(int pos, int val){
        int[] values = new int[this.max];
        if (pos >= 0) System.arraycopy(this.vals, 0, values, 0, pos);
        values[pos] = val;
        if (nkeys + 1 - (pos + 1) >= 0)
            System.arraycopy(this.vals, pos + 1 - 1, values, pos + 1, nkeys + 1 - (pos + 1));
        return values;
    }
}