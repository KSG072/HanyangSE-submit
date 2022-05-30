package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import io.github.hyerica_bdml.indexer.BPlusTree;


public class HanyangSEBPlusTree implements BPlusTree {

    /**
     * B+ tree를 open하는 함수(파일을 열고 준비하는 단계 구현)
     * @param metafile B+ tree의 메타정보 저장(저장할거 없으면 안써도 됨)
     * @param treefile B+ tree의 메인 데이터 저장
     * @param blocksize B+ tree 작업 처리에 이용할 데이터 블록 사이즈
     * @param nblocks B+ tree 작업 처리에 이용할 데이터 블록 개수
     * @throws IOException
     */

    public int blocksize;
    public int nblocks;
    public byte[] buf;
    public ByteBuffer buffer;
    public int maxKeys;
    public RandomAccessFile raf;

    public Block root;
    public int blockNum = 1;
    public int rootIndex = 0;
    public boolean inserted = false;


    @Override
    public void open(String metapath, String treepath, int blocksize, int nblocks) throws IOException {
        // TODO: your code here...
        this.blocksize = blocksize;
        this.nblocks = nblocks;
        this.buf = new byte[blocksize];
        this.buffer = ByteBuffer.wrap(buf);
        this.maxKeys = (blocksize - 16) / 8;

        raf = new RandomAccessFile(treepath, "rw");

        if (raf.length() != 0) {
            rootIndex = raf.readInt();
        }
        else{
            root = new Block();
        }
    }

    @Override
    public void insert(int key, int val) throws IOException {
//        TODO: your code here...
        inserted = true;
        Block block = searchNode(key);

        insertInternal(block, key, val);
    }

    private Block searchNode(int key) throws IOException { //leaf에 있는 block을 리턴
        Block b = root; //
        Block tmp = root;
        while (tmp.leaf == 0) { // leaf일때까지 반복
            for (int i = 0; i < tmp.nkeys; i++) {
                if(tmp.nodeArray.get(i).get(0) > key){
                    tmp = tmp.child.get(i-1);
                    break;
                }
            }
        }
        return tmp;
    }

    private Block split(Block block, int key, int val) {
        int keyNum = block.nkeys + 1;
        int mid = (int)Math.ceil((double)keyNum/2); // 한 block의 key의 갯수를 반으로 쪼개서 올림한 수

        Block newBlock = new Block(blockNum, 1, 0);

        ArrayList<Integer> newNode = new ArrayList(); // 새로운 데이터 arraylist 생성
        newNode.add(key);
        newNode.add(val);

        block.addNode(newNode); // 새로운 데이터 추가

        for(int i = mid+1; i < block.nkeys; i++) { // new block에 절반 이상 만큼 넣기
            newBlock.addNode(new ArrayList<>(block.nodeArray.get(i)));
        }

        for(int i = blockNum; i > mid ; i--) { // new block에서 m부터 그 뒤에 제거하기
            block.nodeArray.remove(new ArrayList<>(block.nodeArray.get(i)));
            block.nkeys--;
        }

        return newBlock;
    }

    private void insertInternal(Block block, int key, int val) throws IOException {
//        int pMid = (int)Math.ceil((double)parent.nkeys/2) + 1; // 중간 위치
        if(block.nkeys == maxKeys) { // 해당 block에 insert할 자리가 없으면 block 하나 더 만들어서 쪼갠 뒤 재귀
            blockNum++;
            Block newBlock = split(block, key, val);
            int midKey = newBlock.nodeArray.get(1).get(0);

            if(block.parent == null) { //root is full
                blockNum++;
                rootIndex = blockNum;
                Block newRoot = new Block(blockNum, 0, 0);
                root = newRoot;
                newRoot.addChild(block);
                newRoot.addChild(newBlock);
                block.parent = newRoot;
                newBlock.parent = newRoot;
                newRoot.nodeArray.get(0).set(1, newRoot.child.get(0).myPos);
                newRoot.nodeArray.get(1).set(1, newRoot.child.get(1).myPos);
            }else if (block.leaf == 0) { //internal is full

            }else{//leaf is full
                Block Parent = block.parent;
            }
        }
        else{
            if(block.leaf == 0){ // don't remain midKey in block

            }
            else{ // remain midKey in block

            }
        }
    }
    @Override
    public int search(int key) throws IOException { // value 값 리턴
        root = readBlock(root_index); //시작노드
        return _search(root, key);
    }

    private Block readBlock(int my_pos) throws IOException {
        Block new_block = new Block();
        for (int i = 4; i < raf.length(); i+=blocksize) { //8192
//            i += (block_numbers-my_pos)*blocksize;
            raf.seek(i);
            int this_pos = raf.readInt();
            if (this_pos== my_pos) {

                new_block.my_pos = this_pos;
                new_block.leaf = raf.readInt();
                new_block.nkeys = raf.readInt();
                raf.readInt();
                new_block.data.get(0).set(1, raf.readInt());
//                new_block.parent_pos = raf.readInt();

                for (int j = 0; j < new_block.nkeys; j++) {
                    ArrayList<Integer> newdata = new ArrayList(); // 새로운 데이터 arraylist 생성
                    newdata.add(raf.readInt()); newdata.add(raf.readInt());
                    new_block.data.add(newdata);
                }

            }
        }
        return new_block;
    }

    private int _search(Block b, int key) throws IOException {
        Block child = b;
        if (b.leaf == 0) { // non-Leaf
            for (int i = 1; i < b.nkeys+1; i++) {
                if (key > b.data.get(i).get(0)) { //if no such exist, set c = last non-null pointer in C
                    child = readBlock(b.data.get(i).get(1)); //변경 i->i+1 / child 맨마지막 child에서 불러오기
                }
                else { //key <= b.keys[i] 되는 값 찾은 경우
                    if (b.data.get(i).get(0) == key) child =  readBlock(b.data.get(i).get(1)); //i+1불러오는거
                    else child =  readBlock(b.data.get(i-1).get(1)); //i 불러오는거
                    break;
                }
            }
            return _search(child, key);
        }
        else { // Leaf
            /* binary or linear search*/

            int low = 1, high = b.nkeys, mid;

            while (low <= high) {
                mid = (low + high) / 2;
                if (key==b.data.get(mid).get(0)) return b.data.get(mid).get(1);
                else if (key<b.data.get(mid).get(0)) high = mid-1;
                else low = mid+1;
            }
            return -1; // 값을 못찾으면 -1
        }
    }


    /**
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // TODO: your code here...
        if (inserted) {
            raf.writeInt(root_index);
            traverse(root);
        }
        raf.close();
    }

    public void traverse(Block b) throws IOException {

        raf.writeInt(b.my_pos);
        raf.writeInt(b.leaf);
        raf.writeInt(b.nkeys);

        for (int i = 0; i < b.data.size(); i++) {
            int key = b.data.get(i).get(0);
            int value = b.data.get(i).get(1);
            raf.writeInt(key); raf.writeInt(value);
        }
        for (int i = b.data.size(); i < (blocksize - 12)/8; i += 1) {
            raf.writeInt(-1); raf.writeInt(-1);
        }

        if (b.leaf == 0) {
            for (int i = 0; i < b.child.size(); i++) {
                traverse(b.child.get(i));
            }
        }
    }

    class Block {

        public int myPos = 0;
        public int leaf = 1;
        public int nkeys = 0; // block 안에 있는 key의 개수
        //        public int parent_pos = -1;
        public ArrayList<ArrayList<Integer>> nodeArray = new ArrayList<>();
        public ArrayList<Block> child = new ArrayList<>();

        public Block parent = null; // 현재 블록의 parent block

        Block() {
        }

        Block(int myPos, int leaf, int nkeys) {
            this.myPos = myPos;
            this.leaf = leaf;
            this.nkeys = nkeys;
        }

        public void addNode(ArrayList<Integer> node){
            int newkey = node.get(0);
            ArrayList<ArrayList<Integer>> newNode = new ArrayList<>();
            int i;
            for(i=0; i<this.nkeys; i++){
                if(newkey <= this.nodeArray.get(i).get(0)){
                    newNode.add(this.nodeArray.get(i));
                }
                else{
                    newNode.add(node);
                    newNode.add(this.nodeArray.get(i));
                    break;
                }
            }
            this.nodeArray = newNode;
            this.nkeys++;
        }

        public void addChild(Block child){
            ArrayList<Block> newChild = new ArrayList<>();
            for(int i=0; i<this.nkeys+1; i++){
                if(child.nodeArray.get(0).get(0) <= this.child.get(i).nodeArray.get(0).get(0)){
                    newChild.add(this.child.get(i));
                }
                else{
                    newChild.add(child);
                    newChild.add(this.child.get(i));
                }
            }
        }
    }
}