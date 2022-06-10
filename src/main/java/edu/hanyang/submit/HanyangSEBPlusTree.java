package edu.hanyang.submit;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import io.github.hyerica_bdml.indexer.BPlusTree;

import javax.print.DocFlavor;

public class HanyangSEBPlusTree implements BPlusTree {

    /**
     * B+ tree를 open하는 함수(파일을 열고 준비하는 단계 구현)
     *
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
    public RandomAccessFile meta;

    public Block root;
    public int blockPos = 1;
    public int rootIndex = 0;
    public boolean inserted = false;
    public Map<Integer, Integer> posInfo = new HashMap<Integer, Integer>();


    @Override
    public void open(String metapath, String treepath, int blocksize, int nblocks) throws IOException {
        // TODO: your code here...
        this.blocksize = blocksize;
        this.nblocks = nblocks;
        this.buf = new byte[blocksize];
        this.buffer = ByteBuffer.wrap(buf);
        this.maxKeys = (blocksize - 16) / 8;

        raf = new RandomAccessFile(treepath, "rw");
        meta = new RandomAccessFile(metapath, "rw");

        if (raf.length() == 0) {
            root = new Block();
        } else {
            rootIndex = meta.readInt();
            raf.seek(0);
            for (int i = 0; i < raf.length(); i += blocksize) {
                raf.read(buf);
                buffer.position(0);
                posInfo.put(buffer.getInt(), i);
            }
        }
    }

    private Block searchNode(int key) throws IOException { //leaf에 있는 block을 리턴
        Block block = root;
        int changed = 0;
        while (block.leaf == 0) {
            changed = 0;
            for (int i = 0; i < block.nkeys; i++) {
                if (key < block.getKey(i)) {
                    block = block.child.get(i);
                    changed = 1;
                    break;
                }
            }
            if(changed == 0) {
                block = block.child.get(block.child.size() - 1);
            }
        }
        return block;
    }

    @Override
    public void insert(int key, int val) throws IOException {
        this.inserted = true;
        Block block = searchNode(key);
        ArrayList<Integer> newNode = new ArrayList<>();
        newNode.add(key);
        newNode.add(val);
        if (block.nkeys+1 > maxKeys) {
            block.addNode(newNode);
            int mid = (int) Math.ceil((double) maxKeys / 2);
            int midKey = block.getKey(mid);
            Block newBlock = split(block, mid);
            if(block.parent == null) {
                Block newRoot = new Block(0);
                this.root = newRoot;
                this.rootIndex = newRoot.myPos;
                newRoot.lastVal = block.myPos;

                block.parent = newRoot;
                newRoot.addChild(block);
                newBlock.parent = newRoot;

            }
            block.parent.addChild(newBlock);
            insertInternal(block.parent, midKey, newBlock.myPos);
        }
        else {
            block.addNode(newNode);
            int i;
            if(block.parent != null){
                for(i=0; i<block.parent.child.size()-1; i++){
                    block.parent.child.get(i).lastVal = block.parent.child.get(i+1).myPos;
                }block.parent.child.get(i).lastVal = -1;
            }
        }
    }

    private Block split(Block block, int mid) {
        Block newBlock = new Block(block.leaf);

        for(int i=0; i<mid; i++) {
            newBlock.addNode(block.getNode(0));
            block.nodeArray.remove(0);
            block.nkeys--;
        }
        if(block.leaf == 0) {
            for(int i=0; i<=mid; i++) {
                newBlock.addChild(block.child.get(0));
                block.child.remove(0);
            }
            newBlock.lastVal = newBlock.child.get(newBlock.nkeys).myPos;
        }


        return newBlock;
    }

    /*
    TODO
        block은 root가 될 수 없음
     */
    private void insertInternal(Block block, int key, int val) throws IOException {
        ArrayList<Integer> newNode = new ArrayList<>();
        newNode.add(key);
        newNode.add(val);
        if(block.nkeys+1 > maxKeys){
            block.addNode(newNode);
            int mid = (int) Math.ceil((double) maxKeys / 2);
            int midKey = block.getKey(mid);
            Block newBlock = split(block, mid);
            block.nodeArray.remove(0); block.nkeys--;
            if(block.parent == null) {
                Block newRoot = new Block(0);
                this.root = newRoot;
                this.rootIndex = newRoot.myPos;
                newRoot.lastVal = block.myPos;

                block.parent = newRoot;
                newRoot.addChild(block);
                newBlock.parent = newRoot;
            }
            block.parent.addChild(newBlock);
            insertInternal(block.parent, midKey, newBlock.myPos);
        }
        else{
            block.addNode(newNode);
        }
    }


    @Override
    public int search(int key) throws IOException { // value 값 리턴
        root = readBlock(rootIndex); //시작노드
        return _search(root, key);
    }

    private Block readBlock(int pos) throws IOException {
        Block block = new Block();
        ArrayList<Integer> node;
        raf.seek(posInfo.get(pos));
        raf.read(buf);
        buffer.position(0);
        block.myPos = buffer.getInt();
        block.leaf = buffer.getInt();
        buffer.getInt();

//        block.myPos = raf.readInt();
//        block.leaf = raf.readInt();
//        raf.readInt(); //nkeys는 추가하면서 알아서 늘어남 기본값 0

        int key;
        int val;

        for (int i = 0; i < maxKeys; i++) {
            key = buffer.getInt();
            val = buffer.getInt();
            if (key != -1){
                node = new ArrayList<>();
                node.add(key);
                node.add(val);
                block.addNode(node);
            }
        }

        block.lastVal = buffer.getInt();

        return block;
    }

    // 변수이름바꾸고 수정할거 있으면 수정해야함
    private int _search(Block block, int key) throws IOException {
        int changed = 0;
        while (block.leaf == 0) { // non-Leaf
            changed = 0;
            for (int i = 0; i < block.nkeys; i++) {
                if (key < block.getKey(i)) { //if no such exist, set c = last non-null pointer in C
                    changed = 1;
                    block = readBlock(block.getValue(i));
                    break;
                }
            }
            if(changed == 0) block = readBlock(block.lastVal);
        }

        int left = 0, right = block.nkeys - 1, mid;
        while (left <= right) {
            mid = (left + right) / 2;
            if (key == block.getKey(mid)) {
                return block.getValue(mid);
            } else if (key < block.getKey(mid)) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return -1;
    }


    /**
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        if (inserted) {
            meta.seek(0);
            meta.writeInt(rootIndex); // 파일을 open할때 첫번째 int인 rootindex를 읽음으로써 rootindex를 알 수 있다.
            raf.seek(0);
            traverse(root);
        }
        meta.close();
        raf.close();
    }

    //이 메소드를 통해서 자신의 자식으로 제귀를 함으로써 트리의 모든 데이터를 써 내려가는 것으로 보임
    // TODO raf.writeInt(b.val0) 도 추가해줘야함
    public void traverse(Block b) throws IOException {
        raf.writeInt(b.myPos);
        raf.writeInt(b.leaf);
        raf.writeInt(b.nkeys);

        for (int i = 0; i < b.nkeys; i++) {
            int key = b.nodeArray.get(i).get(0);
            int value = b.nodeArray.get(i).get(1);
            raf.writeInt(key);
            raf.writeInt(value);
        }
        for (int i = 0; i < maxKeys - b.nkeys; i++) {
            raf.writeInt(-1);
            raf.writeInt(-1);
        }
        raf.writeInt(b.lastVal);
        int wastecount = (blocksize / 4) - (4 + (2 * maxKeys));
        for (int i = 0; i < wastecount; i++) {
            raf.writeInt(-99);
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
        public int lastVal = -1;
        //        public int parent_pos = -1;
        public ArrayList<ArrayList<Integer>> nodeArray = new ArrayList<>();
        public ArrayList<Block> child = new ArrayList<>();

        public Block parent = null; // 현재 블록의 parent block

        Block() {
            this.myPos = blockPos;
            blockPos++;
        }

        /*
         * 이 블락 클래스는 두칸의 어레이 리스트(키, 밸류)가 한칸에 들어가는 이중 어레이 리스트이다. 하지만
         * 비플러스트리에서 밸류는 키보다 하나 더 많으므로 일반 int변수 하나를 추가해서 그 변수가 value의 가장
         * 첫 번째 값을 알려준다.
         */
        Block(int leaf) {
            this.myPos = blockPos; //블락의 일련번호 , 최초의 블락은 0, 그다음부터 1,2,3식으로
            this.leaf = leaf; //1이면 리프, 0이면 논리프
            blockPos++;
        }

        public int getKey(int index) {
            return this.nodeArray.get(index).get(0);
        }

        public int getValue(int index) {
            return this.nodeArray.get(index).get(1);
        }

        public ArrayList<Integer> getNode(int index) {
            return this.nodeArray.get(index);
        }

        /*
         * addNode는 [key, value]형식의 길이가 2인 어레이리스트를 인자값으로 넣어주면
         * key의 값에 맞춰서 블락의 node리스트 내부의 적합한 위치에 맞게 넣어주는 메소드
         */
        public void addNode(ArrayList<Integer> node) {
            if (this.nkeys == 0) {
                this.nodeArray.add(node);
            } else {
                for (int i = 0; i < this.nkeys; i++) {
                    if (node.get(0) < getKey(i)) {
                        this.nodeArray.add(i, node);
                        this.nkeys++;
                        return;
                    }
                }
                this.nodeArray.add(node);
            }
            this.nkeys++;
        }

        /*
         * addChild는 해당 블록객체가 가진 자식인 블록을 저장하는 어레이리스트에 child가
         * 순서대로 정렬될 수 있게 넣어주는 메소드.
         */
        public void addChild(Block child) {
            if (this.child.size() == 0) {
                this.child.add(child);
            } else {
                for (int i = 0; i < this.child.size(); i++) {
                    if (child.myPos == this.child.get(i).myPos) return;
                    if (child.getKey(0) < this.child.get(i).getKey(0)) {
                        this.child.add(i, child);
                        return;
                    }
                }
                this.child.add(child);
            }
        }
    }

    private void write(Block block) throws IOException {
        if (posInfo.containsKey(block.myPos)) {
            raf.seek(posInfo.get(block.myPos));
        }
        else {
            raf.seek(posInfo.size());                       // posInfo의 size -> 포인터의 끝
            posInfo.put(block.myPos, posInfo.size());       // posInfo에 block의 myPos인 key와 posInfo.size인 value를 추가
        }
        buf[0] = (byte) block.myPos;
        buf[4] = (byte) block.leaf;
        buf[8] = (byte) block.nkeys;
        for (int i = 0; i < block.nkeys; i++) {
            buf[(i*4)+12] = (byte) block.getKey(i);
            buf[(i*4)+16] = (byte) block.getValue(i);
        }
        buf[blocksize-4] = (byte) block.lastVal;
        raf.write(buf);
    }

//    public void printTree(Block b, String tab) throws IOException {
//        for (int i = 0; i < b.nodeArray.size(); i++) {
//            System.out.println(tab + b.nodeArray.get(i).get(0));
//        }
//        System.out.println("---------------------------");
//        for (int i = 0; i < b.child.size(); i++) {
//            printTree(b.child.get(i), tab+'\t');
//        }
//    }
//
//    public static void main(String[] args) throws IOException {
//        String metapath = "./tmp/bplustree.meta";
//        String savepath = "./tmp/bplustree.tree";
//        int blocksize = 52;
//        int nblocks = 10;
//
//        File treefile = new File(savepath);
//        if (treefile.exists()) {
//            if (! treefile.delete()) {
//                System.err.println("error: cannot remove files");
//                System.exit(1);
//            }
//        }
//
//        HanyangSEBPlusTree tree = new HanyangSEBPlusTree();
//        tree.open(metapath, savepath, blocksize, nblocks);
//
//        tree.insert(5, 10);
//        tree.printTree(tree.root, "");
//        tree.insert(6, 15);
//        tree.printTree(tree.root, "");
//        tree.insert(4, 20);
//        tree.printTree(tree.root, "");
//        tree.insert(7, 1);
//        tree.printTree(tree.root, "");
//        tree.insert(8, 5);
//        tree.printTree(tree.root, "");
//        tree.insert(17, 7);
//        tree.printTree(tree.root, "");
//        tree.insert(30, 8);
//        tree.printTree(tree.root, "");
//        tree.insert(1, 8);
//        tree.printTree(tree.root, "");
//        tree.insert(58, 1);
//        tree.printTree(tree.root, "");
//        tree.insert(25, 8);
//        tree.printTree(tree.root, "");
//        tree.insert(96, 32);
//        tree.printTree(tree.root, "");
//        tree.insert(21, 8);
//        tree.printTree(tree.root, "");
//        tree.insert(9, 98);
//        tree.printTree(tree.root, "");
//        tree.insert(57, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(157, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(247, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(357, 254);
//        tree.printTree(tree.root, "");
//        tree.insert(557, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(400, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(567, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(700, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(557, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(558, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(559, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(560, 54);
//        tree.printTree(tree.root, "");
//        tree.insert(561, 54);
//        tree.printTree(tree.root, "");
//        tree.close();
//    }
}