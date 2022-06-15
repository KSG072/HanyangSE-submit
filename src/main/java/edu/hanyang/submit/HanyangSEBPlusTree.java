package edu.hanyang.submit;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import io.github.hyerica_bdml.indexer.BPlusTree;

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
    public int blockPos = 0;
    public int rootIndex = 0;
    public boolean inserted = false;
    public ArrayList<Block> blockBuffer = new ArrayList<>(); //insert가 끝나고 써야하는 블락들을 모아놓은 버퍼


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
            root = new Block(1);
            rootIndex = root.myPos;
            writeBlock(root);
        } else {
            raf.seek(0);
            rootIndex = meta.readInt();
        }

    }

    private Block searchNode(int key) throws IOException { //leaf에 있는 block을 리턴
        Block block = readBlock(rootIndex);
        boolean flag;
        while (block.leaf == 0) {
            flag = true;
            for (int i = 0; i < block.nkeys; i++) {
                if (key < block.getKey(i)) {
                    block = readBlock(block.getValue(i));
                    flag = false;
                    break;
                }
            }
            if(flag) {
                block = readBlock(block.lastVal);
            }
        }
        return block;
    }

    @Override
    public void insert(int key, int val) throws IOException {
        this.inserted = true;
        Block block = searchNode(key);
        blockBuffer.add(block);
        ArrayList<Integer> newNode = new ArrayList<>();
        newNode.add(key);
        newNode.add(val);
        if (block.nkeys+1 > maxKeys) {
            block.addNode(newNode);
            int mid = (int) Math.ceil((double) maxKeys / 2);
            int midKey = block.getKey(mid);
            Block newBlock = split(block, mid);
            blockBuffer.add(newBlock);
            if(block.parent == -1) {
                Block newRoot = new Block(0);
                blockBuffer.add(newRoot);
                this.root = newRoot;
                this.rootIndex = newRoot.myPos;
                newRoot.lastVal = block.myPos;

                meta.seek(0);
                meta.writeInt(rootIndex);

                block.parent = newRoot.myPos;
                newRoot.child.add(block.myPos);
                newBlock.parent = block.parent;
                insertInternal(newRoot, midKey, newBlock.myPos);
            }
            else{
                Block parent = readBlock(block.parent);
                newBlock.parent = parent.myPos;
                insertInternal(parent, midKey, newBlock.myPos);
            }
        }
        else {
            block.addNode(newNode);
        }
        for(int i=0; !blockBuffer.isEmpty(); i++){
            writeBlock(blockBuffer.get(0));
            blockBuffer.remove(0);
        }
    }

    private Block split(Block block, int mid) {
        Block newBlock = new Block(block.leaf);

        for(int i=0; i<mid; i++) {
            newBlock.addNode(block.getNode(0));
            block.nodeArray.remove(0);
            block.nkeys--;
        }
        return newBlock;
    }

    /*
    TODO
        논리프 블락이 스플릿 될경우 새블락의 차일드로 들어가는 블락들은 부모가 바뀌지 않아서 서치하는데 오류가 발생함
        그래서 필요한 것은 블락에 ㅇ차일드의 마이포스만 저장하는 배열을 만들고 논리프가 스플릿됐을때 그 배열을 재설정해야함
        하지만 이렇게 할 경우 블락버퍼에 들어가는 블락이 매우 많아질 수 있음. 하지만 이건 일단 고려안하고 위의 내용을 구현한 다음에
        블락버퍼에서 힙에러가 발생할 경우 중간에 한번 써서 비워주는 식으로 수정 필요.
     */
    private void insertInternal(Block block, int key, int val) throws IOException {
        ArrayList<Integer> newNode = new ArrayList<>();
        blockBuffer.add(block);
        newNode.add(key);
        newNode.add(val);
        block.addNode(newNode);
        if(block.nkeys > maxKeys){
            int mid = (int) Math.ceil((double) maxKeys / 2);
            int midKey = block.getKey(mid);
            Block newBlock = split(block, mid);
            blockBuffer.add(newBlock);
            newBlock.lastVal = block.getValue(0);
            block.nodeArray.remove(0); block.nkeys--;
            newBlock.setChild();
            block.setChild();
            newBlock.refreshParent();
            block.refreshParent();
            if(block.parent == -1) {
                Block newRoot = new Block(0);
                blockBuffer.add(newRoot);
                this.root = newRoot;
                this.rootIndex = newRoot.myPos;
                meta.seek(0);
                meta.writeInt(rootIndex);
                newRoot.lastVal = block.myPos;

                block.parent = newRoot.myPos;
                newBlock.parent = block.parent;
                insertInternal(newRoot, midKey, newBlock.myPos);
            }
            else{
                Block parent = readBlock(block.parent);
                newBlock.parent = parent.myPos;
                insertInternal(parent, midKey, newBlock.myPos);
            }
        }
        else{
            block.setChild();
        }
    }


    @Override
    public int search(int key) throws IOException { // value 값 리턴
        root = readBlock(rootIndex); //시작노드
        return _search(root, key);
    }

    private Block readBlock(int pos) throws IOException {
        if(!blockBuffer.isEmpty()){
            for(Block blockInBuffer : blockBuffer){
                if(blockInBuffer.myPos == pos){
                    return blockInBuffer;
                }
            }
        }
        Block block = new Block();
        ArrayList<Integer> node;
        raf.seek((long) blocksize*pos);
        raf.read(buf);
        buffer.position(0);
        block.myPos = buffer.getInt();
        block.leaf = buffer.getInt();
        block.parent = buffer.getInt();

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
        }
        meta.close();
//        raf.seek(0);
//        int i =0;
//        while(raf.getFilePointer()<raf.length()){
//            System.out.print(raf.readInt()+" ");
//            i++;
//            if(i % (blocksize/4) == 0) {
//                System.out.println();
//                i=0;
//            }
//        }

        raf.close();
    }

    class Block {

        public int myPos = 0;
        public int leaf = 1;
        public int nkeys = 0; // block 안에 있는 key의 개수
        public int lastVal = -1;
        public ArrayList<Integer> child = new ArrayList<>();
        //        public int parent_pos = -1;
        public ArrayList<ArrayList<Integer>> nodeArray = new ArrayList<>();
//        public ArrayList<Block> child = new ArrayList<>();

        public int parent = -1; // 현재 블록의 parent block

        Block() {
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

        public void setChild(){
            if(!this.child.isEmpty()) this.child.clear();
            for(int i=0; i<this.nkeys; i++){
                this.child.add(this.getValue(i));
            }
            this.child.add(this.lastVal);
        }

        public void refreshParent() throws IOException{
            for(int i=0; i<this.child.size(); i++){
                Block child = readBlock(this.child.get(i));
                child.parent = this.myPos;
                blockBuffer.add(child);
            }

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
    }
    private void writeBlock(Block block) throws IOException {
        buffer.position(0);
        raf.seek((long) blocksize*block.myPos);
        buffer.putInt(block.myPos);
        buffer.putInt(block.leaf);
        buffer.putInt(block.parent);
        for (int i = 0; i < block.nkeys; i++) {
            buffer.putInt(block.getKey(i));
            buffer.putInt(block.getValue(i));
        }
        for (int i = 0; i < maxKeys - block.nkeys; i++) {
            buffer.putInt(-1);
            buffer.putInt(-1);
        }
        buffer.putInt(block.lastVal);
        int wasteCount = (blocksize / 4) - (4 + (2 * maxKeys));
        for (int i = 0; i < wasteCount; i++) {
            buffer.putInt(-99);
        }
        raf.write(buf);
    }
}