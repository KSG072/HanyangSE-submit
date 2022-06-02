package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
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
            while (raf.getFilePointer() != raf.length()) {
                System.out.print(raf.readInt());
                System.out.print(" ");
                if (raf.getFilePointer() % blocksize == 0) System.out.println();
            }
            for (int i = 0; i < raf.length(); i += blocksize) {
                raf.seek(i);
                posInfo.put(raf.readInt(), i);
            }
        }
    }

    private Block searchNode(int key) throws IOException { //leaf에 있는 block을 리턴
        Block block = root;
        int changed = 0;
        while (block.leaf == 0) {
            changed = 0;
            for (int i = 0; i < block.nkeys; i++) {
                if (key <= block.getKey(i)) {
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
        Block block = searchNode(key);
        insertInternal(block, key, val);
    }

    /*
    TODO
        block은 root가 될 수 없음
     */
    private void insertInternal(Block block, int key, int val) throws IOException {
        ArrayList<Integer> newNode = new ArrayList<>();
        newNode.add(key);
        newNode.add(val);
        block.addNode(newNode);

        if (block.nkeys > maxKeys) {
            Block newBlock;
            int mid = (int) Math.ceil((double) maxKeys / 2);
            int midKey = block.getKey(mid);
            if (block.leaf == 1) {
                newBlock = new Block(block.leaf);
                for (int i = block.nkeys-1; i >= mid; i--) {
                    newBlock.addNode(block.getNode(i));
                    block.nodeArray.remove(block.getNode(i));
                    block.nkeys--;
                }
            } else { // TODO : 여기 else 칸만 고쳐보면 될듯?
                newBlock = new Block(block.leaf);
                newBlock.lastVal = block.lastVal;
                block.lastVal = block.getValue(mid + 1);
                ArrayList<Integer> firstnode = new ArrayList<>();
                firstnode.add(block.getKey(mid + 1));
                firstnode.add(block.getValue(mid));
                newBlock.addNode(firstnode);
                for (int i = block.nkeys-1; i >= mid+2; i--) {
                    newBlock.addNode(block.getNode(i));
                    block.nodeArray.remove(block.getNode(i));
                    block.nkeys--;
                }
                block.nodeArray.remove(block.nodeArray.size()-1);
                block.nkeys--;
                block.nodeArray.remove(block.nodeArray.size()-1);
                block.nkeys--;
                if(block.child.size() > 0){
                    for (int i = block.child.size()-1; i > mid; i--){
                        newBlock.addChild(block.child.get(i));
                        block.child.remove(block.child.get(i));
                    }
                }
            }
            if (block.parent != null) {
                block.parent.addChild(newBlock);
                newBlock.parent = block.parent;
                insertInternal(block.parent, midKey, newBlock.myPos);
                block.parent.lastVal = block.parent.child.get(block.parent.child.size()-1).myPos;
            } else {
                Block newRoot = new Block(0);
                block.parent = newRoot;
                newBlock.parent = newRoot;
                rootIndex = newRoot.myPos;
                ArrayList<Integer> newRootSibling = new ArrayList<>();
                newRootSibling.add(midKey);
                newRootSibling.add(block.myPos);
                newRoot.addNode(newRootSibling);
                newRoot.lastVal = newBlock.myPos;
                newRoot.addChild(block);
                newRoot.addChild(newBlock);
                this.root = newRoot;
            }
            if (block.leaf == 1) {
                newBlock.lastVal = block.lastVal;
                block.lastVal = newBlock.myPos;
            }
        }
    }

    @Override
    public int search(int key) throws IOException { // value 값 리턴
        root = readBlock(rootIndex); //시작노드
        return _search(root, key);
    }

    private Block readBlock(int pos) throws IOException {
        Block block = new Block();
        ArrayList<Integer> node = new ArrayList<>();
        int key, val;
        System.out.println(posInfo.toString() + pos);
        raf.seek(posInfo.get(pos));
        block.myPos = raf.readInt();
        block.leaf = raf.readInt();
        raf.readInt();

        for (int i = 0; i < block.nkeys; i++) {
            key = raf.readInt();
            val = raf.readInt();
            node.add(0, key);
            node.add(1, val);
            if (node.get(0) != -1 && node.get(1) != -1) {
                block.addNode(node);
            }
        }
        block.lastVal = raf.readInt();

        return block;
    }

    // 변수이름바꾸고 수정할거 있으면 수정해야함
    private int _search(Block block, int key) throws IOException {
        Block child;
        if (block.leaf == 0) { // non-Leaf
            for (int i = 0; i < block.nkeys; i++) {
                if (key > block.nodeArray.get(i).get(0)) { //if no such exist, set c = last non-null pointer in C
                    child = readBlock(block.nodeArray.get(i).get(1)); //변경 i->i+1 / child 맨마지막 child에서 불러오기
                    return _search(child, key);
                }
            }
            return _search(readBlock(block.lastVal), key);
        } else { // Leaf
            int left = 0, right = block.nkeys - 1, mid;
            while (left <= right) {
                mid = (left + right) / 2;
                if (key == block.nodeArray.get(mid).get(0)) {
                    return block.nodeArray.get(mid).get(1);
                } else if (key < block.nodeArray.get(mid).get(0)) {
                    right = mid - 1;
                } else {
                    left = mid + 1;
                }
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
            int newkey = node.get(0);
            int inserted = 0;
            ArrayList<ArrayList<Integer>> newNode = new ArrayList<>();

            if (this.nkeys == 0) {
                newNode.add(node);
            } else {
                for (int i = 0; i < this.nkeys; i++) {
                    if (node.get(0) < getKey(i) && inserted == 0) {
                        inserted = 1;
                        newNode.add(node);
                        newNode.add(this.getNode(i));
                    } else newNode.add(this.getNode(i));
                }
                if(inserted == 0){
                    newNode.add(node);
                }
            }
            this.nkeys++;
            this.nodeArray = newNode;
        }

        /*
         * addChild는 해당 블록객체가 가진 자식인 블록을 저장하는 어레이리스트에 child가
         * 순서대로 정렬될 수 있게 넣어주는 메소드.
         */
        public void addChild(Block child) {
            ArrayList<Block> newChild = new ArrayList<>();
            int inserted = 0;
            if (this.child.size() == 0) {
                newChild.add(child);
            } else {
                for (int i = 0; i < this.child.size(); i++) {
                    if (child.getKey(0) < this.child.get(i).getKey(0)) {
                        inserted = 1;
                        newChild.add(child);
                        newChild.add(this.child.get(i));
                    } else {
                        newChild.add(this.child.get(i));
                    }
                }
                if (inserted == 0) newChild.add(child);
            }
            this.child = newChild;
        }
    }
}