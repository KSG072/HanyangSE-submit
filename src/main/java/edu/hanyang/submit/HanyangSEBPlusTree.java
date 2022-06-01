package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import io.github.hyerica_bdml.indexer.BPlusTree;
import org.apache.lucene.analysis.ar.ArabicAnalyzer;


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
        }
        else{
            rootIndex = meta.readInt();
            for(int i=0; i<raf.length(); i+=blocksize){
                raf.seek(i);
                posInfo.put(raf.readInt(), i);
            }
        }
    }

    @Override
    public void insert(int key, int val) throws IOException {
//        TODO: your code here...

        inserted = true;
        Block block = searchNode(key);
        if(block.nkeys == maxKeys){

            Block newBlock = split(block, key, val);

            if(block.parent==null){ // root 일때
                root = new Block(blockPos, 0, 0, newBlock.myPos);
                rootIndex = root.myPos;
                block.parent = root;
                root.addChild(block);
            }
            block.parent.addChild(newBlock);
            newBlock.parent = block.parent;
            insertInternal(block.parent, newBlock.nodeArray.get(0).get(0), newBlock.myPos);
        }
        else{
            ArrayList<Integer> node = new ArrayList<>();
            node.add(key); node.add(val);
            block.addNode(node);
        }
    }

    private Block searchNode(int key) throws IOException { //leaf에 있는 block을 리턴
        Block block = new Block();
        while (block.leaf == 0) { // leaf일때까지 반복
            for (int i = 0; i < root.nkeys; i++) {
                if(block.nodeArray.get(i).get(0) > key){
                    block = root.child.get(i-1);
                    return block;
                }
                else if(block.nodeArray.get(i).get(0) == key){
                    block = root.child.get(i);
                    return block;
                }
                else{
                    block = root.child.get(i);
                }
            }
        }
        return block;
    }

    /*
    TODO
        완성
        1) leaf 일때는 새로생긴 Block 리턴
        2) non leaf 일때는 중간값인 node 를 Block 으로 만들어서 리턴
     */
    private Block split(Block block, int key, int val) {

        Block newBlock = new Block(blockPos, block.leaf, 0, -1);

        ArrayList<Integer> node = new ArrayList<>();
        node.add(key); node.add(val);
        block.addNode(node);

        int keyNum = block.nkeys;
        int mid = (int)Math.ceil((double)(keyNum)/2);

        if(block.leaf == 1) //leaf node 일 때
        {
            for(int i = mid+1; i < block.nkeys; i++) {
                newBlock.addNode(block.nodeArray.get(i));
            }
            for(int i = keyNum; i > mid ; i--) {
                block.nodeArray.remove(block.nodeArray.get(i));
            }
            newBlock.nkeys = block.nkeys - mid;
            block.nkeys = mid;
            newBlock.val0 = block.val0;
            block.val0 = newBlock.myPos;

            return newBlock;
        }
        else // non-leaf node
        {
            // newBlock key -> max - (mid - 1) - 1 +1
            for(int i=mid+1; i<block.nkeys; i++)
                newBlock.addNode(block.nodeArray.get(i));
            newBlock.nkeys = block.nkeys - mid;
            newBlock.val0 = block.val0;

            // 기존 block key -> mid-1개
            for(int i=0; i < block.nkeys - (mid-1);i++)
                block.nodeArray.remove(mid);
            block.nkeys = mid-1;
            block.val0 = block.nodeArray.get(mid).get(1);

            // midNode 를 가지고 있는 block
            Block childToParent = new Block(blockPos, 0, 1, newBlock.myPos);
            ArrayList<Integer> newRootNode = new ArrayList<>();
            newRootNode.add(block.nodeArray.get(mid).get(0));
            newRootNode.add(block.nodeArray.get(mid).get(1));
            childToParent.addNode(newRootNode);

            return childToParent;
        }


    }

    /*
    TODO
        아직 변경 안함
     */
    private void insertInternal(Block block, int key, int val) throws IOException {
        ArrayList<Integer> jumpedNode = new ArrayList<>();
        int p = (int)Math.ceil((double)block.nkeys/2);
        jumpedNode.add(key);
        jumpedNode.add(val);
        block.addNode(jumpedNode);
        if(block.nkeys > maxKeys){
            Block newBlock = new Block(blockPos, 0, 0, -1);

            for(int i=p+1; i<block.nkeys; i++){
                newBlock.addNode(block.nodeArray.get(i));
                newBlock.addChild(block.child.get(i));
            }
            newBlock.val0 = newBlock.child.get(0).myPos;

            int tmpKey = block.nodeArray.get(p).get(0);

            for(int i=block.nkeys;i>p;i--){
                block.nodeArray.remove(new ArrayList<>(block.nodeArray.get(i)));
                block.child.remove(block.child.get(i));
                block.nkeys--;
            }
            if(block.parent==null){
                Block tmpRoot = new Block(blockPos, 0, 0, block.myPos);
                block.parent = tmpRoot;
                newBlock.parent = tmpRoot;
                tmpRoot.child.add(block);
                tmpRoot.child.add(newBlock);
                root = tmpRoot;

            }
            insertInternal(block.parent, tmpKey, newBlock.myPos);
        }
    }
    @Override
    public int search(int key) throws IOException { // value 값 리턴
        root = readBlock(rootIndex); //시작노드
        return _search(root, key);
    }

    //변수 이름바꾸고 수정할 거 있으면 수정해야함 ++ ㅁ연주네 계속 틀렸던이유가 여기서 파일을 읽어오는데
    //블락사이즈가 실제로 읽을때 좀 다른 듯 이건 나중에 디버깅해볼때 문제 생기면 만져봐야할듯

    /*
    TODO
        readBlock이 모든 block 다 읽는 로직으로 되어있는데
        readBlock(pos) 했을때 1Block만 읽어오도록 변경 -> searchNode에서 key비교를 통해 해당Block을 찾음
     */
    private Block readBlock(int pos) throws IOException {
        Block block = new Block();
        ArrayList<Integer> node = new ArrayList<>();
        raf.seek(posInfo.get(pos));
        block.myPos = raf.readInt();
        block.leaf = raf.readInt();
        block.nkeys = raf.readInt();

        for(int i=0; i<maxKeys*2; i++){
            node.add(0, raf.readInt());
            node.add(1, raf.readInt());
            if(node.get(0) != -1 && node.get(1) != -1) {
                block.addNode(node);
            }
        }
        block.val0 = raf.readInt();

        return block;
    }

    // 변수이름바꾸고 수정할거 있으면 수정해야함
    private int _search(Block block, int key) throws IOException {
        Block child;
        if (block.leaf == 0) { // non-Leaf
            for (int i = 0; i < block.nkeys; i++) {
                if (key > block.nodeArray.get(i).get(0)) { //if no such exist, set c = last non-null pointer in C
                    child = readBlock(block.nodeArray.get(i).get(1)); //변경 i->i+1 / child 맨마지막 child에서 불러오기
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
                if (key==b.nodeArray.get(mid).get(0)) return b.nodeArray.get(mid).get(1);
                else if (key<b.nodeArray.get(mid).get(0)) high = mid-1;
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
        if (inserted) {
            meta.writeInt(rootIndex); // 파일을 open할때 첫번째 int인 rootindex를 읽음으로써 rootindex를 알 수 있다.
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

        for (int i = 0; i < b.nodeArray.size(); i++) {
            int key = b.nodeArray.get(i).get(0);
            int value = b.nodeArray.get(i).get(1);
            raf.writeInt(key); raf.writeInt(value);
        }  for (int i = 0; i < maxKeys-b.nkeys; i++) {
            raf.writeInt(-1); raf.writeInt(-1);
        }
        raf.writeInt(b.val0);

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
        public int val0 = -1;
        //        public int parent_pos = -1;
        public ArrayList<ArrayList<Integer>> nodeArray = new ArrayList<>();
        public ArrayList<Block> child = new ArrayList<>();

        public Block parent = null; // 현재 블록의 parent block

        Block() {
        }
        /*
         * 이 블락 클래스는 두칸의 어레이 리스트(키, 밸류)가 한칸에 들어가는 이중 어레이 리스트이다. 하지만
         * 비플러스트리에서 밸류는 키보다 하나 더 많으므로 일반 int변수 하나를 추가해서 그 변수가 value의 가장
         * 첫 번째 값을 알려준다.
         */
        Block(int myPos, int leaf, int nkeys, int val0) {
            this.myPos = myPos; //블락의 일련번호 , 최초의 블락은 0, 그다음부터 1,2,3식으로
            this.leaf = leaf; //1이면 리프, 0이면 논리프
            this.nkeys = nkeys;
            this.val0 = val0; // value의 첫번째 값. 이유는 아래
            blockPos++;
        }

        /*
         * addNode는 [key, value]형식의 길이가 2인 어레이리스트를 인자값으로 넣어주면
         * key의 값에 맞춰서 블락의 node리스트 내부의 적합한 위치에 맞게 넣어주는 메소드
         */
        public void addNode(ArrayList<Integer> node){
            int newkey = node.get(0);
            ArrayList<ArrayList<Integer>> newNode = new ArrayList<>();

            int keyPos;
            for(keyPos=0; keyPos<this.nkeys; keyPos++)
                if(newkey <= this.nodeArray.get(keyPos).get(0)) break;

            for(int j=0; j<nkeys+1; j++)
            {
                if(keyPos != j){
                    newNode.add(this.nodeArray.get(j));
                }
                else {
                    newNode.add(node);
                    newNode.add(this.nodeArray.get(j));
                }
            }

            this.nodeArray = newNode;
            this.nkeys++;
        }
        /*
         * addChild는 해당 블록객체가 가진 자식인 블록을 저장하는 어레이리스트에 child가
         * 순서대로 정렬될 수 있게 넣어주는 메소드.
         */
        public void addChild(Block child){
            ArrayList<Block> newChild = new ArrayList<>();

            int i;
            for(i=0; i<this.nkeys+1; i++)
                if(child.nodeArray.get(0).get(0) < this.child.get(i).nodeArray.get(0).get(0)) break;

            for(int j=0; j<this.nkeys+1; j++)
            {
                if( i != j)
                    newChild.add(this.child.get(j));
                else{
                    newChild.add(child);
                    newChild.add(this.child.get(j));
                }
            }
            this.child = newChild;
            this.nkeys++;
        }
    }
}