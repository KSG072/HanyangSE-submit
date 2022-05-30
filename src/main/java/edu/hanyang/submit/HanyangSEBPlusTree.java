package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.*;

import io.github.hyerica_bdml.indexer.BPlusTree;
import scala.concurrent.BlockContext;


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
    public int block_numbers = 0;
    public int root_index = 0;
    public int flag = 0;


    @Override
    public void open(String metapath, String treepath, int blocksize, int nblocks) throws IOException {
        // TODO: your code here...
        this.blocksize = blocksize;
        this.nblocks = nblocks;
        this.buf = new byte[blocksize];
        this.buffer = ByteBuffer.wrap(buf);
        this.maxKeys = (blocksize - 16) / 8;

        raf = new RandomAccessFile(treepath, "rw");

        if (raf.length() == 0) { // 빈파일일때는 root만들기
            root = new Block(block_numbers, 1, 0);
//            raf.writeInt(0); // my_pos
//            raf.writeInt(1); // 1이 leaf고 0이 non-leaf
//            raf.writeInt(0); // nkeys
//            raf.writeInt(-1); // parent 없으니까 -
        }
        else {
            root_index = raf.readInt();
        }
//        else {
//            raf.seek(0);
//            for (int i = 0; i < raf.length() / 8; i++) {
//                int key = raf.readInt();
//                int value = raf.readInt();
//                insert(key, value);
//            }
//        }

    }

    @Override
    public void insert(int key, int val) throws IOException {
//        TODO: your code here...
        flag = 1;
        Block block = searchNode(key);

        if (block.nkeys + 1 > maxKeys) { // 해당 block에 insert할 자리가 없으면 block 하나 더 만들어서 쪼갠 뒤 재귀
            block_numbers++;
            Block newnode = split(block, key, val);
            int new_node_key = newnode.data.get(1).get(0);

            if (block.parent == null) {
                block_numbers++;
                root_index = block_numbers;
                Block new_parent = new Block(block_numbers, 0, 0);
                root = new_parent;
                new_parent.child.add(block);
                new_parent.child.add(newnode);
                new_parent.sort2();
                block.parent = new_parent;
                newnode.parent = new_parent;
                new_parent.data.get(0).set(1, new_parent.child.get(0).my_pos);
            }

            else {
                block.parent.child.add(newnode);
                block.parent.sort2();
                newnode.parent = block.parent;
//                newnode.parent.nkeys++;

            }

            insertInternal(block.parent, newnode.my_pos, new_node_key);

        }
        else { // 해당 block에 insert할 빈 공간 존재 그냥 그 블락 안에서 알맞은 위치 찾아서 넣으면 끝
            ArrayList<Integer> key_value = new ArrayList<>();
            key_value.add(key); key_value.add(val);
            block.data.add(key_value);
            block.sort();

            block.nkeys++;
//            raf.seek(block.my_pos + 8);
//            raf.writeInt(block.nkeys++);

        }
    }

    private Block searchNode(int key) throws IOException { //leaf에 있는 block을 리턴
        Block b = root; //
        Block tmp = root;
        while (tmp.leaf == 0) { // leaf일때까지 반복
            for (int i = 1; i < b.nkeys+1; i++) {
                if (key > b.data.get(i).get(0)) { //if no such exist, set c = last non-null pointer in C
                    tmp = b.child.get(i); //변경 i->i+1 / child 맨마지막 child에서 불러오기
                }
                else { //key <= b.keys[i] 되는 값 찾은 경우
                    if (b.data.get(i).get(0) == key) {
                        tmp = b.child.get(i); //i+1불러오는거
                    }
                    else {
                        tmp = b.child.get(i-1); //i 불러오는거
                    }
                    break;
                }
            }
        }
        return tmp;
    }

    private Block split(Block block, int key, int val) {
        block.nkeys += 1;
        int block_key_num = block.nkeys;
        int m = (int)Math.ceil((double)block.nkeys/2); // 한 block의 key의 갯수를 반으로 쪼개서 올림한 수

        Block newblock = new Block(block_numbers, 1, 0);

        ArrayList<Integer> newdata = new ArrayList(); // 새로운 데이터 arraylist 생성

        newdata.add(key);
        newdata.add(val);

        block.data.add(newdata); // 새로운 데이터 추가

        block.sort(); // 정렬 -> 결과 보고 수정해야할지도..?

        for(int i = m+1; i < block_key_num+1; i++) { // new block에 절반 이상 만큼 넣기
            newblock.data.add(new ArrayList<>(block.data.get(i)));
            newblock.nkeys++; //
        }

        for(int i = block_key_num; i > m ; i--) { // new block에서 m부터 그 뒤에 제거하기
            block.data.remove(new ArrayList<>(block.data.get(i)));
            block.nkeys--;
        }


        return newblock;
    }

    private void insertInternal(Block parent, int my_pos, int new_node_key) throws IOException {
        int par_size = parent.nkeys;
        int p = (int)Math.ceil((double)parent.nkeys/2) + 1; // 중간 위치

        parent.data.get(0).set(1, parent.child.get(0).my_pos);

        ArrayList<Integer> newdata = new ArrayList(); // 새로운 데이터 arraylist 생성

        newdata.add(new_node_key);
        newdata.add(my_pos);
        parent.data.add(newdata);
        parent.sort();
        parent.nkeys += 1;

        if (par_size + 1 > maxKeys){ // 꽉찼을 때
            block_numbers++;
            root_index = block_numbers;
            Block newpar = new Block(block_numbers, 0, 0);

            for(int i = p; i < parent.nkeys + 1; i ++){ // 새로 쪼갠 parent에 값 추가
                newpar.data.add(new ArrayList<>(parent.data.get(i)));
                newpar.nkeys++;
                newpar.child.add(parent.child.get(i));
            }
            int key = parent.data.get(p).get(0);
            int pointer = newpar.my_pos;

            newpar.data.get(0).set(1, newpar.child.get(0).my_pos);
            for(int i = parent.nkeys; i >= p ; i--){ // 기존 parent 값 제거
                parent.data.remove(new ArrayList<>(parent.data.get(i)));
                parent.nkeys--;
                parent.child.remove(parent.child.get(i));
            }

            block_numbers++; root_index = block_numbers;
            Block par_parent = new Block(block_numbers, 0, 0);
            root = par_parent;

            parent.parent = par_parent; newpar.parent = par_parent;
            par_parent.child.add(parent); par_parent.child.add(newpar);

            insertInternal(parent.parent, pointer, key); // 가운데 값 parent로 올림
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
        if (flag == 1) {
            raf.writeInt(root_index);
            traverse(root);
        }
        flag = 0;
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
            raf.writeInt(-2); raf.writeInt(-2);
        }

        if (b.leaf == 0) {
            for (int i = 0; i < b.child.size(); i++) {
                traverse(b.child.get(i));
            }
        }
    }

    class Block {

        public int my_pos = -1;
        public int leaf = 1;
        public int nkeys = 0; // block 안에 있는 key의 개수
        //        public int parent_pos = -1;
        public ArrayList<ArrayList<Integer>> data = new ArrayList<>();
        public ArrayList<Block> child = new ArrayList<>();

        public Block parent = null; // 현재 블록의 parent block




        public void sort() {
            Comparator<ArrayList<Integer>> comparator = new Comparator<ArrayList<Integer>>() {
                public int compare(ArrayList<Integer> o1, ArrayList<Integer> o2) {
                    return o1.get(0).compareTo(o2.get(0));
                }
            };
            Collections.sort(data, comparator);
        }

        public void sort2() {
            Comparator<Block> comparator = new Comparator<Block>() {
                public int compare(Block o1, Block o2) {
                    return o1.data.get(1).get(0).compareTo(o2.data.get(1).get(0));
                }
            };
            Collections.sort(child, comparator);
        }

        Block() {
            ArrayList<Integer> pointer = new ArrayList<>();
//            pointer.add(-1); pointer.add(-1);
//            this.data.add(pointer);
        }

        Block(int my_pos, int leaf, int nkeys) {
            this.my_pos = my_pos;
            this.leaf = leaf;
            this.nkeys = nkeys;
            ArrayList<Integer> pointer = new ArrayList<>();
//            pointer.add(-1); pointer.add(-1);
//            this.data.add(pointer);
        }
    }
}