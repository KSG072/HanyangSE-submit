package edu.hanyang.submit;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;


import io.github.hyerica_bdml.indexer.BPlusTree;
import scala.util.control.Exception;


public class HanyangSEBPlusTree implements BPlusTree {
    private int blocksize;
    private int nblocks;
    private byte[] buf;
    private ByteBuffer buffer;
    private int maxKeys;
    private int rootindex;
    private RandomAccessFile raf;
    /**
     * B+ tree를 open하는 함수(파일을 열고 준비하는 단계 구현)
     * @param metafile B+ tree의 메타정보 저장(저장할거 없으면 안써도 됨)
     * @param treefile B+ tree의 메인 데이터 저장
     * @param blocksize B+ tree 작업 처리에 이용할 데이터 블록 사이즈
     * @param nblocks B+ tree 작업 처리에 이용할 데이터 블록 개수
     * @throws IOException
     */
    @Override
    public void open(String metafile, String treefile, int blocksize, int nblocks) throws IOException {
        // TODO: your code here...
        this.blocksize = blocksize;
        this.nblocks = nblocks;
        this.buf = new byte[blocksize];
        this.buffer = ByteBuffer.wrap(buf);
        this.maxKeys = (blocksize - 16) / 8;
        this.rootindex = 0;

        raf = new RandomAccessFile(treefile, "rw");
//        mata = new RandomAccessFile(matafile, "rw");
    }

    /**
     * B+ tree에 데이터를 삽입하는 함수
     * @param key
     * @param value
     * @throws IOException
     */
    @Override
    public void insert(int key, int value) throws IOException {
        // TODO: your code here...

        if (raf.length() == 0)
        { // 처음 넣을 때 block 생성
            Block block = new Block(-1,0,1,maxKeys);
            block.addKey(key, value);
            writeBlock(block, 0);
        }
        else {
            Block block = searchNode(key); // insert 할 block

            if (block.nkeys + 1 > maxKeys) { // 넘쳤을 때
                Block newnode = split(block, key, value);
                insertInternal(block, newnode);
            } else {
                // TODO: your code here...
                block.addKey(key, value);
                raf.seek(block.parent);

                writeBlock(block, 1);
            }
        }
    }

    /*
     * 꽉찬 블록을 나누어주는 함수
     *
     * @param block 나눌 블록
     * @param key   추가 key값
     * @param value 추가 val값
     * @return new block    새로 만들어진 블록
     */
    private Block split(Block block, int key, int value) throws IOException {
        /*
        기존 블락 + 새로운거
        => sort
        기존 블락 : 앞에서 절반
        새로운 블락 : 나머지
         */
        // TODO: fill here...

        raf.seek(block.parent);
        int blockPos = raf.readInt(); // 참조 block 의 시작 pointer

        Block newBlock = new Block(-2, 0, 0,maxKeys); // Parent값 의미x

        int mid = (int) Math.ceil((float)maxKeys/2);

        for(int i=0; i<maxKeys;i++)
        {
            // 넣어주고 반으로
            if(key < block.keys[i] )
            {
                for(int j=0; j < maxKeys+1 - mid; j++)
                { // newBlock 에 나머지 keys, vals 복제
                    newBlock.keys[j] = block.keys[mid -1 + j];
                    newBlock.vals[j] = block.vals[mid -1 + j];
                }
                raf.seek(raf.length());
                writeBlock(newBlock, 0); // raf에 new Block create
                //newBlock 완성

                for(int j=0; j < mid - i; j++)
                { // block에 key 보다 큰 기존key들 한칸씩 뒤로 옮김
                    block.keys[i + j + 1] = block.keys[i + j];
                    block.vals[i + j + 1] = block.vals[i + j];
                }
                // key insert
                block.keys[i] = key;
                block.vals[i] = value;
                // block 완성
            }
            else // 그냥 반 짤라서 block 으로 하고 나머지 newBlock
            {
                for(int j=0; j < i-mid ; j++)
                { // newBlock에 key보다 작은 기존 key,val 복제
                    newBlock.keys[j] = block.keys[mid + j];
                    newBlock.vals[j] = block.vals[mid + j];
                }
                // key insert
                newBlock.keys[i - mid] = key;
                newBlock.vals[i - mid] = value;

                for(int j=0; j < maxKeys - i; j++)
                { // 나머지 복제
                    newBlock.keys[i + j - mid] = block.keys[i + j];
                    newBlock.vals[i + j - mid] = block.vals[i + j];
                }
                // newBlock 완성

            }
        }

        /*
        공통적으로 할 일
        nkeys 변경
        다음 block pointer 변경
         */
        block.nkeys = mid;
        newBlock.nkeys = maxKeys + 1 - mid;

        newBlock.vals[maxKeys+1] = block.vals[maxKeys+1]; // newBlock 다음 block pointer =  기존의 다음 block pointer
        block.vals[maxKeys+1] = (int)(raf.length() + 1); // block 마지막 pointer = newBlock pointer

        return newBlock;
    }

    private void writeBlock(Block b, int mode) throws IOException{
        int blockPointer;
        if(b.parent >= 0){
            raf.seek(b.parent);
            blockPointer = raf.readInt();
        }
        else if(mode == 1) {//over write
            raf.seek(rootindex);
            blockPointer = raf.readInt();
            raf.seek(blockPointer);
        }
        else{//new write
            raf.seek(raf.length());
        }

        raf.writeInt(b.parent);
        raf.writeInt(b.type);
        raf.writeInt(b.nkeys);

        for(int i=0; i<b.nkeys; i++) {
            raf.writeInt(b.keys[i]);
        } for(int i=0; i<b.max-b.nkeys; i++) raf.writeInt(-2);

        for(int i=0; i<b.nkeys+1; i++) {
            raf.writeInt(b.vals[i]);
        } for(int i=0; i<b.max-b.nkeys+1; i++) raf.writeInt(-2);

    }

    /**
     * 블록의 parent와 연결시켜주는 함수
     *
     * @param parent 기존 블록의 부모 위치
     * @param pos    새로운 블록의 부모를 가리키는 포인터의 위치
     * @throws IOException
     */
    public void insertInternal(Block block, Block newBlock) throws IOException {
        // TODO: fill here...
        raf.seek(pos);
        int value = (int) raf.getFilePointer(); // 새로운 블록의 위치 저장
        raf.writeInt(parent);
        raf.seek(pos + (4 * 3));    // 맨 처음 key 값 앞으로 커서 옮김
        int key = raf.readInt();        // 맨 처음 key 값 저장

        Block block = readBlock(parent);    // insert 할 block

        if(block.nkeys + 1 > maxKeys){ // 넘쳤을 때
            Block newnode = split(block, key, value);
            insertInternal(block.parent, newnode.parent);
        }
        else{
            // TODO: your code here...
            block.addKey(key, value);
        }
//        raf.writeBlock(parent);
    }


    private Block searchNode(int key) throws IOException
    {
        Block rb = readBlock(rootindex); // root block

        while(rb.type != 0) //leaf 일때 종료
        {
            raf.seek(rb.parent);  // 부모로 감
            int pos = raf.readInt(); // 자식꺼 얻어옴
            raf.seek(pos + 3); // rb의 첫번째 key의 pointer
            for(int i=0; i<rb.nkeys;i++) // key 비교
            {
                if(key < raf.readInt()) // 비교대상보다 작거나
                {
                    raf.seek(pos + 3 + maxKeys + i); // rb의 첫번째 value 의 pointer
                    rb = readBlock(raf.readInt());
                    break;
                }
                if(i == rb.nkeys -1) { // 비교대상이 없으면
                    raf.seek(pos + blocksize/4 - 1); //rb의 마지막 value
                    rb = readBlock(raf.readInt());
                    break;
                }
            }
        }
        return rb;
    }

    private Block readBlock(int index) throws IOException{
        int parent, type, nkeys;

        raf.seek(index);

        parent = raf.readInt();
        type = raf.readInt();
        nkeys = raf.readInt();

        Block b = new Block(parent, type, nkeys, maxKeys);

        for(int i=0; i<nkeys; i++){
            b.keys[i] = raf.readInt();
        }

        raf.seek(raf.getFilePointer() + 4L *(b.max-b.nkeys));

        for(int i=0; i<nkeys+1; i++){
            b.vals[i] = raf.readInt();
        }
        return b;
    }


    /**
     * B+ tree에 있는 데이터를 탐색하는 함수
     * @param key 탐색할 key
     * @return 탐색된 value 값
     * @throws IOException
     */
    @Override
    public int search(int key) throws IOException {
        // TODO: your code here...
        Block rb = readBlock(rootindex);
        return _search(rb, key);
    }

    private int _search(Block b, int key) throws IOException {
        if (b.type == 1) {// non-leaf
            Block child;
            int i;
            for(i=0; i<b.nkeys; i++){
                if (b.keys[i] < key) {
                    child = readBlock(b.vals[i]);
                    return _search(child, key);
                }
            }
            child = readBlock(b.vals[i]);
            return _search(child, key);
        } else { // Leaf
            /* binary search */
            return binaryKeySearch(b.keys, b.vals, key, b.nkeys);
            // if do not exist, return -1
        }
    }

    private int binaryKeySearch(int[] keys, int[] vals, int key, int nkeys) {
        int left = 0, right = nkeys-1;
        int pivot = nkeys/2;

        while(right <= left){
            if(keys[pivot] < key){
                right = pivot + 1;
                pivot = (left + right)/2;
            }
            else if(keys[pivot] > key){
                left = pivot - 1;
                pivot = (left + right)/2;
            }
            else if(keys[pivot] == key){
                return vals[pivot];
            }
        }
        return -1;
    }

    /*
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     * @throws IOException
     */

    @Override
    public void close (){
        try {
            raf.close();
        } catch (IOException e) {
            System.out.println(e);
        }
    }
}