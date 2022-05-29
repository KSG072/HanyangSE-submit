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
            Block block = new Block(maxKeys);
            block.nkeys = 1;
            block.type = 0;
            block.parent = 0;
            block.vals[0] = value;
            block.keys[0] = key;

            buffer.putInt(0,block.parent);
          
            buffer.putInt(1,block.type);
            buffer.putInt(2,block.nkeys);
            buffer.putInt(3,key);
            buffer.putInt(4 + maxKeys, value);

            raf.write(buf);

        }

        Block block = searchNode(key);

        if(block.nkeys + 1 > maxKeys){
            Block newnode = split(block, key, value);
            insertInternal(readBlock(block.parent), newnode.parent);
        }
        else{
            // TODO: your code here...
            block.nkeys++;
            raf.seek(block.parent + 12 + (8L * block.nkeys));
            raf.write(key);
            raf.write(value);

        }
    }

    public Block searchNode(int key) throws IOException
    {
        Block rb = readBlock(rootindex); // root block

        buf = new byte[4];

        while(rb.type != 0) //leaf 일때 종료
        {
            raf.seek(rb.parent);  // 부모로 감
            int pos = raf.read(buf); // 자식꺼 얻어옴
            raf.seek(pos + 12); // rb의 첫번째 key의 pointer
            for(int i=0; i<rb.nkeys;i++) // key 비교
            {
                if(key < raf.read(buf)) // 비교대상보다 작거나
                {
                    raf.seek(pos + 12 + maxKeys* 4L + 4L*i); // rb의 첫번째 value 의 pointer
                    rb = readBlock(raf.read(buf));
                    break;
                }
                if(i == rb.nkeys -1) { // 비교대상이 없으면
                    raf.seek(pos + blocksize - 4); //rb의 마지막 value
                    rb = readBlock(raf.read(buf));
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

        raf.seek(raf.getFilePointer() + (maxKeys-nkeys));

        for(int i=0; i<nkeys+1; i++){
            b.vals[i] = raf.readInt();
        }

        return b;
    }

    public Block split(Block block, int key, int value){
        /*
        기존 블락 + 새로운거
        => sort
        기존 블락 : 앞에서 절반
        새로운 블락 : 나머지
         */
        int[] tmp = new int[maxKeys];
        System.arraycopy(block.keys, 0, tmp, 0, maxKeys);
        tmp[maxKeys] = key;
        Arrays.sort(tmp);
        return null;
    }

    public void insertInternal(Block parent, int pos){

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
            // TODO: your code here...
            if (block.keys[i] < key) {
                child = readBlock(b.vals[i]);
            }
            //TODO: your code here...
        } else { // Leaf
            /* binary or linear search */
            // if exists
            return val;
            // else
            return -1;
        }
    }

    /*
     * B+ tree를 닫는 함수, 열린 파일을 닫고 저장하는 단계
     * @throws IOException
     */

    @Override
    public void close () throws IOException {
        // TODO: your code here...
        try {
            raf.close();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}