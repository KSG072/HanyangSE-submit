package edu.hanyang.submit;

import java.io.*;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.*;

import io.github.hyerica_bdml.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;
import scala.Mutable;


public class HanyangSEExternalSort implements ExternalSort {

    /**
     * External sorting
     *
     * @param infile    정렬되지 않은 데이터가 있는 파일
     * @param outfile   정렬된 데이터가 쓰일 파일
     * @param tmpdir    임시파일을 위한 디렉토리
     * @param blocksize 허용된 메모리 블록 하나의 크기z
     * @param nblocks   허용된 메모리 블록 개수
     */

    /*
     * 테스함수에서는 infile에 data의 경로
     * outfile에 결과파일의 이름과 저장할 경로
     * tmpdir에는 "tmp/"
     * blocksize에는 한 블럭의 메모리크기(1024*8) => buf size : 1024*8
     * nblocks에는 (1000) => 999 - way merge
     * 14649개 block 있음
     */
    @Override
    public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
        // TODO: your code here...
        //1)initial phase
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>(blocksize);


        int BLOCKNUM = 15;
        byte[] buffer = new byte[blocksize * BLOCKNUM];
        int termId, docId, pos;
        int runNum = 0;

        // infile 모두 read
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(infile)));
            while ((inputStream.read(buffer, blocksize * runNum * BLOCKNUM, blocksize * BLOCKNUM)) != -1) {
//                inputStream.read(buffer, (buffer.length * runNum) + 1, buffer.length);
                runNum += 1;

                //1run (=15 block) 단위 read
                for (int i = 0; i < buffer.length; i += 3) { // 3개씩 나눠서 dataArr 에 tuple 로 넣음
                    termId = buffer[i];
                    docId = buffer[i + 1];
                    pos = buffer[i + 2];
                    dataArr.add(new MutableTriple<>(termId, docId, pos));
                }

                // dataArr sort
                Collections.sort(dataArr);

                System.out.print("run0" + dataArr);
                System.out.print(dataArr.size());

                // dataArr => tmp run0.[]
                DataOutputStream initTmp = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir + File.separator + "run0." + runNum + ".data")));

                int dataNum = 0;
                while (dataArr.size() > 0) {
                    dataNum += 1;
                    MutableTriple<Integer, Integer, Integer> data = dataArr.remove(dataNum);
                    initTmp.write(data.getLeft());
                    initTmp.write(data.getMiddle());
                    initTmp.write(data.getRight());
                }
                // buffer 초기화

                initTmp.flush();
                initTmp.close();
                inputStream.close();
            }
        } catch (Exception e) {
            System.out.println("no file to read\n");
        }
    }

    private void _externalMergeSort(String tmpDir, String outputFile,int step, int nblocks, int blocksize) throws IOException {
        File[] fileArr = (new File(tmpDir + File.separator + "run" + String.valueOf(step - 1))).listFiles();
        ArrayList<DataInputStream> files = new ArrayList<>(nblocks);
        int cnt = 0;
        if (fileArr.length <= nblocks) {
            for (File f : fileArr) {
                FileInputStream fileStream = new FileInputStream(f);
                BufferedInputStream buffStream = new BufferedInputStream(fileStream);
                DataInputStream DataStream = new DataInputStream(buffStream);
                files.add(DataStream);
            }
            n_way_merge(files, outputFile, blocksize);
        } else {
            for (File f : fileArr) {
                FileInputStream fileStream = new FileInputStream(f);
                BufferedInputStream buffStream = new BufferedInputStream(fileStream);
                DataInputStream DataStream = new DataInputStream(buffStream);
                files.add(DataStream);
                cnt++;
                if (cnt == nblocks) {
                    n_way_merge(files, outputFile, blocksize);
                    files.clear();
                    cnt = 0;
                }
                _externalMergeSort(tmpDir, outputFile, step + 1, nblocks, blocksize);
            }
        }
    }

    public void n_way_merge(List<DataInputStream> files, String outputFile, int blocksize) throws IOException {
        PriorityQueue<DataManager> queue = new PriorityQueue<DataManager>(files.size(), new Comparator<DataManager>() {
            public int compare(DataManager o1, DataManager o2) {
                return o1.tuple.compareTo(o2.tuple);
            }
        });
        for(DataInputStream f : files){
            queue.offer(new DataManager(f));
        }
        while(queue.size()!=0){
            DataManager dm = queue.poll();
            MutableTriple<Integer, Integer, Integer> tmp = new MutableTriple<>();
            dm.getTuple(tmp);

        }
    }
}
