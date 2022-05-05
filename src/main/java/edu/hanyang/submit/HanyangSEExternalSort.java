package edu.hanyang.submit;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import io.github.hyerica_bdml.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;
import org.apache.lucene.util.PriorityQueue;


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


    }

//    private void _externalMergeSort(String tmpDir, String outputFile, int step, int nblocks, int blocksize) throws IOException {
//        File[] fileArr = (new File(tmpDir + File.separator + "run" + String.valueOf(step-1))).listFiles();
//        ArrayList<BufferedInputStream> files = new ArrayList<>(nblocks);
//        int cnt = 0;
//        if (fileArr.length <= nblocks - 1) {
//            for (File f : fileArr) {
//                FileInputStream fileStream = new FileInputStream(f);
//                BufferedInputStream buffStream = new BufferedInputStream(fileStream);
//                files.add(buffStream);
//            }
//            n_way_merge(files, outputFile);
//        } else {
//            for (File f : fileArr) {
//                FileInputStream fileStream = new FileInputStream(f);
//                BufferedInputStream buffStream = new BufferedInputStream(fileStream);
//                files.add(buffStream);
//                cnt++;
//                if (cnt == nblocks - 1) {
//                    n_way_merge(files, outputFile);
//                    files.clear(); cnt = 0;
//                }
//                _externalMergeSort(tmpDir, outputFile, step + 1, nblocks, blocksize);
//            }
//        }
//    }
//
//    public void n_way_merge(ArrayList<BufferedInputStream> files, String outputFile) throws IOException {
//        PriorityQueue<DataManager> queue = new PriorityQueue<>
//                (files.size(), new Comparator<DataManager>() {
//                    public int compare(DataManager o1, DataManager o2) {
//                        return o1.tuple.compareTo(o2.tuple);
//                    }
//                });
//        while (queue.size() != 0) {
//            DataManager dm = queue.poll();
//            MutableTriple<Integer, Integer, Integer> tmp = dm.getTuple();
//        }
//    }
}
