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
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>();

        int R = 15;
        byte[] buffer = new byte[blocksize * R];
        int termId, docId, pos;
        int runNum = 0;

        // infile 모두 read
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(infile)));
            int initdata = -1;
            while ((initdata = inputStream.read(buffer))!= -1) {
//                inputStream.read(buffer, (buffer.length * runNum) + 1, buffer.length);


                //1run (=15 block) 단위 read
                for (int i = 0; i < buffer.length; i += 3) { // 3개씩 나눠서 dataArr 에 tuple 로 넣음
                    termId = buffer[i];
                    docId = buffer[i + 1];
                    pos = buffer[i + 2];
                    dataArr.add(new MutableTriple<>(termId, docId, pos));
                }

                // dataArr sort
                Collections.sort(dataArr);

                System.out.print(" write run0." + runNum+ "\n");
                // dataArr => tmp run0.[]
                makeFile(tmpdir, "run", dataArr,0, runNum);
                runNum += 1;
            }
            inputStream.close();
        } catch (Exception e) {
            System.out.println("no file to read\n");
        }


        //2) n-way merge
        _externalMergeSort(tmpdir, outfile, 1, nblocks, blocksize);
    }

    private void _externalMergeSort(String tmpDir, String outputFile,int step, int nblocks, int blocksize) throws IOException {
        File[] fileArr = (new File(tmpDir + File.separator + String.valueOf(step - 1))).listFiles();
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
                    n_way_merge(files, tmpDir, blocksize);
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

    public static void makeFile(String dir, String outPutFileName, ArrayList<MutableTriple<Integer, Integer, Integer>> tupArr, int step, int runNum) {
        try{
            String fullpath;
            if(step >= 0){
                fullpath = dir + File.separator + step + File.separator + outPutFileName + runNum + ".data";
                String pathExceptFileName = fullpath.substring(0, fullpath.lastIndexOf(File.separator));
                File f = new File(pathExceptFileName);
                if(!f.exists()) f.mkdir();
            }
            else {
                fullpath = outPutFileName;
            }

            DataOutputStream outputStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fullpath)));
            while(tupArr.size() > 0){
                MutableTriple<Integer, Integer, Integer> data = tupArr.remove(0);
                outputStream.write(data.getLeft());
                outputStream.write(data.getMiddle());
                outputStream.write(data.getRight());
            }
            outputStream.flush();
            outputStream.close();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
