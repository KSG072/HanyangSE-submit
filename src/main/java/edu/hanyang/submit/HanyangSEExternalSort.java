package edu.hanyang.submit;

import java.io.*;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.*;

import io.github.hyerica_bdml.indexer.ExternalSort;
import org.apache.commons.lang3.tuple.MutableTriple;


public class HanyangSEExternalSort implements ExternalSort {
    /**
     * External sorting
     * @param infile    Input file
     * @param outfile   Output file
     * @param tmpdir    Temporary directory to be used for writing intermediate runs on
     * @param blocksize Available blocksize in the main memory of the current system
     * @param nblocks   Available block numbers in the main memory of the current system
     * @throws IOException  Exception while performing external sort
     */
    @Override
    public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
        // TODO: your code here...

        //1)initial phase
        ArrayList<MutableTriple<Integer, Integer, Integer>> dataArr = new ArrayList<>();

        int buffer = (blocksize*9)/4;
        int termId, docId, pos;
        int runNum = 0;

        // infile 모두 read
        try {
            DataInputStream inputStream = new DataInputStream(new BufferedInputStream(new FileInputStream(infile)));
            try {
                while (true) {
                    //1run (=15 block) 단위 read
                    for (int i = 0; i < buffer; i += 3) { // 3개씩 나눠서 dataArr 에 tuple 로 넣음
                        termId = inputStream.readInt();
                        docId = inputStream.readInt();
                        pos = inputStream.readInt();
                        dataArr.add(new MutableTriple<>(termId, docId, pos));
                    }
                    // dataArr sort
                    Collections.sort(dataArr);
                    // dataArr => tmp run0.[]
                    DataOutputStream outputStream = makeOutputStream(tmpdir, String.valueOf(0), String.valueOf(runNum));
                    writeFile(outputStream, dataArr);

                    outputStream.close();
                    dataArr.clear();
                    runNum += 1;
                }
            }
            catch(EOFException e){
                Collections.sort(dataArr);
                // dataArr => tmp run0.[]
                DataOutputStream outputStream = makeOutputStream(tmpdir, String.valueOf(0), String.valueOf(runNum));
                writeFile(outputStream, dataArr);

                outputStream.close();
                dataArr.clear();

                System.out.println("initialization done.");
                inputStream.close();
                //2) n-way merge
                _externalMergeSort(tmpdir, outfile, 1, nblocks, blocksize);
            }
        }
        catch(Exception e){
            System.out.println("no read file.");
        }
    }


    private void _externalMergeSort(String tmpDir, String outputFile, int step, int nblocks, int blocksize) throws IOException {
        File[] fileArr = (new File(tmpDir + File.separator + (step - 1) + File.separator)).listFiles();
        ArrayList<DataInputStream> files = new ArrayList<>(nblocks);
        int runNum = 0;
        int cnt = 0;
        if (fileArr.length <= nblocks-1) {
            for (File f : fileArr) {
                files.add(new DataInputStream(new BufferedInputStream(new FileInputStream(f))));

            }
            n_way_merge(files, outputFile, "", "", blocksize);
        } else {
            for (File f : fileArr) {
                files.add( new DataInputStream(new BufferedInputStream(new FileInputStream(f))));
                cnt++;
                if (cnt == nblocks-1) {
                    n_way_merge(files, tmpDir, String.valueOf(step), String.valueOf(runNum), blocksize);
                    runNum++;
                    files.clear();
                    cnt = 0;
                }
            }
            if(!files.isEmpty()) n_way_merge(files, tmpDir, String.valueOf(step), String.valueOf(runNum), blocksize);
            for(DataInputStream inputStrm : files){
                inputStrm.close();
                inputStrm = null;
            }
            _externalMergeSort(tmpDir, outputFile, step+1, nblocks, blocksize);
        }
    }

    public void n_way_merge(List<DataInputStream> files, String outputFile, String step, String runNum, int blocksize) throws IOException {
        ArrayList<MutableTriple<Integer, Integer, Integer>> outputbuf = new ArrayList<>();
        PriorityQueue<DataManager> queue = new PriorityQueue<>(files.size(), new Comparator<DataManager>() {
            public int compare(DataManager o1, DataManager o2) {
                return o1.tuple.compareTo(o2.tuple);
            }
        });
        DataOutputStream outputStream = makeOutputStream(outputFile, step, runNum);

        MutableTriple<Integer, Integer, Integer> data = new MutableTriple<>();

        for(DataInputStream f : files){
            queue.offer(new DataManager(f));
        }
        while(queue.size()!=0){
            DataManager dm = queue.poll();
            dm.getTuple(data);
            outputbuf.add(data);
            if(!dm.isEOF) {
                queue.add(dm);
            }
            else{
                dm.closeStream();
            }
            if (outputbuf.size() >= blocksize / 12) {
                writeFile(outputStream, outputbuf);
            }
        }
        writeFile(outputStream, outputbuf);
        outputStream.close();
    }

    public static DataOutputStream makeOutputStream(String path, String step, String runNum){
        try {
            String fullpath = path + File.separator + step + File.separator + runNum + ".data";
            if(path.equals("data/output_10000000.data")) fullpath = "data" + File.separator + "output_10000000.data";

            String pathExceptFileName = fullpath.substring(0, fullpath.lastIndexOf(File.separator));
            File f = new File(pathExceptFileName);
            if (!f.exists()) f.mkdir();

            return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(fullpath)));
        }
        catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }

    public static void writeFile(DataOutputStream outputStream, ArrayList<MutableTriple<Integer, Integer, Integer>> tupArr){
        try {
            while(tupArr.size() > 0){
                MutableTriple<Integer, Integer, Integer> data = tupArr.remove(0);
                outputStream.writeInt(data.getLeft());
                outputStream.writeInt(data.getMiddle());
                outputStream.writeInt(data.getRight());
            }
            outputStream.flush();
        }
        catch (IOException e){
            e.printStackTrace();
        }
    }
}