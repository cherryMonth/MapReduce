package org.apache.spark;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

public class GenAge {

    public static final int CAPACITY = 10000000;
    public static final String PATH = "ages.txt";
    public static List<Integer> ages = new LinkedList<Integer>();

    public static void main(String[] args) throws Exception {
        Random random = new Random();

        for (int i = 0; i < CAPACITY; i++) {
            ages.add(Math.abs(random.nextInt(100) + 1));
        }

        WriteToFile();
    }

    public static void WriteToFile() throws Exception {
        FileOutputStream fos = new FileOutputStream(new File(PATH));
        OutputStreamWriter osw = new OutputStreamWriter(fos);

        Iterator<Integer> it = ages.iterator();

        int index = 0;
        while (it.hasNext()) {
            osw.write(index++ + " " + it.next() +"\n");
        }

        osw.flush();
        osw.close();
    }
}
