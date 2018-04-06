package cmd;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Key;

public class Generate {
  public static void main(String[] args) throws Exception {

    if(args.length != 5) {
      System.err.println("Usage : Generate <prefix> <numTablets> <numFiles> <numEntries> <dir>");
      System.exit(2);
    }

    String prefix = args[0];
    int numTablets = Integer.parseInt(args[1]);
    int numFiles = Integer.parseInt(args[2]);
    int numEntries = Integer.parseInt(args[3]);
    String dir = args[4];

    long increment = Long.MAX_VALUE / numTablets;

    Random rand = new Random();

    ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 4);

    List<Future<?>> futures = new ArrayList<>();


    int entriesPerFile = numEntries / numFiles;

    for(int i = 0; i < numFiles; i++) {
      int fileNum = i;

      Runnable fgt = () -> {
        long start = rand.nextInt(numTablets) * increment;
        List<String> rows = new ArrayList<>();

        for(int r = 0; r < entriesPerFile; r++) {
          rows.add(String.format("%016x", Math.abs(rand.nextLong()) % increment + start));
        }

        Collections.sort(rows);

        String name = String.format("%s/bf%06d.rf", dir,fileNum);
        try(RFileWriter writer = RFile.newWriter().to(name).build()){

          writer.startDefaultLocalityGroup();
          int c = 0;
          for (String row : rows) {
            writer.append(new Key(row,"f1","q1"), prefix+"-"+(fileNum*entriesPerFile+c));
            c++;
          }

        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      };

      futures.add(es.submit(fgt));
    }

    for (Future<?> future : futures) {
      future.get();
    }

    es.shutdown();
  }
}
