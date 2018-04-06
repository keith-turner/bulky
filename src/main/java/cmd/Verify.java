package cmd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

import acbase.CmdUtil;

class PrefixInfo {

  TreeSet<Long> seen = new TreeSet<>();
  int dupes = 0;

  void add(long v) {
    if(!seen.add(v)) {
      dupes++;
    }
  }

  @Override
  public String toString() {

    int missing = 0;

    Iterator<Long> iter1 = seen.iterator();
    Iterator<Long> iter2 = seen.iterator();

    if(iter2.hasNext()) {
      iter2.next();
    }

    while(iter2.hasNext()) {
      Long prev = iter1.next();
      Long curr = iter2.next();
      missing += curr-prev-1;
    }

    return String.format("min:%d max:%d dupes:%d missing:%d", seen.first(), seen.last(), dupes, missing);
  }

  static PrefixInfo merge(PrefixInfo pi1, PrefixInfo pi2) {
    PrefixInfo ret = new PrefixInfo();
    ret.dupes = pi1.dupes + pi2.dupes;
    ret.seen = new TreeSet<>(pi1.seen);
    pi2.seen.forEach(l -> {
      if(!ret.seen.add(l)) {
        ret.dupes++;
      }
    });
    return ret;
  }
}

public class Verify {
  public static void main(String[] args) throws Exception {

    if(args.length != 0) {
      System.err.println("Usage : Verify");
      System.exit(2);
    }

    Connector conn = CmdUtil.getConnector();


    List<Range> ranges = getTabletRanges(conn);

    ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    List<Future<Map<String, PrefixInfo>>> futures = new ArrayList<>();

    for (Range range : ranges) {
      futures.add(es.submit(() -> examineValues(conn, range)));
    }


    Map<String, PrefixInfo> prefixes = new HashMap<>();

    for (Future<Map<String, PrefixInfo>> future : futures) {
      Map<String, PrefixInfo> rangeData = future.get();

      rangeData.forEach((p,pi) -> {
        prefixes.merge(p, pi, PrefixInfo::merge);
      });
    }

    es.shutdown();

    prefixes.forEach((p,pi) -> System.out.printf("%s %s\n", p, pi));
  }

  private static List<Range> getTabletRanges(Connector conn)
      throws TableNotFoundException, AccumuloSecurityException, AccumuloException {


    List<Text> splits = new ArrayList<>(conn.tableOperations().listSplits("bulky"));

    if(splits.size() == 0) {
      return Collections.singletonList(new Range());
    }

    Collections.sort(splits);

    List<Range> ranges = new ArrayList<>();


    ranges.add(new Range(null, true, splits.get(0), true));

    for(int i = 1; i < splits.size(); i++) {
      ranges.add(new Range(splits.get(i-1), false, splits.get(i), true));

    }

    ranges.add(new Range(splits.get(splits.size()-1), false, null, true));
    return ranges;
  }

  private static  Map<String, PrefixInfo> examineValues(Connector conn, Range range) {
    try(Scanner scanner = conn.createScanner("bulky", Authorizations.EMPTY)){
      scanner.setRange(range);
      Map<String, PrefixInfo> prefixes = new HashMap<>();

      for (Entry<Key,Value> entry : scanner) {
        String[] val = entry.getValue().toString().split("-");
        prefixes.computeIfAbsent(val[0], k -> new PrefixInfo()).add(Long.parseLong(val[1]));
      }

      return prefixes;
    } catch (TableNotFoundException e) {
      throw new RuntimeException(e);
    }
  }
}
