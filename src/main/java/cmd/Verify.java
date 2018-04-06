package cmd;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;

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

}

public class Verify {
  public static void main(String[] args) throws Exception {

    if(args.length != 0) {
      System.err.println("Usage : Verify");
      System.exit(2);
    }

    Connector conn = CmdUtil.getConnector();

    Scanner scanner = conn.createScanner("bulky", Authorizations.EMPTY);

    Map<String, PrefixInfo> prefixes = new HashMap<>();

    for (Entry<Key,Value> entry : scanner) {
      String[] val = entry.getValue().toString().split("-");
      prefixes.computeIfAbsent(val[0], k -> new PrefixInfo()).add(Long.parseLong(val[1]));
    }

    prefixes.forEach((p,pi) -> System.out.printf("%s %s\n", p, pi));

  }
}
