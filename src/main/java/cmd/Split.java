package cmd;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.admin.NewTableConfiguration;
import org.apache.hadoop.io.Text;

import acbase.CmdUtil;

public class Split {
  public static void main(String[] args) throws Exception {

    if(args.length != 2) {
      System.err.println("Usage : Split <splits> [online|offline]");
      System.exit(2);
    }

    int numTablets = Integer.parseInt(args[0]);

    SortedSet<Text> splits = new TreeSet<>();

    long increment = Long.MAX_VALUE / numTablets;

    for(int i = 1; i< numTablets; i++) {
      String split = String.format("%016x", i * increment);
      splits.add(new Text(split));
    }

    Connector conn = CmdUtil.getConnector();

    NewTableConfiguration ntc = new NewTableConfiguration();
    if(args[1].equals("offline")) {
      ntc.createOffline();
    } else if (!args[1].equals("online")) {
      throw new IllegalArgumentException("Expected online or offline, not : "+args[1]);
    }
    ntc.withSplits(splits);

    long t1 = System.currentTimeMillis();
    conn.tableOperations().create("bulky", ntc);
    long t2 = System.currentTimeMillis();
    System.out.println("Create time:"+(t2 - t1));

  }
}
