package cmd;

import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.Connector;
import org.apache.hadoop.io.Text;

import acbase.CmdUtil;

public class Split {
  public static void main(String[] args) throws Exception {

    if(args.length != 1) {
      System.err.println("Usage : Split <splits>");
      System.exit(2);
    }

    Connector conn = CmdUtil.getConnector();

    conn.tableOperations().create("bulky");

    int numTablets = Integer.parseInt(args[0]);

    SortedSet<Text> partitionKeys = new TreeSet<>();

    long increment = Long.MAX_VALUE / numTablets;

    for(int i = 1; i< numTablets; i++) {
      String split = String.format("%016x", i * increment);
      partitionKeys.add(new Text(split));
    }


    conn.tableOperations().addSplits("bulky", partitionKeys);

  }
}
