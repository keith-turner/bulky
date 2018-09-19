package cmd;

import org.apache.accumulo.core.client.Connector;

import acbase.CmdUtil;

public class Import {
  public static void main(String[] args) throws Exception {
    if(args.length != 2) {
      System.err.println("Usage : Import old|new <dir>");
      System.exit(2);
    }

    Connector conn = CmdUtil.getConnector();
    String dir = args[1];

    if(args[0].equals("old")) {
      long t1 = System.currentTimeMillis();
      conn.tableOperations().importDirectory("bulky", dir, dir+"-fail", false);
      long t2 = System.currentTimeMillis();
      System.out.println(t2 - t1);
    } else {
      throw new IllegalArgumentException(args[0]);
    }
  }
}
