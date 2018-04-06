package cmd;

import org.apache.accumulo.core.client.Connector;

import acbase.CmdUtil;

public class Import {
  public static void main(String[] args) throws Exception {
    if(args.length != 1) {
      System.err.println("Usage : Import <dir>");
      System.exit(2);
    }

    Connector conn = CmdUtil.getConnector();
    conn.tableOperations().addFilesTo("bulky").from(args[0]).load();
  }
}
