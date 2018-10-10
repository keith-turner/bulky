package cmd;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.LoadPlan;
import org.apache.accumulo.core.data.LoadPlan.Builder;
import org.apache.accumulo.core.data.LoadPlan.RangeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import acbase.CmdUtil;

public class Import {
  public static void main(String[] args) throws Exception {
    if(!(args.length == 2 || (args.length == 3 && args[2].equals("plan")))) {
      System.err.println("Usage : Import old|new <dir> [plan]");
      System.exit(2);
    }

    Connector conn = CmdUtil.getConnector();
    String dir = args[1];

    long t1 = System.currentTimeMillis();
    if(args[0].equals("old")) {
      conn.tableOperations().importDirectory("bulky", dir, dir+"-fail", false);
    } else {
      if(args.length == 3) {
         FileStatus[] files = FileSystem.get(new Configuration()).listStatus(new Path(dir));
         Builder builder = LoadPlan.builder();
         for (FileStatus fileStatus : files) {
           String name = fileStatus.getPath().getName();
           String[] nameParts = name.split("[-.]");
           builder.loadFileTo(name, RangeType.FILE, nameParts[1], nameParts[2]);
         }

         LoadPlan plan = builder.build();
         conn.tableOperations().importDirectory(dir).to("bulky").plan(plan).load();
      } else {
        conn.tableOperations().importDirectory(dir).to("bulky").load();
      }
    }

    long t2 = System.currentTimeMillis();
    System.out.println("Import time:"+(t2 - t1));
  }
}
