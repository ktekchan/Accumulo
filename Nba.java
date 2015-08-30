/*
 * Author: Khushboo Tekchandani Popularity of NBA teams
 */
package hw5; 
import java.io.IOException; 
import java.text.DecimalFormat; 
import java.util.HashMap; 
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat; 
import org.apache.accumulo.core.data.Mutation; 
import org.apache.accumulo.core.data.Value; 
import org.apache.accumulo.core.security.ColumnVisibility; 
import org.apache.commons.cli.BasicParser; 
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter; 
import org.apache.commons.cli.Option; 
import org.apache.commons.cli.Options; 
import org.apache.commons.cli.Parser; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.Mapper; 
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.util.ToolRunner;

public class Nba extends Configured implements Tool{

   private static Options opts; private static Option passwordOpt; private
      static Option usernameOpt;

   static int flagFirst = 0;

   static { usernameOpt = new Option("u", "username", true, "username");
      passwordOpt = new Option("p", "password", true, "password");

      opts = new Options();

      opts.addOption(usernameOpt); opts.addOption(passwordOpt); }

   public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
      @Override public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {

      String line = value.toString(); //receive one line

      String teamName = ((FileSplit)
            context.getInputSplit()).getPath().getName();

      if (teamName.indexOf(".csv") >= 0) teamName = teamName.substring(0,
            teamName.indexOf(".csv"));

      int numWin = 0; int numLose = 0;

      String[] words = line.split(" ");

      for (String word : words){ if (word.trim().toLowerCase().equals("win")) {
         context.write(new Text(teamName + "/win"), new Text("1")); numWin++; }

         if (word.trim().toLowerCase().equals("lose")) { context.write(new
               Text(teamName + "/lose"), new Text("1")); numLose++; } }

      if (numWin == 0){ context.write(new Text(teamName + "/win"), new
            Text("0")); }

      if (numLose == 0){ context.write(new Text(teamName + "/lose"), new Text
            ("0")); }

      }

   }


   public static class ReducerClass extends Reducer<Text, Text, Text, Mutation>
   { @Override public void reduce(Text key, Iterable<Text> values, Context
         context) throws IOException, InterruptedException {

         String[] teamInfo = key.toString().split("/"); String teamName =
            teamInfo[0]; String winLose = teamInfo[1];

         HashMap <String, String> teamCoast = new HashMap <String,String>();

         teamCoast.put("Celtics", "east"); teamCoast.put("Knicks", "east");
         teamCoast.put("76ers", "east"); teamCoast.put("Nets", "east");
         teamCoast.put("Raptors", "east"); teamCoast.put("Bulls", "east");
         teamCoast.put("Pacers", "east"); teamCoast.put("Bucks", "east");
         teamCoast.put("Pistons", "east"); teamCoast.put("Cavs", "east");
         teamCoast.put("MiamiHeat", "east");
         teamCoast.put("OrlandoMagic", "east"); teamCoast.put("Hawks", "east");
         teamCoast.put("Bobcats", "east"); teamCoast.put("Wizards", "east");

         teamCoast.put("okcthunder", "west"); teamCoast.put("Nuggets", "west");
         teamCoast.put("TrailBlazers", "west"); teamCoast.put("UtahJazz",
               "west"); teamCoast.put("TWolves", "west");
         teamCoast.put("Lakers", "west"); teamCoast.put("Suns", "west");
         teamCoast.put("GSWarriors", "west"); teamCoast.put("Clippers", "west");
         teamCoast.put("NBAKings", "west"); teamCoast.put("GoSpursGo", "west");
         teamCoast.put("Mavs", "west"); teamCoast.put("Hornets", "west");
         teamCoast.put("Grizzlies", "west");       teamCoast.put("Rockets",
               "west");

         int valCount = 0;

         String coast = teamCoast.get(teamName);

         for (Text value : values){ if (value.toString().equals("1"))
            valCount++; }

         Text rowID = new Text(String.valueOf("Row")); Text columnFamily = new
            Text(winLose); Text columnQualifier = new Text(teamName); Value
            rowVal = new Value(String.valueOf(valCount).getBytes());


         if (winLose.equals("win")) { Mutation mutation1 = new Mutation(rowID);
            mutation1.put(columnFamily, columnQualifier,new
                  ColumnVisibility(coast), rowVal); context.write(new
                  Text("Wins"), mutation1); } else { Mutation mutation2 = new
                     Mutation(rowID); mutation2.put(columnFamily,
                           columnQualifier,new ColumnVisibility(coast), rowVal);
                     context.write(new Text("Losses"), mutation2); } } } public
                        int run(String[] unprocessed_args) throws Exception {

      Parser p = new BasicParser();

      CommandLine cl = p.parse(opts, unprocessed_args); String[] args =
         cl.getArgs();

      String username = cl.getOptionValue(usernameOpt.getOpt(), "root"); String
         password = cl.getOptionValue(passwordOpt.getOpt(), "acc");

      String instanceName = args[0]; String zookeepers = args[1]; String
         inputPath = args[2]; String tableName = args[3];

      Job job = new Job(getConf(), Nba.class.getName());
      job.setJarByClass(this.getClass());

      job.setInputFormatClass(TextInputFormat.class);
      TextInputFormat.setInputPaths(job, new Path(inputPath));

      job.setMapperClass(MapClass.class);
      job.setReducerClass(ReducerClass.class);

      job.setOutputFormatClass(AccumuloOutputFormat.class);
      job.setOutputKeyClass(Text.class); job.setOutputValueClass(Text.class);
      AccumuloOutputFormat.setOutputInfo(job, username, password.getBytes(),
            true, tableName); AccumuloOutputFormat.setZooKeeperInstance(job,
               instanceName, zookeepers); job.waitForCompletion(true);

      return 0; }

   public static void main(String[] args) throws Exception { int res =
      ToolRunner.run(new Configuration(), new Nba(), args); System.exit(res); }
}
