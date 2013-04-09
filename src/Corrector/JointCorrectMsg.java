/*
    JointCorrectMsg.java
    2012 Ⓒ ReadStackCorrector, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/ReadStackCorrector
*/
package Corrector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;



public class JointCorrectMsg extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(JointCorrectMsg.class);


	// PopBubblesMapper
	///////////////////////////////////////////////////////////////////////////

	public static class JointCorrectMsgMapper extends MapReduceBase
	implements Mapper<LongWritable, Text, Text, Text>
	{
		private static int K = 0;
        private static Node node = new Node();
        
        //-- in-mapper combiner
        private  Map<String,String> mOutput ;  
        private int N ;  
          
        protected void setup(Context context) throws IOException,InterruptedException {  
            mOutput = new HashMap<String,String>();  
            N = 0;  
        }     
        //\\\\\\\\\\\\\\\\
        
		public void configure(JobConf job)
		{
			K = Integer.parseInt(job.get("K"));
		}

		
        public void map(LongWritable lineid, Text nodetxt,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException {
        	
        }
        protected void map(LongWritable lineid, Text nodetxt, Context context)  
        throws IOException, InterruptedException  
		{
            node.fromNodeMsg(nodetxt.toString());
            if (node.str_raw().equals("X")) {
                List<String> confirmations = node.getConfirmations();
                if (confirmations != null)
                {
                    for(String confirmation : confirmations)
                    {
                        String [] vals = confirmation.split("\\|");
                        String id    = vals[0];
                        String confirm_msg   = vals[1];

                        /*output.collect(new Text(id),
                                       new Text(confirm_msg));*/
                        //\\ in-mapper combiner
                        N++;
                        if(!mOutput.containsKey(id)){  
                            mOutput.put(id, confirm_msg);  
                        }else{  
                            String tmp = mOutput.get(id);  
                            //\\ merge correct msg
                            mOutput.put(id, tmp);  
                        }  
                        //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                    }
                }
            } 
            
            if(N == 1000){  
                for(Map.Entry<String, String> m : mOutput.entrySet()){  
                    context.write(new Text(m.getKey()), new Text(m.getValue()));  
                }  
                N = 0;  
                mOutput.clear();  
                //System.out.println("write two key/value");  
            }  
           
		}
	}

	// JointCorrectMsgReducer
	///////////////////////////////////////////////////////////////////////////

	public static class JointCorrectMsgReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
		}

        public class Correct
        {
            public char chr;
            public int pos;
            public Correct(int pos1, char chr1) throws IOException
            {
                pos = pos1;
                chr = chr1;
            }
        }

		public void reduce(Text nodeid, Iterator<Text> iter,
				OutputCollector<Text, Text> output, Reporter reporter)
		throws IOException
		{
			Node node = new Node(nodeid.toString());

			int sawnode = 0;

			boolean killnode = false;
			float extracov = 0;
            //List<Fix> fixs = new ArrayList<Fix>();
            List<String> confirms = new ArrayList<String>();
            //List<String> corrects = new ArrayList<String>();
            List<Correct> corrects = new ArrayList<Correct>();
            
			while(iter.hasNext())
			{
				String msg = iter.next().toString();

				char[] code_msg = Node.code2str(msg).toCharArray();
                //System.err.println(nodeid.toString() + "\t" + msg);

				/*String [] vals = msg.split("\t");

				if (vals[0].equals(Node.NODEMSG))
				{
					node.parseNodeMsg(vals, 0);
					sawnode++;
				}
                else if (vals[0].equals(Node.UPDATEMSG))
				{
					//Confirm confirm = new Confirm(vals, 0);
					char[] code_msg = Node.code2str(vals[1]).toCharArray();
                    for(int i=0; i < code_msg.length; i++) {
                        if (code_msg[i] == 'N') {
                            confirms.add(i+"");
                        } else if (code_msg[i] == 'A' || code_msg[i] == 'T'|| code_msg[i] == 'C'|| code_msg[i] == 'G' ){
                            corrects.add(new Correct(i, code_msg[i]));
                        }
                    }
                    //confirms.addAll(confirm.pos);
				}
				else
				{
					throw new IOException("Unknown msgtype: " + msg);
				}*/
			}

			if (sawnode != 1)
			{
                throw new IOException("ERROR: Didn't see exactly 1 nodemsg (" + sawnode + ") for " + nodeid.toString());
			}
            
            boolean failed_reads = false;
            Correct[] c_array = corrects.toArray(new Correct[corrects.size()]);
            corrects.clear();
            if (c_array.length > 0) {
                //\\ 0:A 1:T 2:C 3:G 4:Sum
                int[][] array = new int[node.len()][5];
                for(int i=0; i < node.len(); i++) {
                    for(int j=0; j < 5; j++) {
                        array[i][j] = 0;
                    }
                }
                for(int i=0; i < c_array.length; i++) {
                    //String [] vals = fix_msg.split(",");
                    int pos = c_array[i].pos;
                    char fix_chr = c_array[i].chr;
                    //\\
                    if (confirms.contains(pos+"")){
                        array[pos][4] = -100;
                        reporter.incrCounter("Brush", "confirms", 1);
                        continue;
                    }
                    //\\
                    array[pos][4] = array[pos][4] + 1;
                    if (fix_chr == 'A') {
                        array[pos][0] = array[pos][0] + 1;
                    } else if (fix_chr == 'T') {
                        array[pos][1] = array[pos][1] + 1;
                    } else if (fix_chr == 'C') {
                        array[pos][2] = array[pos][2] + 1;
                    } else if (fix_chr == 'G') {
                        array[pos][3] = array[pos][3] + 1;
                    }      
                }
                // fix str content
                String fix_str = ""; //node.str().substring(0, pos) + fix_char + node.str().substring(pos+1); 
                String fix_qv = "";
                for(int i=0; i < array.length; i++){
                    if (array[i][4] > 0 && array[i][0] == array[i][4] ) {
                        fix_str = fix_str + "A";
                        fix_qv = fix_qv + (char)33;
                        reporter.incrCounter("Brush", "fix_char", 1);
                    } else if (array[i][4] > 0 && array[i][1] == array[i][4] ) {
                        fix_str = fix_str + "T";
                        fix_qv = fix_qv + (char)33;
                        reporter.incrCounter("Brush", "fix_char", 1);
                    } else if (array[i][4] > 0 && array[i][2] == array[i][4] ) {
                        fix_str = fix_str + "C";
                        fix_qv = fix_qv + (char)33;
                        reporter.incrCounter("Brush", "fix_char", 1);
                    } else if (array[i][4] > 0 && array[i][3] == array[i][4] ) {
                        fix_str = fix_str + "G";
                        fix_qv = fix_qv + (char)33;
                        reporter.incrCounter("Brush", "fix_char", 1);
                    } else if (array[i][4] < 0 ) {
                        fix_str = fix_str + node.str().charAt(i);
                        //fix_qv = fix_qv + (char)53;
                        fix_qv = fix_qv + node.Qscore_1().charAt(i);
                    } else {
                        fix_str = fix_str + node.str().charAt(i);
                        fix_qv = fix_qv + node.Qscore_1().charAt(i);
                    }
                }
                node.setstr(fix_str);
                node.setQscore(fix_qv);
            }
            output.collect(nodeid, new Text(node.toNodeMsg()));
		}
	}


	// Run Tool
	///////////////////////////////////////////////////////////////////////////

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: JointCorrectMsg [0/7]");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(JointCorrectMsg.class);
		conf.setJobName("JointCorrectMsg " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
        //conf.setBoolean("mapred.output.compress", true);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(JointCorrectMsgMapper.class);
		conf.setReducerClass(JointCorrectMsgReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}


	// Parse Arguments and run
	///////////////////////////////////////////////////////////////////////////

	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";

		Config.K = 21;

		run(inputPath, outputPath);
		return 0;
	}


	// Main
	///////////////////////////////////////////////////////////////////////////

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new JointCorrectMsg(), args);
		System.exit(res);
	}
}



