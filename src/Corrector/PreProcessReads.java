/*
    PreProcessReads.java
    2012 Ⓒ ReadStackCorrector, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/ReadStackCorrector
*/

package Corrector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class PreProcessReads extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(PreProcessReads.class);

	public static class PreProcessReadsMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
        public static int READLEN = 36;
		public static int TRIM5 = 0;
		public static int TRIM3 = 0;

		public void configure(JobConf job)
		{
            READLEN = (int)Long.parseLong(job.get("READLENGTH"));
		}

		public void map(LongWritable lineid, Text nodetxt,
				        OutputCollector<Text, Text> output, Reporter reporter)
		                throws IOException
		{
			String[] fields = nodetxt.toString().split("\t");

			if (fields.length != 3)
			{
				reporter.incrCounter("Brush", "input_lines_invalid", 1);
				return;
			}

			String tag = fields[0];

			tag = tag.replaceAll(" ", "_");
			tag = tag.replaceAll(":", "_");
			tag = tag.replaceAll("#", "_");
			tag = tag.replaceAll("-", "_");
			tag = tag.replaceAll("\\.", "_");
            tag = tag.replaceAll("/", "_");

			String seq = fields[1].toUpperCase();
            String qscore = fields[2].toString();

			// Hard chop a few bases off of each end of the read
			if (TRIM5 > 0 || TRIM3 > 0)
			{
				seq = seq.substring(TRIM5, seq.length() - TRIM5 - TRIM3);
			}

			// Automatically trim Ns off the very ends of reads
			/*int endn = 0;
			while (endn < seq.length() && seq.charAt(seq.length()-1-endn) == 'N') { endn++; }
			if (endn > 0) { seq = seq.substring(0, seq.length()-endn); }

			int startn = 0;
			while (startn < seq.length() && seq.charAt(startn) == 'N') { startn++; }
			if (startn > 0 && (seq.length() - startn) > startn) {
                seq = seq.substring(startn, seq.length() - startn);
            }*/
            //if (startn > 0) { seq = seq.substring(startn, seq.length() - startn); }

			// Check for non-dna characters
			if (seq.matches(".*[^ACGT].*"))
			{
				//System.err.println("WARNING: non-DNA characters found in " + tag + ": " + seq);
				reporter.incrCounter("Brush", "reads_skipped", 1);
				return;
			}
			
			// Check length of seq equal to length of qv
			if (seq.length() != qscore.length()) {
				reporter.incrCounter("Brush", "reads_skipped", 1);
				return;
			}

			// check for short reads
			if (seq.length() <= READLEN/2)
			{
				//System.err.println("WARNING: read " + tag + " is too short: " + seq);
				reporter.incrCounter("Brush", "reads_short", 1);
				return;
			}

			// Now emit the prefix of the reads
            Node node = new Node(tag);
            node.setstr(seq);
            //node.setQscore(qscore);
            node.setQV(Node.qv2str(qscore));
            node.setCoverage(1);
            //output.collect(new Text(node.getNodeId()), new Text(node.Qscore()) );
            output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
			reporter.incrCounter("Brush", "reads_good", 1);
			reporter.incrCounter("Brush", "reads_goodbp", seq.length());
            
            for(int i=0; i < qscore.length(); i++) {
                reporter.incrCounter("Brush", "qscore_bp", 1);
                if (((int)qscore.charAt(i)-33) < 0) {
                    reporter.incrCounter("Brush", "Q0", 1);
                } else if (((int)qscore.charAt(i)-33) >= 0 && ((int)qscore.charAt(i)-33) < 10 ) {
                    reporter.incrCounter("Brush", "Q0_Q10", 1);
                } else if (((int)qscore.charAt(i)-33) >= 10 && ((int)qscore.charAt(i)-33) < 20 ) {
                    reporter.incrCounter("Brush", "Q10_Q20", 1);
                } else if (((int)qscore.charAt(i)-33) >= 20 && ((int)qscore.charAt(i)-33) < 30 ) {
                    reporter.incrCounter("Brush", "Q20_Q30", 1);
                } else if (((int)qscore.charAt(i)-33) >= 30 && ((int)qscore.charAt(i)-33) < 40 ) {
                    reporter.incrCounter("Brush", "Q30_Q40", 1);
                } else {
                    reporter.incrCounter("Brush", "Q40", 1);
                }
            }
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: PreProcessReads");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PreProcessReads.class);
		conf.setJobName("PreProcessReads " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PreProcessReadsMapper.class);
		//conf.setReducerClass(PreProcessReadsReducer.class);
		conf.setNumReduceTasks(0);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception
	{
		String inputPath  = "/cygdrive/contrail-bio/data/Ec10k.sim.sfa";
		String outputPath = "/cygdrive/contrail-bio/";
		Config.K = 21;

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new PreProcessReads(), args);
		System.exit(res);
	}
}


