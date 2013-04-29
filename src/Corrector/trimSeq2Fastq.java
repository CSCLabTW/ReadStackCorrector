package Corrector;

import java.io.IOException;

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
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import Corrector.trimSeq2Fastq.trimSeq2FastqMapper;

public class trimSeq2Fastq extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(trimSeq2Fastq.class);

	public static class trimSeq2FastqMapper extends MapReduceBase
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
            if (seq == null) {
            	seq = "";
            }
            if (qscore == null) {
            	qscore = "";
            }

			// Hard chop a few bases off of each end of the read
			/*if (TRIM5 > 0 || TRIM3 > 0)
			{
				seq = seq.substring(TRIM5, seq.length() - TRIM5 - TRIM3);
			}*/

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

			/*Node node = new Node(tag);
            node.setstr(seq);
            node.setQscore(qscore);
            node.setCoverage(1);*/
			// Check for non-dna characters
			if (seq.matches(".*[^ACGT].*") || seq.length() != qscore.length() || seq.length() <= READLEN/2 )
			{
				//System.err.println("WARNING: non-DNA characters found in " + tag + ": " + seq);
				reporter.incrCounter("Brush", "trimSeq", 1);
				output.collect(new Text("@" + tag), new Text(""));
				output.collect(new Text(seq), new Text(""));
				output.collect(new Text("+" + tag), new Text(""));
				output.collect(new Text(qscore), new Text(""));
				
			}
		}
	}

	public RunningJob run(String inputPath, String outputPath) throws Exception
	{
		sLogger.info("Tool name: trimSeq2Fastq");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(trimSeq2Fastq.class);
		conf.setJobName("trimSeq2Fastq " + inputPath + " " + Config.K);

		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(trimSeq2FastqMapper.class);
		conf.setNumReduceTasks(0);
		//conf.setReducerClass(trimSeq2FastqReducer.class);

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
		int res = ToolRunner.run(new Configuration(), new trimSeq2Fastq(), args);
		System.exit(res);
	}
}


