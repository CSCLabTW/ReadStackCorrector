/*
    PreCorrect.java
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;


public class PreCorrect extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(PreCorrect.class);

	public static class PreCorrectMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public static int TRIM5 = 0;
		public static int TRIM3 = 0;
        private static int IDX = 0;

		public void configure(JobConf job)
		{
            IDX = Integer.parseInt(job.get("IDX"));
		}

		public void map(LongWritable lineid, Text nodetxt,
				        OutputCollector<Text, Text> output, Reporter reporter)
		                throws IOException
		{
            Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
            
            //slide the split K-mer windows for each read in both strands
            int end = node.len() - IDX -1;
            for (int i = 0; i < end; i++)
            {
                String window_tmp = node.str().substring(i, i+(IDX/2)) + node.str().substring(i+(IDX/2+1), i+(IDX+1));
                String window_r_tmp = Node.rc(node.str().substring(node.len()-(IDX+1)-i, node.len()-(IDX/2+1)-i) + node.str().substring(node.len()-(IDX/2)-i, node.len()-i));
                String window = Node.str2dna(window_tmp);
                String window_r = Node.str2dna(window_r_tmp);
                int f_pos = i + (IDX/2);
                int r_pos = node.len()-(IDX/2+1)-i;
                if ( !window_tmp.matches("A*") && !window_tmp.matches("T*") && !window_tmp.equals(window_r_tmp)) {
                    output.collect(new Text(window),
                                   new Text(node.getNodeId() + "\t" + "f" + "\t" + f_pos + "\t" + node.str().charAt(f_pos) + "\t" + node.Qscore_1().charAt(f_pos)));
                }
                if (!window_tmp.matches("A*") && !window_tmp.matches("T*") && !window_tmp.equals(window_r_tmp)) {
                    output.collect(new Text(window_r),
                                   new Text(node.getNodeId() + "\t" + "r" + "\t" + r_pos + "\t" + Node.rc(node.str().charAt(r_pos) + "") + "\t" + node.Qscore_1().charAt(r_pos)));
                }
            }
            
		}
	}

	public static class PreCorrectReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
		private static int K = 0;
        private static long HighKmer = 0;
        private static long IDX = 0;

		public void configure(JobConf job) {
			K = Integer.parseInt(job.get("K"));
            HighKmer = Long.parseLong(job.get("UP_KMER"));
            IDX = Integer.parseInt(job.get("IDX"));
		}

        public class ReadInfo
		{
			public String id;
            public String dir;
			public int pos;
            public String base;
            public char qv;
            public float cov;

			public ReadInfo(String id1, String dir1, int pos1, String base1, char qv1) throws IOException
			{
				id = id1;
                dir = dir1;
                pos = pos1;
                base = base1;
                qv = qv1;
                //cov = cov1;
			}

            public String toString()
			{
				return id + "!" + dir + "|" + pos + "|" + base + "|" + qv + "[" + ((int)qv-33) +"]";
			}
		}
        
		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            List<String> corrects_list = new ArrayList<String>();
			List<ReadInfo> readlist = new ArrayList<ReadInfo>();

            int prefix_sum = 0;
            int belong_read = 0;
            int kmer_count = 0;
            List<String> ReadID_list = new ArrayList<String>();
            
            //\\ 0:A 1:T 2:C 3:G 4:Count
            int[] base_array = new int[5];
            for(int i=0; i<5; i++) {
                base_array[i] = 0;
            }
            boolean lose_A = true;
            boolean lose_T = true;
            boolean lose_C = true;
            boolean lose_G = true;
            while(iter.hasNext())
			{
				String msg = iter.next().toString();
				String [] vals = msg.split("\t");
                ReadInfo read_item = new ReadInfo(vals[0],vals[1],Integer.parseInt(vals[2]), vals[3], vals[4].charAt(0));
                base_array[4] = base_array[4] + 1;
                int quality_value = ((int)read_item.qv-33);
                if (quality_value < 0 ) {
                    quality_value = 0;
                } else if (quality_value > 40) {
                    quality_value = 40;
                }
                if (read_item.base.equals("A")){
                    base_array[0] = base_array[0] + quality_value;
                    if (quality_value >= 20) {
                        lose_A = false;
                    }
                } else if (read_item.base.equals("T")) {
                    base_array[1] = base_array[1] + quality_value;
                    if (quality_value >= 20) {
                        lose_T = false;
                    }
                } else if (read_item.base.equals("C")) {
                    base_array[2] = base_array[2] + quality_value;
                    if (quality_value >= 20) {
                        lose_C = false;
                    }
                } else if (read_item.base.equals("G")) {
                    base_array[3] = base_array[3] + quality_value;
                    if (quality_value >= 20) {
                        lose_G = false;
                    }
                }
                readlist.add(read_item);
			}
            
            //\\
            if (readlist.size() <= 6) {
                return;
            }
            //\\\
            
            String correct_base = "N";
            int majority = 60;
            int reads_threshold = 6;
            float winner_sum = 0;
            if (base_array[0] > base_array[1] && base_array[0] > base_array[2] && base_array[0] > base_array[3] && base_array[0] >= majority && base_array[4] >= reads_threshold) {
                correct_base = "A";
                winner_sum = base_array[0];
            } else if (base_array[1] > base_array[0] && base_array[1] > base_array[2] && base_array[1] > base_array[3] && base_array[1] >= majority && base_array[4] >= reads_threshold) {
                correct_base = "T";
                winner_sum = base_array[1];
            } else if (base_array[2] > base_array[0] && base_array[2] > base_array[1] && base_array[2] > base_array[3] && base_array[2] >= majority && base_array[4] >= reads_threshold) {
                correct_base = "C";
                winner_sum = base_array[2];
            } else if (base_array[3] > base_array[0] && base_array[3] > base_array[1] && base_array[3] > base_array[2] && base_array[3] >= majority && base_array[4] >= reads_threshold) {
                correct_base = "G";
                winner_sum = base_array[3];
            }
            
            ReadInfo[] readarray = readlist.toArray(new ReadInfo[readlist.size()]);
            readlist.clear();
            if (!correct_base.equals("N") ) {
                boolean fix = true;
                for(int i=0; i < readarray.length; i++) {
                    if (!readarray[i].base.equals(correct_base)) {
                        //\\
                        if (readarray[i].base.equals("A") && ((float)base_array[0]/winner_sum > 0.25f || !lose_A )){
                            fix = false;
                            //continue;
                        }
                        if (readarray[i].base.equals("T") && ((float)base_array[1]/winner_sum > 0.25f || !lose_T )){
                            fix = false;
                            //continue;
                        }
                        if (readarray[i].base.equals("C") && ((float)base_array[2]/winner_sum > 0.25f || !lose_C )){
                            fix = false;
                            //continue;
                        }
                        if (readarray[i].base.equals("G") && ((float)base_array[3]/winner_sum > 0.25f || !lose_G )){
                            fix = false;
                            //continue;
                        }
                        //\\
                        if (readarray[i].dir.equals("f") && fix) {
                            String correct_msg =readarray[i].id + "," + readarray[i].pos + "," + correct_base;
                            if (!corrects_list.contains(correct_msg)){
                                corrects_list.add(correct_msg);
                            }
                            reporter.incrCounter("Brush", "fix_char", 1);
                        } 
                        if (readarray[i].dir.equals("r") && fix) {
                            String correct_msg = readarray[i].id + "," + readarray[i].pos + "," + Node.rc(correct_base);
                            if (!corrects_list.contains(correct_msg)){
                                corrects_list.add(correct_msg);
                                reporter.incrCounter("Brush", "fix_char", 1);
                            } 
                        }
                    }
                }
            }
            // replace close() function
            // create fake node to pass message
            Node node = new Node(readarray[0].id);
            node.setstr_raw("X");
            node.setCoverage(1);
            
            Map<String, List<String>> out_list = new HashMap<String, List<String>>();
            for(int i=0; i < corrects_list.size(); i++) {
                String[] vals = corrects_list.get(i).split(",");
                String id = vals[0];
                String msg = vals[1] + "," + vals[2];
                if (out_list.containsKey(id)){
                    out_list.get(id).add(msg);
                } else {
                    List<String> tmp_corrects = new ArrayList<String>();
                    tmp_corrects.add(msg);
                    out_list.put(id, tmp_corrects);
                }
                //mOutput.collect(new Text(vals[0]), new Text(vals[1] + "," + vals[2]));
            }

            for(String read_id : out_list.keySet())
            {
                String msgs="";
                List<String> correct_msg = out_list.get(read_id);
                // to many correct msg may cause by repeat
                msgs = correct_msg.get(0);
                if ( correct_msg.size() > 1) {
                    for (int i=1; i < correct_msg.size(); i++) {
                        msgs = msgs + "!" + correct_msg.get(i);
                    }
                } 
                node.addCorrections(read_id, msgs);
            }
            if (corrects_list.size() > 0 ) {
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            }
		}
	}



	public RunningJob run(String inputPath, String outputPath, int idx) throws Exception
	{
		sLogger.info("Tool name: PreCorrect");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(PreCorrect.class);
		conf.setJobName("PreCorrect " + inputPath + " " + Config.K);
        conf.setLong("IDX", idx);
        
		Config.initializeConfiguration(conf);

		FileInputFormat.addInputPath(conf, new Path(inputPath));
		FileOutputFormat.setOutputPath(conf, new Path(outputPath));

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(PreCorrectMapper.class);
		conf.setReducerClass(PreCorrectReducer.class);

		//delete the output directory if it exists already
		FileSystem.get(conf).delete(new Path(outputPath), true);

		return JobClient.runJob(conf);
	}

	public int run(String[] args) throws Exception
	{
		String inputPath  = "";
		String outputPath = "";
		Config.K = 21;

		long starttime = System.currentTimeMillis();

		run(inputPath, outputPath, 0);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new PreCorrect(), args);
		System.exit(res);
	}
}

