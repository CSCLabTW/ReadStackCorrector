/*
    FindError.java
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


public class FindError extends Configured implements Tool
{
	private static final Logger sLogger = Logger.getLogger(FindError.class);

	public static class FindErrorMapper extends MapReduceBase
    implements Mapper<LongWritable, Text, Text, Text>
	{
		public static int K = 0;
        public static int IDX = 0;
		public static int TRIM5 = 0;
		public static int TRIM3 = 0;

		public void configure(JobConf job)
		{
			//K = Integer.parseInt(job.get("K"));
            IDX = Integer.parseInt(job.get("IDX"));
		}

		public void map(LongWritable lineid, Text nodetxt,
				        OutputCollector<Text, Text> output, Reporter reporter)
		                throws IOException
		{
            Node node = new Node();
			node.fromNodeMsg(nodetxt.toString());
           
            //slide the split K-mer windows for each read in both strands
            int end = node.len() - IDX;
            for (int i = 0; i < end; i++)
            {
                String window_tmp = node.str().substring(i, i+IDX);
                String window_r_tmp = Node.rc(node.str().substring(node.len()-IDX-i, node.len()-i));
                String window = Node.str2dna(window_tmp);
                String window_r = Node.str2dna(window_r_tmp);
                int f_pos = i;
                int r_pos = i;
                if ( !window_tmp.matches("A*") && !window_tmp.matches("T*") && !window_tmp.equals(window_r_tmp) ) {
                    output.collect(new Text(window),
                                   new Text(node.getNodeId() + "\t" + "f" + "\t" + f_pos + "\t" + node.str_raw() + "\t" + node.Qscore_1()));
                }
                String Qscore_reverse = new StringBuffer(node.Qscore_1()).reverse().toString();
                if (!window_tmp.matches("A*") && !window_tmp.matches("T*") && !window_tmp.equals(window_r_tmp) ) {
                    output.collect(new Text(window_r),
                                   new Text(node.getNodeId() + "\t" + "r" + "\t" + r_pos + "\t" + node.str_raw() + "\t" + Qscore_reverse));
                }
            }
            
		}
	}

	public static class FindErrorReducer extends MapReduceBase
	implements Reducer<Text, Text, Text, Text>
	{
        private static int IDX = 0;
        private static long HighKmer = 0;

		public void configure(JobConf job) {
            IDX = Integer.parseInt(job.get("IDX"));
            HighKmer = Long.parseLong(job.get("UP_KMER"));
		}

        public class ReadInfo
		{
			public String id;
            public String dir;
			public int pos;
            //public String seq;
            public char[] seq;
            //public String qv;
            public int[] int_qv;

			public ReadInfo(String id1, String dir1, int pos1, String seq1, String qv1) throws IOException
			{
				id = id1;
                dir = dir1;
                pos = pos1;
                if (dir.equals("f")) {
                    seq = Node.dna2str(seq1).toCharArray();                    
                } else {
                    seq = Node.rc(Node.dna2str(seq1)).toCharArray();
                }
                //qv = qv1;
                int_qv = new int[qv1.length()];
                for(int i=0; i < int_qv.length; i++){
                    int_qv[i] = (int)qv1.charAt(i)-33;
                }
			}

            public String toString()
			{
                return id + "!" + dir + "|" + pos + "|" + seq ;   
			}
		}
        
        class ReadComparator_right implements Comparator {
            public int compare(Object element1, Object element2) {
                ReadInfo obj1 = (ReadInfo) element1;
                ReadInfo obj2 = (ReadInfo) element2;
                if ((int) ( (obj1.seq.length- obj1.pos) - (obj2.seq.length - obj2.pos) ) > 0) {
                    return -1;
                } else if ((int) ( (obj1.seq.length- obj1.pos) - (obj2.seq.length - obj2.pos) ) < 0) {
                    return 1;
                } else {
                    if ( obj1.id.compareTo(obj2.id) < 0) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
        }
        
        class ReadComparator implements Comparator {
            public int compare(Object element1, Object element2) {
                ReadInfo obj1 = (ReadInfo) element1;
                ReadInfo obj2 = (ReadInfo) element2;
                if ((int) ( obj1.pos - obj2.pos ) > 0) {
                    return -1;
                } else if ((int) ( obj1.pos - obj2.pos ) < 0) {
                    return 1;
                } else {
                    if ( obj1.id.compareTo(obj2.id) < 0) {
                        return -1;
                    } else {
                        return 1;
                    }
                }
            }
        }
        
		public void reduce(Text prefix, Iterator<Text> iter,
						   OutputCollector<Text, Text> output, Reporter reporter)
						   throws IOException
		{
            List<String> code_list = new ArrayList<String>();
			List<ReadInfo> readlist = new ArrayList<ReadInfo>();
            List<String> ReadID_list = new ArrayList<String>();
            
            while(iter.hasNext())
			{
				String msg = iter.next().toString();
				String [] vals = msg.split("\t");
                ReadInfo read_item = new ReadInfo(vals[0],vals[1],Integer.parseInt(vals[2]), vals[3], vals[4]);
                readlist.add(read_item);
                if (readlist.size() > HighKmer) {
                    //break;
                    return;
                }
			}
            
            //\\
            if (readlist.size() <= 5 ) {
                return;
            }
            //\\\
            
            Collections.sort(readlist, new ReadComparator_right());
            int right_len = readlist.get(0).seq.length - IDX - readlist.get(0).pos;
            Collections.sort(readlist, new ReadComparator());
            int left_len = readlist.get(0).pos;
            //\\ 0:A 1:T 2:C 3:G 4:Sum 5:max 
            int[][] left_array = new int[left_len][6];
            int[][] left_lose = new int[left_len][4];
            for(int i=0; i < left_len; i++) {
                for(int j=0; j < 6; j++) {
                    left_array[i][j] = 0;
                    if( j < 4) {
                        left_lose[i][j] = 0;
                    }
                } 
            }
            //\\
            int [][] IDX_lose = new int[IDX][4];
            int [][] IDX_array = new int[IDX][6];
            for(int i=0; i < IDX; i++) {
                for(int j=0; j < 6; j++){
                    IDX_array[i][j] = 0;
                	if ( j < 4){
                    	IDX_lose[i][j] = 0;
                    }
                }
            }
            //\\
            int[][] right_array = new int[right_len][6];
            int[][] right_lose = new int[right_len][4];
            for(int i=0; i < right_len; i++) {
                for(int j=0; j < 6; j++) {
                    right_array[i][j] = 0;
                    if ( j < 4) {
                        right_lose[i][j] = 0;
                    }
                }
            }
            //
            ReadInfo[] readarray = readlist.toArray(new ReadInfo[readlist.size()]);
            readlist.clear();
            for(int i=0; i < readarray.length; i++){
                // compute_letf_array
                for(int j=0; j < readarray[i].pos; j++) {
                    left_array[left_len - readarray[i].pos + j][4] = left_array[left_len - readarray[i].pos + j][4] + 1;
                    int quality_value = readarray[i].int_qv[j];
                    if (quality_value < 0) {
                        quality_value = 0;
                    } else if (quality_value > 40) {
                        quality_value = 40;
                    }
                    if (readarray[i].seq[j] == 'A') {
                        left_array[left_len - readarray[i].pos + j][0] = left_array[left_len - readarray[i].pos + j][0] + quality_value;
                        if (quality_value >= 20)
                            left_lose[left_len - readarray[i].pos + j][0] = left_lose[left_len - readarray[i].pos + j][0]+1;
                    } else if (readarray[i].seq[j] == 'T') {
                        left_array[left_len - readarray[i].pos + j][1] = left_array[left_len - readarray[i].pos + j][1] + quality_value;
                        if (quality_value >= 20)
                            left_lose[left_len - readarray[i].pos + j][1] = left_lose[left_len - readarray[i].pos + j][1]+1;
                    } else if (readarray[i].seq[j] == 'C') {
                        left_array[left_len - readarray[i].pos + j][2] = left_array[left_len - readarray[i].pos + j][2] + quality_value;
                        if (quality_value >= 20)
                            left_lose[left_len - readarray[i].pos + j][2] = left_lose[left_len - readarray[i].pos + j][2]+1;
                    } else if (readarray[i].seq[j] == 'G') {
                        left_array[left_len - readarray[i].pos + j][3] = left_array[left_len - readarray[i].pos + j][3] + quality_value;
                        if (quality_value >= 20)
                            left_lose[left_len - readarray[i].pos + j][3] = left_lose[left_len - readarray[i].pos + j][3]+1;
                    }
                }
                //\compute IDX array
                for(int j=readarray[i].pos; j < readarray[i].pos + IDX; j++) {
                    int quality_value = (int)readarray[i].int_qv[j];
                    if (quality_value < 0) {
                        quality_value = 0;
                    } else if (quality_value > 40) {
                        quality_value = 40;
                    }
                    IDX_array[j-readarray[i].pos][4] = IDX_array[j-readarray[i].pos][4] + 1;
                    if (readarray[i].seq[j] == 'A') {
                        IDX_array[j-readarray[i].pos][0]= IDX_array[j-readarray[i].pos][0] + quality_value;
                    	if  (quality_value >= 20)
                            IDX_lose[j-readarray[i].pos][0] = IDX_lose[j-readarray[i].pos][0]+1;
                    } else if (readarray[i].seq[j] == 'T') { 
                    	IDX_array[j-readarray[i].pos][1]= IDX_array[j-readarray[i].pos][1] + quality_value;
                    	if  (quality_value >= 20)
                            IDX_lose[j-readarray[i].pos][1] = IDX_lose[j-readarray[i].pos][1]+1;
                    } else if (readarray[i].seq[j] == 'C') { 
                    	IDX_array[j-readarray[i].pos][2]= IDX_array[j-readarray[i].pos][2] + quality_value;
                    	if  (quality_value >= 20)
                            IDX_lose[j-readarray[i].pos][2] = IDX_lose[j-readarray[i].pos][2]+1;
                    } else if (readarray[i].seq[j] == 'G') {
                    	IDX_array[j-readarray[i].pos][3]= IDX_array[j-readarray[i].pos][3] + quality_value;
                    	if  (quality_value >= 20)
                            IDX_lose[j-readarray[i].pos][3] = IDX_lose[j-readarray[i].pos][3]+1;
                    }
                }
                //\\\
                // compute_right_array
                for(int j=readarray[i].pos + IDX; j < readarray[i].seq.length; j++) {
                    int quality_value = (int)readarray[i].int_qv[j];
                    if (quality_value < 0) {
                        quality_value = 0;
                    } else if (quality_value > 40) {
                        quality_value = 40;
                    }
                    right_array[j-readarray[i].pos-IDX][4] = right_array[j-readarray[i].pos-IDX][4] + 1;
                    if (readarray[i].seq[j] == 'A') {
                        right_array[j-readarray[i].pos-IDX][0] = right_array[j-readarray[i].pos-IDX][0] + quality_value;
                        if  (quality_value >= 20)
                            right_lose[j-readarray[i].pos-IDX][0] = right_lose[j-readarray[i].pos-IDX][0]+1;
                    } else if (readarray[i].seq[j] == 'T') { 
                        right_array[j-readarray[i].pos-IDX][1] = right_array[j-readarray[i].pos-IDX][1] + quality_value;
                        if  (quality_value >= 20)
                            right_lose[j-readarray[i].pos-IDX][1] = right_lose[j-readarray[i].pos-IDX][1]+1;
                    } else if (readarray[i].seq[j] == 'C') { 
                        right_array[j-readarray[i].pos-IDX][2] = right_array[j-readarray[i].pos-IDX][2] + quality_value;
                        if  (quality_value >= 20)
                            right_lose[j-readarray[i].pos-IDX][2] = right_lose[j-readarray[i].pos-IDX][2]+1;
                    } else if (readarray[i].seq[j] == 'G') {
                        right_array[j-readarray[i].pos-IDX][3] = right_array[j-readarray[i].pos-IDX][3] + quality_value;
                        if  (quality_value >= 20)
                            right_lose[j-readarray[i].pos-IDX][3] = right_lose[j-readarray[i].pos-IDX][3]+1;
                    }
                }        
            }
            
            int majority = 2;
            int reads_threshold = 6;
            // compute left consensus
            String left_consensus = "";
            int left_branch=-1;
            for(int i=0; i < left_array.length; i++){
                if ( left_array[i][0] > left_array[i][1] && left_array[i][0] > left_array[i][2] && left_array[i][0] > left_array[i][3] /*&& left_lose[i][0] >= majority && left_array[i][4] >= reads_threshold*/) {
                    left_consensus = left_consensus + "A";
                    left_array[i][5] = left_array[i][0];
                } else if (left_array[i][1] > left_array[i][0] && left_array[i][1] > left_array[i][2] && left_array[i][1] > left_array[i][3] /*&& left_lose[i][1] >= majority && left_array[i][4] >= reads_threshold*/) {
                    left_consensus = left_consensus + "T";
                    left_array[i][5] = left_array[i][1];
                } else if (left_array[i][2] > left_array[i][0] && left_array[i][2] > left_array[i][1] && left_array[i][2] > left_array[i][3] /*&& left_lose[i][2] >= majority && left_array[i][4] >= reads_threshold*/) {
                    left_consensus = left_consensus + "C";
                    left_array[i][5] = left_array[i][2];
                } else if (left_array[i][3] > left_array[i][0] && left_array[i][3] > left_array[i][1] && left_array[i][3] > left_array[i][2] /*&& left_lose[i][3] >= majority && left_array[i][4] >= reads_threshold*/) {
                    left_consensus = left_consensus + "G";
                    left_array[i][5] = left_array[i][3];
                } else {
                    left_consensus = left_consensus + "N";
                    left_array[i][5] = left_array[i][0];
                }
                //\\ apply branch
                int support = 0;
                if (left_lose[i][0] >= majority ){
                	support = support +1;
                }
                if (left_lose[i][1] >= majority ){
                	support = support +1;
                }
                if (left_lose[i][2] >= majority ){
                	support = support +1;
                }
                if (left_lose[i][3] >= majority ){
                	support = support +1;
                }
                if (support >=2){
                	left_branch =i;
                }
            }
            //\\ apply branch, using N to replace char before left_branch point
            String left_branch_str="";
            if (left_branch >= 0) {
            	left_branch = left_branch -1; //exclude branch point
            }
            for (int i=0; i<= left_branch;i++) {
            	left_branch_str = left_branch_str+ "N";
            }
            left_consensus = left_branch_str + left_consensus.substring(left_branch_str.length());
            //\\\\\\\\\
            // compute right consensus
            String right_consensus = "";
            int right_branch=-1;
            for(int i=0; i < right_array.length; i++){
                if (right_array[i][0] > right_array[i][1] && right_array[i][0] > right_array[i][2] && right_array[i][0] > right_array[i][3] /*&& right_lose[i][0] >= majority && right_array[i][4] >= reads_threshold*/) {
                    right_consensus = right_consensus + "A";
                    right_array[i][5] = right_array[i][0];
                } else if (right_array[i][1] > right_array[i][0] && right_array[i][1] > right_array[i][2] && right_array[i][1] > right_array[i][3] /*&& right_lose[i][1] >= majority && right_array[i][4] >= reads_threshold*/) {
                    right_consensus = right_consensus + "T";
                    right_array[i][5] = right_array[i][1];
                } else if (right_array[i][2] > right_array[i][0] && right_array[i][2] > right_array[i][1] && right_array[i][2] > right_array[i][3] /*&& right_lose[i][2] >= majority && right_array[i][4] >= reads_threshold*/) {
                    right_consensus = right_consensus + "C";
                    right_array[i][5] = right_array[i][2];
                } else if (right_array[i][3] > right_array[i][0] && right_array[i][3] > right_array[i][1] && right_array[i][3] > right_array[i][2] /*&& right_lose[i][3] >= majority && right_array[i][4] >= reads_threshold*/) {
                    right_consensus = right_consensus + "G";
                    right_array[i][5] = right_array[i][3];
                } else {
                    right_consensus = right_consensus + "N";
                    right_array[i][5] = right_array[i][0];
                }
              //\\ apply branch
                int support = 0;
                if (right_lose[i][0] >= majority ){
                	support = support +1;
                }
                if (right_lose[i][1] >= majority ){
                	support = support +1;
                }
                if (right_lose[i][2] >= majority ){
                	support = support +1;
                }
                if (right_lose[i][3] >= majority ){
                	support = support +1;
                }
                if (support >=2 && right_branch < 0){
                	right_branch =i;
                }
            }
            //\\ apply branch, using N to replace char after right_branch point
            if (right_branch >= 0) {
	            String right_branch_str="";
	            for (int i=0; i< right_consensus.length()-(right_branch+1);i++) {
	            	right_branch_str = right_branch_str+ "N";
	            }
	            right_consensus = right_consensus.substring(0,right_branch+1)+right_branch_str;
            }
            
            Node node = new Node(readarray[0].id);
            node.setstr_raw("X");
            node.setCoverage(1);   
            Map<String, StringBuffer> outcode_list = new HashMap<String, StringBuffer>();
            for(int i=0; i < readarray.length; i++){
                // left array
                for(int j=0; j < readarray[i].pos; j++) {
                    String id = readarray[i].id;
                    int pos = 0;
                    char chr = 'X';
                    if (left_consensus.charAt(left_len - readarray[i].pos + j) == readarray[i].seq[j]) {
                        // Comfirmation
                        boolean confirm = false;
                        if (left_lose[left_len - readarray[i].pos + j][0] >= 3 && readarray[i].seq[j] == 'A' && (left_lose[left_len - readarray[i].pos + j][1] < 2 && left_lose[left_len - readarray[i].pos + j][2] < 2 && left_lose[left_len - readarray[i].pos + j][3] < 2)) {
                            confirm = true;
                        }
                        if (left_lose[left_len - readarray[i].pos + j][1] >= 3 && readarray[i].seq[j] == 'T' && (left_lose[left_len - readarray[i].pos + j][0] < 2 && left_lose[left_len - readarray[i].pos + j][2] < 2 && left_lose[left_len - readarray[i].pos + j][3] < 2)) {
                            confirm = true;
                        }
                        if (left_lose[left_len - readarray[i].pos + j][2] >= 3 && readarray[i].seq[j] == 'C' && (left_lose[left_len - readarray[i].pos + j][0] < 2 && left_lose[left_len - readarray[i].pos + j][1] < 2 && left_lose[left_len - readarray[i].pos + j][3] < 2)) {
                            confirm = true;
                        }
                        if (left_lose[left_len - readarray[i].pos + j][3] >= 3 && readarray[i].seq[j] == 'G' && (left_lose[left_len - readarray[i].pos + j][0] < 2 && left_lose[left_len - readarray[i].pos + j][1] < 2 && left_lose[left_len - readarray[i].pos + j][2] < 2)) {
                            confirm = true;
                        }
                        if (confirm ) {
                            boolean submit = true;
                            if (readarray[i].dir.equals("f") && readarray[i].int_qv[j] < 20){
                                pos = j;
                            } else if (readarray[i].dir.equals("r") && (int)readarray[i].int_qv[readarray[i].seq.length-1-j] < 20) {
                                pos = (readarray[i].seq.length-1-j);
                            } else {
                                submit = false;
                            }
                            if (submit) {
                                if (outcode_list.containsKey(id)){
                                    StringBuffer sb = outcode_list.get(id);
                                    if (pos >= sb.length()){
                                        for(int k=sb.length(); k<=pos; k++) {
                                            sb.append("X");
                                        }
                                        sb.setCharAt(pos, 'N');
                                    } else {
                                        sb.setCharAt(pos, 'N');
                                    }
                                    outcode_list.put(id, sb);
                                    reporter.incrCounter("Brush", "confirm_char", 1);
                                } else {
                                    StringBuffer sb = new StringBuffer();
                                    for(int k=0; k <= pos; k++) {
                                        sb.append("X");
                                    }
                                    sb.setCharAt(pos, 'N');
                                    outcode_list.put(id, sb);
                                    reporter.incrCounter("Brush", "confirm_char", 1);
                                }
                            }
                        }
                        //\\\\
                    } else {
                        if (!(left_consensus.charAt(left_len - readarray[i].pos + j) == 'N')) {
                            //\\
                            if(readarray[i].seq[j] == 'A' && ( left_lose[left_len - readarray[i].pos + j][0] >= 2 || (float)left_array[left_len - readarray[i].pos + j][0]/(float)left_array[left_len - readarray[i].pos + j][5] > 0.25f)) {
                                continue;
                            }
                            if(readarray[i].seq[j] == 'T' && ( left_lose[left_len - readarray[i].pos + j][1] >= 2 || (float)left_array[left_len - readarray[i].pos + j][1]/(float)left_array[left_len - readarray[i].pos + j][5] > 0.25f)) {
                                continue;
                            }
                            if(readarray[i].seq[j] == 'C' && ( left_lose[left_len - readarray[i].pos + j][2] >= 2 || (float)left_array[left_len - readarray[i].pos + j][2]/(float)left_array[left_len - readarray[i].pos + j][5] > 0.25f)) {
                                continue;
                            }
                            if(readarray[i].seq[j] == 'G' && ( left_lose[left_len - readarray[i].pos + j][3] >= 2 || (float)left_array[left_len - readarray[i].pos + j][3]/(float)left_array[left_len - readarray[i].pos + j][5] > 0.25f)) {
                                continue;
                            }
                            if (readarray[i].dir.equals("f")){
                                pos = j;
                                chr = left_consensus.charAt(left_len - readarray[i].pos + j);
                            } else if (readarray[i].dir.equals("r")) {
                                pos = readarray[i].seq.length-1-j;
                                chr = Node.rc(left_consensus.charAt(left_len - readarray[i].pos + j)+"").charAt(0);
                            }
                            
                            if (outcode_list.containsKey(id)){
                                StringBuffer sb = outcode_list.get(id);
                                if (pos >= sb.length()){
                                    for(int k=sb.length(); k<=pos; k++) {
                                        sb.append("X");
                                    }
                                    sb.setCharAt(pos, chr);
                                } else {
                                    if (sb.charAt(pos) == 'X') {
                                        sb.setCharAt(pos, chr);
                                    } 
                                }
                                outcode_list.put(id, sb);
                                reporter.incrCounter("Brush", "fix_char", 1);
                            } else {
                                StringBuffer sb = new StringBuffer();
                                for(int k=0; k <= pos; k++) {
                                    sb.append("X");
                                }
                                sb.setCharAt(pos, chr);
                                outcode_list.put(id, sb);
                                reporter.incrCounter("Brush", "fix_char", 1);
                            }
                            //\\\\\\\\\\\\\\
                        }
                    }
                }
                //\\ IDX array confirmation
                for(int j=readarray[i].pos; j < readarray[i].pos + IDX; j++) {
                    String id = readarray[i].id;
                    int pos = 0;
                    boolean confirm = false;
                    if (readarray[i].seq[j] == 'A' && IDX_lose[j-readarray[i].pos][0] >= 3 ) {
                        confirm = true;
                    }
                    if (readarray[i].seq[j] == 'T' && IDX_lose[j-readarray[i].pos][1] >= 3 ) {
                        confirm = true;
                    }
                    if (readarray[i].seq[j] == 'C' && IDX_lose[j-readarray[i].pos][2] >= 3 ) {
                        confirm = true;
                    }
                    if (readarray[i].seq[j] == 'G' && IDX_lose[j-readarray[i].pos][3] >= 3 ) {
                        confirm = true;
                    }
                    if (confirm ) {
                        boolean submit=true;
                        if (readarray[i].dir.equals("f") && readarray[i].int_qv[j]< 20){
                            pos = j;
                        } else if (readarray[i].dir.equals("r") && readarray[i].int_qv[readarray[i].seq.length-1-j] < 20) {
                            pos = (readarray[i].seq.length-1-j);
                        } else {
                            submit = false;
                        }
                        if (submit) {
                            if (outcode_list.containsKey(id)){
                                StringBuffer sb = outcode_list.get(id);
                                if (pos >= sb.length()){
                                    for(int k=sb.length(); k<=pos; k++) {
                                        sb.append("X");
                                    }
                                    sb.setCharAt(pos, 'N');
                                } else {
                                    sb.setCharAt(pos, 'N');
                                }
                                outcode_list.put(id, sb);
                                reporter.incrCounter("Brush", "confirm_char", 1);
                            } else {
                                StringBuffer sb = new StringBuffer();
                                for(int k=0; k <= pos; k++) {
                                    sb.append("X");
                                }
                                sb.setCharAt(pos, 'N');
                                outcode_list.put(id, sb);
                                reporter.incrCounter("Brush", "confirm_char", 1);
                            }
                        }
                        //\\\\\\\\\\\\\\\\\\\\\\\\\\
                    }
                }
                //\\ righ array
                for(int j=readarray[i].pos + IDX; j < readarray[i].seq.length; j++) {
                    String id = readarray[i].id;
                    int pos = 0;
                    char chr = 'X';
                    if (right_consensus.charAt(j-readarray[i].pos-IDX) == readarray[i].seq[j]){
                        // Comfirmation
                        boolean confirm = false;
                        if (readarray[i].seq[j] == 'A' && ( right_lose[j-readarray[i].pos-IDX][1] < 2 && right_lose[j-readarray[i].pos-IDX][2] < 2 && right_lose[j-readarray[i].pos-IDX][3] < 2) && right_lose[j-readarray[i].pos-IDX][0] >= 3 ) {
                            confirm = true;
                        }
                        if (readarray[i].seq[j] == 'T' && ( right_lose[j-readarray[i].pos-IDX][0] < 2 && right_lose[j-readarray[i].pos-IDX][2] < 2 && right_lose[j-readarray[i].pos-IDX][3] < 2) && right_lose[j-readarray[i].pos-IDX][1] >= 3 ) {
                            confirm = true;
                        }
                        if (readarray[i].seq[j] == 'C' && ( right_lose[j-readarray[i].pos-IDX][0] < 2 && right_lose[j-readarray[i].pos-IDX][1] < 2 && right_lose[j-readarray[i].pos-IDX][3] < 2) && right_lose[j-readarray[i].pos-IDX][2] >= 3 ) {
                            confirm = true;
                        }
                        if (readarray[i].seq[j] == 'G' && ( right_lose[j-readarray[i].pos-IDX][0] < 2 && right_lose[j-readarray[i].pos-IDX][1] < 2 && right_lose[j-readarray[i].pos-IDX][2] < 2) && right_lose[j-readarray[i].pos-IDX][3] >= 3 ) {
                            confirm = true;
                        }
                        
                        if (confirm ) {
                            boolean submit=true;
                            if (readarray[i].dir.equals("f") && readarray[i].int_qv[j] < 20){
                                pos = j;
                            } else if (readarray[i].dir.equals("r") && readarray[i].int_qv[readarray[i].seq.length-1-j] < 20) {
                                pos = readarray[i].seq.length-1-j;
                            } else {
                                submit = false;
                            }
                            if (submit) {
                                if (outcode_list.containsKey(id)){
                                    StringBuffer sb = outcode_list.get(id);
                                    if (pos >= sb.length()){
                                        for(int k=sb.length(); k<=pos; k++) {
                                            sb.append("X");
                                        }
                                        sb.setCharAt(pos, 'N');
                                    } else {
                                        sb.setCharAt(pos, 'N');
                                    }
                                    outcode_list.put(id, sb);
                                    reporter.incrCounter("Brush", "confirm_char", 1);
                                } else {
                                    StringBuffer sb = new StringBuffer();
                                    for(int k=0; k <= pos; k++) {
                                        sb.append("X");
                                    }
                                    sb.setCharAt(pos, 'N');
                                    outcode_list.put(id, sb);
                                    reporter.incrCounter("Brush", "confirm_char", 1);
                                }
                            }
                            //\\\\\\\\\\\\\\\\\\\\\\\\\\\\\
                        }
                    } else {
                        if (!(right_consensus.charAt(j-readarray[i].pos-IDX) == 'N')) {
                            //\\
                            if (readarray[i].seq[j] == 'A' && ( right_lose[j-readarray[i].pos-IDX][0] >= 2 || (float)right_array[j-readarray[i].pos-IDX][0]/(float)right_array[j-readarray[i].pos-IDX][5] > 0.25f) ) {
                                continue;
                            }
                            if (readarray[i].seq[j] == 'T' && ( right_lose[j-readarray[i].pos-IDX][1] >= 2 || (float)right_array[j-readarray[i].pos-IDX][1]/(float)right_array[j-readarray[i].pos-IDX][5] > 0.25f )) {
                                continue;
                            }
                            if (readarray[i].seq[j] == 'C' && ( right_lose[j-readarray[i].pos-IDX][2] >= 2 || (float)right_array[j-readarray[i].pos-IDX][2]/(float)right_array[j-readarray[i].pos-IDX][5] > 0.25f )) {
                                continue;
                            }
                            if (readarray[i].seq[j] == 'G' && ( right_lose[j-readarray[i].pos-IDX][3] >= 2 || (float)right_array[j-readarray[i].pos-IDX][3]/(float)right_array[j-readarray[i].pos-IDX][5] > 0.25f )) {
                                continue;
                            }
                            //\\
                            if (readarray[i].dir.equals("f")){
                                pos = j;
                                chr = right_consensus.charAt(j-readarray[i].pos-IDX);
                            } else if (readarray[i].dir.equals("r")) {
                                pos = (readarray[i].seq.length-1-j);
                                chr = Node.rc(right_consensus.charAt(j-readarray[i].pos-IDX)+"").charAt(0);
                            }
                            if (outcode_list.containsKey(id)){
                                StringBuffer sb = outcode_list.get(id);
                                if (pos >= sb.length()){
                                    for(int k=sb.length(); k<=pos; k++) {
                                        sb.append("X");
                                    }
                                    sb.setCharAt(pos, chr);
                                } else {
                                    if (sb.charAt(pos) == 'X') {
                                        sb.setCharAt(pos, chr);
                                    } 
                                }
                                outcode_list.put(id, sb);
                                reporter.incrCounter("Brush", "fix_char", 1);
                            } else {
                                StringBuffer sb = new StringBuffer();
                                for(int k=0; k <= pos; k++) {
                                    sb.append("X");
                                }
                                sb.setCharAt(pos, chr);
                                outcode_list.put(id, sb);
                                reporter.incrCounter("Brush", "fix_char", 1);
                            }
                            //\\\\\\\\\\\\\\\\\\\\\\
                        }
                    }
                }
            }
            boolean export = false;
            for(String read_id : outcode_list.keySet())
            {
                String msg="";
                msg = Node.str2code(outcode_list.get(read_id).toString());
                node.addConfirmations(read_id, msg);
                export = true;
            }
            if (export ) {
                output.collect(new Text(node.getNodeId()), new Text(node.toNodeMsg()));
            }
		}
	}



	public RunningJob run(String inputPath, String outputPath, int idx) throws Exception
	{
		sLogger.info("Tool name: FindError");
		sLogger.info(" - input: "  + inputPath);
		sLogger.info(" - output: " + outputPath);

		JobConf conf = new JobConf(FindError.class);
		conf.setJobName("FindError " + inputPath + " " + Config.K);
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

		conf.setMapperClass(FindErrorMapper.class);
		conf.setReducerClass(FindErrorReducer.class);

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

		run(inputPath, outputPath, 1);

		long endtime = System.currentTimeMillis();

		float diff = (float) (((float) (endtime - starttime)) / 1000.0);

		System.out.println("Runtime: " + diff + " s");

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new FindError(), args);
		System.exit(res);
	}
}

