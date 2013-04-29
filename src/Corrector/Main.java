/*
    Main.java
    2012 Ⓒ ReadStackCorrector, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/ReadStackCorrector
*/

package Corrector;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Appender;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.TTCCLayout;
import org.apache.log4j.helpers.DateLayout;


//import org.jfree.data.general.DefaultPieDataset;



public class Main extends Configured implements Tool
{
    private static DecimalFormat df = new DecimalFormat("0.00");
	private static FileOutputStream logfile;
	private static PrintStream logstream;
	
	JobConf baseconf = new JobConf(Main.class);

    static String loadreads = "00-loadreads";
    static String precorrect = "01-precorrect";
    static String finderror = "02-finderror";
    static String screening = "03-screening";
    static String stopwords = "stopwords";
    static String sfa = "04-sfa";
    

    // Message Management
	///////////////////////////////////////////////////////////////////////////

	long GLOBALNUMSTEPS = 0;
	long JOBSTARTTIME = 0;
	public void start(String desc)
	{
		msg(desc + ":\t");
		JOBSTARTTIME = System.currentTimeMillis();
		GLOBALNUMSTEPS++;
	}

	public void end(RunningJob job) throws IOException
	{
		long endtime = System.currentTimeMillis();
		long diff = (endtime - JOBSTARTTIME) / 1000;

		msg(job.getJobID() + " " + diff + " s");

		if (!job.isSuccessful())
		{
			System.out.println("Job was not successful");
			System.exit(1);
		}
	}

	public static void msg(String msg)
	{
		logstream.print(msg);
		System.out.print(msg);
	}

	public long counter(RunningJob job, String tag) throws IOException
	{
		return job.getCounters().findCounter("Brush", tag).getValue();
	}

    // Stage Management
	///////////////////////////////////////////////////////////////////////////

	boolean RUNSTAGE = false;
	private String CURRENTSTAGE;

	public boolean runStage(String stage)
	{
		CURRENTSTAGE = stage;

		if (Config.STARTSTAGE == null || Config.STARTSTAGE.equals(stage))
		{
			RUNSTAGE = true;
		}

		return RUNSTAGE;
	}

	public void checkDone()
	{
		if (Config.STOPSTAGE != null && Config.STOPSTAGE.equals(CURRENTSTAGE))
		{
			RUNSTAGE = false;
			msg("Stopping after " + Config.STOPSTAGE + "\n");
			System.exit(0);
		}
	}

    // File Management
	///////////////////////////////////////////////////////////////////////////

	public void cleanup(String path) throws IOException
	{
		FileSystem.get(baseconf).delete(new Path(path), true);
	}

	public void save_result(String base, String opath, String npath) throws IOException
	{
		//System.err.println("Renaming " + base + opath + " to " + base + npath);

		msg("Save result to " + npath + "\n\n");

		FileSystem.get(baseconf).delete(new Path(base+npath), true);
		FileSystem.get(baseconf).rename(new Path(base+opath), new Path(base+npath));
	}

    // convertFasta
	///////////////////////////////////////////////////////////////////////////

	public void convertFasta(String basePath, String graphdir, String fastadir) throws Exception
	{
        Graph2Fasta g2f = new Graph2Fasta();
		start("convertFasta " + graphdir);
		RunningJob job = g2f.run(basePath + graphdir, fastadir.substring(0,fastadir.length()-1) +"_file");
        //FileSystem.get(baseconf).delete(new Path(basePath), true);
        end(job);
        msg("\n");
	}

    // convertFasta
	///////////////////////////////////////////////////////////////////////////

	public void convertSfa(String basePath, String graphdir, String sfadir) throws Exception
	{
        Graph2Sfa g2s = new Graph2Sfa();
		start("\nconvertsfa " + graphdir + " > " + sfadir);
		RunningJob job = g2s.run(basePath + graphdir, sfadir);
		end(job);
		long nodes = counter(job, "nodes");
		msg ("  " + nodes + " converted\n");
	}
    
    // preprocess
	///////////////////////////////////////////////////////////////////////////
    public void loadreads(String inputPath, String basePath, String loadreads, String stopword) throws Exception
	{
		RunningJob job;
        //long trans_edge = 0;
        start("\n  Load Reads");
        PreProcessReads ppr = new PreProcessReads();
        job = ppr.run(inputPath, basePath + loadreads);
        end(job);
        long nodecnt      = counter(job, "nodecount");
        long reads_goodbp = counter(job, "reads_goodbp");
        long reads_good   = counter(job, "reads_good");
        long reads_short  = counter(job, "reads_short");
        long reads_skip   = counter(job, "reads_skipped");
        long invalid_line = counter(job, "input_lines_invalid");
        long reads_all = reads_good + reads_short + reads_skip;
        if (reads_good == 0)
        {
            throw new IOException("No good reads");
        }
        String frac_reads = df.format(100*reads_good/reads_all);
        msg( " [" + reads_good +" (" + frac_reads + "%) good reads, " + reads_goodbp + " bp]");
        long Q0 = counter(job, "Q0");
        long Q0_Q10   = counter(job, "Q0_Q10");
        long Q10_Q20  = counter(job, "Q10_Q20");
        long Q20_Q30  = counter(job, "Q20_Q30");
        long Q30_Q40  = counter(job, "Q30_Q40");
        long Q40   = counter(job, "Q40");
        msg( " [" + Q0 + " base(<Q0), " + Q0_Q10 + " base(Q0-Q10), " + Q10_Q20 + " base(Q10-Q20), " + Q20_Q30 + " base(Q20-Q30), " + Q30_Q40 + " base(Q30-Q40), " + Q40 + " base(>Q40)");
        msg("\n");
        start("\n  Build High Frequency Kmer List");
        BuildHighKmerList bhk = new BuildHighKmerList();
        job = bhk.run(basePath + loadreads, basePath + stopword);
        end(job);
        long hkmer = counter(job, "hkmer");
        msg("H_kmer: " + hkmer);
        msg("\n");
       
	}
    
    public void trimreads(String inputPath, String outdir) throws Exception
    {
    	RunningJob job;
        //long trans_edge = 0;
        start("\n  Trim Reads");
        trimSeq2Fastq t2f = new trimSeq2Fastq();
        job = t2f.run(inputPath, outdir.substring(0,outdir.length()-1) +"_trim");
        end(job);
        msg("\n");
    }
    
   
    public void PreCorrection(String basePath, String input, String output, int idx_len, String hkmerlist) throws Exception{
    	RunningJob job;
    	msg("\nPreCorrect:");
        start("\n  PreCorrect ");
        PreCorrect pc = new PreCorrect();
        job = pc.run(basePath + input, basePath + input + ".msg", idx_len, basePath + hkmerlist);
        long hkmer = counter(job, "hkmer");
        long fix_char = counter(job, "fix_char");
        msg(" " +  hkmer + " HKmer_skip  " + fix_char + " fix_chars \n");
        end(job);
        start("\n  PCorrection ");
        PCorrection pcorr = new PCorrection();
        job = pcorr.run(basePath + input + "," + basePath + input + ".msg", basePath + output);
        fix_char = counter(job, "fix_char");
        msg("  " + fix_char + " fix_chars \n");
        end(job);
        msg("\n");
    }
    
    public void ErrorCorrection(/*String inputPath,*/ String basePath, String input, String output, int idx_len, String hkmerlist) throws Exception
	{
        RunningJob job;
        /*start("\n  PreCorrect ");
        PreCorrect pc = new PreCorrect();
        job = pc.run(basePath + preprocess, basePath + preprocess + ".msg", idx_len);
        long fix_char = counter(job, "fix_char");
        msg("  " + fix_char + " fix_chars \n");
        end(job);
        start("\n  PCorrection ");
        PCorrection pcorr = new PCorrection();
        job = pcorr.run(basePath + preprocess + "," + basePath + preprocess + ".msg", basePath + preprocess + ".pre");
        fix_char = counter(job, "fix_char");
        msg("  " + fix_char + " fix_chars \n");
        end(job);*/
        //msg("\nError Correction:");
        msg("\nFindError:");
        String current = input;
        long fix_char = 1;
        long hkmer = 0;
        long round = 0;
        while (fix_char > 0 && round < 2)
        {
            round++;
            start("\n  FindError ");
            FindError fe = new FindError();
            job = fe.run(basePath + current, basePath + current + ".fe", idx_len, basePath + hkmerlist);
            fix_char = counter(job, "fix_char");
            hkmer = counter(job, "hkmer");
            long confirm_char = counter(job, "confirm_char");
            msg(" " +  hkmer + " HKmer_skip  " + confirm_char + " confirm_chars  " + fix_char + " fix_chars \n");
            end(job);  
            if (round > 1 && fix_char == 0) {
                break;
            }
           
            start("\n  Correction ");
            Correction corr = new Correction();
            job = corr.run(basePath + current + "," + basePath + current + ".fe", basePath + output + "." + round);
            fix_char = counter(job, "fix_char");
            confirm_char = counter(job, "confirms");
            msg(" " + confirm_char + " confirms " + fix_char + " fix_chars \n");
            end(job);
            current = output + "." + round;
        }
        msg("\n");
        save_result(basePath, current, output);
        
        
        //\\ trusted k-mer
        //String current = error + ".2";
        /*KmerFrequencyOfReads kfr = new KmerFrequencyOfReads();
        IdentifyTrustedReads itr = new IdentifyTrustedReads();
        TagTrustedReads tagr = new TagTrustedReads();
        start("\n  Kmer Frequency of Reads");
        job = kfr.run(basePath + current, basePath + error + ".kfr");
        end(job);
        start("\n  Identify Trusted Reads");
        job = itr.run(basePath + error + ".kfr", basePath + error + ".itr", 1);
        end(job);
        start("\n  Tag Trusted Reads");
        job = tagr.run(basePath + current + "," + basePath + error + ".itr", basePath + error);
        long failed_reads = counter(job, "failed_reads");
        msg("  " + failed_reads + " failed_reads \n");
        end(job);*/
        //\\\\\\\\\\\\\\\\\\
        //save_result(basePath, current, error);
        
    }
    
    public void Screening(/*String inputPath,*/ String basePath, String input, String output, int idx_len) throws Exception
	{
        RunningJob job;
        //\\ trusted k-mer
        //String current = error + ".2";
        KmerFrequencyOfReads kfr = new KmerFrequencyOfReads();
        IdentifyTrustedReads itr = new IdentifyTrustedReads();
        TagTrustedReads tagr = new TagTrustedReads();
        msg("\nScreening:");
        start("\n  Kmer Frequency of Reads");
        job = kfr.run(basePath + input, basePath + output + ".kfr");
        end(job);
        start("\n  Identify Trusted Reads");
        job = itr.run(basePath + output + ".kfr", basePath + output + ".itr", 1);
        end(job);
        start("\n  Tag Trusted Reads");
        job = tagr.run(basePath + input + "," + basePath + output + ".itr", basePath + output);
        long failed_reads = counter(job, "failed_reads");
        msg("  " + failed_reads + " failed_reads \n");
        end(job);
        //\\\\\\\\\\\\\\\\\\
        //save_result(basePath, current, error);
        
    }
   
    
    // Run an entire assembly
	///////////////////////////////////////////////////////////////////////////
    public int run(String[] args) throws Exception
	{
        Config.parseOptions(args);
	    Config.validateConfiguration();

		// Setup to use a file appender
	    BasicConfigurator.resetConfiguration();

		TTCCLayout lay = new TTCCLayout();
		lay.setDateFormat("yyyy-mm-dd HH:mm:ss.SSS");

	    FileAppender fa = new FileAppender(lay, Config.localBasePath+"error.details.log", true);
	    fa.setName("File Appender");
	    fa.setThreshold(Level.INFO);
	    BasicConfigurator.configure(fa);

	    logfile = new FileOutputStream(Config.localBasePath+"error.log", true);
	    logstream = new PrintStream(logfile);

		Config.printConfiguration();

		// Time stamp
		DateFormat dfm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		msg("== Starting time " + dfm.format(new Date()) + "\n");
		long globalstarttime = System.currentTimeMillis();
        long ECstarttime=0;
        long ECendtime=0;
        long FPstarttime=0;
        long FPendtime=0;
        
		
        if (runStage("loadreads"))
        {
            ECstarttime = System.currentTimeMillis();
            loadreads(Config.hadoopReadPath, Config.hadoopTmpPath, loadreads, stopwords);
            trimreads(Config.hadoopReadPath, Config.hadoopBasePath);
            checkDone();
        }

        int round=1;
        String error_out=loadreads;
        
        if (runStage("precorrect")){
            PreCorrection(Config.hadoopTmpPath, loadreads, precorrect, 24, stopwords);
        } 
        
        if (runStage("finderror")){
            ErrorCorrection(Config.hadoopTmpPath, precorrect, finderror, 24, stopwords);
        }
        
        if (runStage("convertFasta"))
        {
            convertFasta(Config.hadoopTmpPath, finderror, Config.hadoopBasePath);
            checkDone();
        }
        
        if (runStage("screening")){
            Screening(Config.hadoopTmpPath, finderror, screening, 24);
        } 

        if (runStage("convertSfa"))
        {
            convertSfa(Config.hadoopTmpPath, screening, Config.hadoopBasePath);
            ECendtime = System.currentTimeMillis();
            checkDone();
        }

       
		

        // Final timestamp
		long globalendtime = System.currentTimeMillis();
		long globalduration = (globalendtime - globalstarttime)/1000;
        long ecduration = (ECendtime - ECstarttime)/1000;

        long fpduration = (FPendtime - FPstarttime)/1000;
		msg("== Ending time " + dfm.format(new Date()) + "\n");
		msg("== Duration: " + globalduration + " s, " + GLOBALNUMSTEPS + " total steps\n");
        msg(ecduration + " s, Error Correction\n");
		return 0;
	}

    public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new Main(), args);
		System.exit(res);
	}
}
