/*
    Config.java
    2012 Ⓒ ReadStackCorrector, developed by Chien-Chih Chen (rocky@iis.sinica.edu.tw), 
    released under Apache License 2.0 (http://www.apache.org/licenses/LICENSE-2.0) 
    at: https://github.com/ice91/ReadStackCorrector

    The file is derived from Contrail Project which is developed by Michael Schatz, 
    Jeremy Chambers, Avijit Gupta, Rushil Gupta, David Kelley, Jeremy Lewi, 
    Deepak Nettem, Dan Sommer, Mihai Pop, Schatz Lab and Cold Spring Harbor Laboratory, 
    and is released under Apache License 2.0 at: 
    http://sourceforge.net/apps/mediawiki/contrail-bio/
*/

package Corrector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.mapred.JobConf;

public class Config {
    // important paths
	public static String hadoopReadPath = null;
	public static String hadoopBasePath = null;
	public static String hadoopTmpPath = null;
	public static String localBasePath = "work";

	// hadoop options
	public static int    HADOOP_MAPPERS    = 40;
	public static int    HADOOP_REDUCERS   = 40;
	public static int    HADOOP_LOCALNODES = 1000;
	public static long   HADOOP_TIMEOUT    = 0;
	public static String HADOOP_JAVAOPTS   = "-Xmx4000m";

	// Assembler options
	public static String STARTSTAGE = null;
	public static String STOPSTAGE = null;
	public static int QV_ASCII = 33;

    // restart options
	public static boolean validateonly = false;
	public static boolean forcego = false;
	public static int RESTART_INITIAL = 0;
	public static int RESTART_TIP = 0;
	public static int RESTART_TIP_REMAIN = 0;
	public static int RESTART_COMPRESS = 0;
	public static int RESTART_COMPRESS_REMAIN = 0;
	public static String SCREENING = "on";

    // initial node construction
	public static long K = -1;
	public static long READLEN = 36;
    
  
    // kmer status
	public static long LOW_KMER = 1;
	public static long UP_KMER = 500;

    // randomize threshold
	public static long RANDOM_PASS = 100;
	
	// stats
	public static String RUN_STATS = null;
	public static long  N50_TARGET = -1;
	public static String CONVERT_FA = null;

	public static void validateConfiguration()
	{
        int err = 0;
        if ((RUN_STATS == null) && (CONVERT_FA == null)) {
            if (hadoopBasePath == null) { err++; System.err.println("ERROR: -out is required"); }
            if (STARTSTAGE == null && hadoopReadPath == null) { err++; System.err.println("ERROR: -in is required"); }
        }
        if (err > 0) { System.exit(1); }
        if (!hadoopBasePath.endsWith("/")) { hadoopBasePath += "/"; }
		if (!localBasePath.endsWith("/"))  { localBasePath += "/"; }
        hadoopTmpPath = hadoopBasePath.substring(0, hadoopBasePath.length()-1) + ".tmp" + "/";
	}

    public static void initializeConfiguration(JobConf conf)
	{
		validateConfiguration();

        conf.setNumMapTasks(HADOOP_MAPPERS);
		conf.setNumReduceTasks(HADOOP_REDUCERS);
		conf.set("mapred.child.java.opts", HADOOP_JAVAOPTS);
		conf.set("mapred.task.timeout", Long.toString(HADOOP_TIMEOUT));
		conf.setLong("LOCALNODES", HADOOP_LOCALNODES);

		conf.setLong("RANDOM_PASS", RANDOM_PASS);
        
		conf.setLong("UP_KMER", UP_KMER);
        conf.setLong("LOW_KMER", LOW_KMER);
        conf.setLong("K", K);
        conf.setLong("READLENGTH", READLEN);
       
	}

    public static void printConfiguration()
	{
		validateConfiguration();

		//Main.msg("Contrail " + Contrail.VERSION + "\n");
		Main.msg("==================================================================================\n");
		Main.msg("Input: "         + hadoopReadPath + "\n");
		Main.msg("Workdir: "       + hadoopBasePath  + "\n");
		Main.msg("localBasePath: " + localBasePath + "\n");

		Main.msg("HADOOP_MAPPERS = "    + HADOOP_MAPPERS + "\n");
		Main.msg("HADOOP_REDUCERS = "   + HADOOP_REDUCERS + "\n");
		Main.msg("HADOOP_JAVA_OPTS = "  + HADOOP_JAVAOPTS + "\n");
		Main.msg("HADOOP_TIMEOUT = "    + HADOOP_TIMEOUT + "\n");
		Main.msg("HADOOP_LOCALNODES = " + HADOOP_LOCALNODES + "\n");

		if (STARTSTAGE != null)  { Main.msg("STARTSTAGE = " + STARTSTAGE + "\n"); }

		if (STOPSTAGE  != null) { Main.msg("STOPSTAGE = "  + STOPSTAGE + "\n");  }
        Main.msg("READLENGTH = "               + READLEN + "\n");
		Main.msg("K = "               + K + "\n");
        Main.msg("READ STACK UPPER BOUND = "    + UP_KMER + "\n");
        Main.msg("RANDOM PASS = "    + RANDOM_PASS + "\n");
        Main.msg("SCREENING PHASE = "    + SCREENING + "\n");
        
		//Main.msg("KMER LOW BOUND = "  + LOW_KMER + "\n");
		

		Main.msg("\n");

		if (validateonly && !forcego)
		{
			System.exit(0);
		}
	}

    public static void parseOptions(String [] args)
	{
		Options options = new Options();

        options.addOption(new Option("help",     "print this message"));
		options.addOption(new Option("h",        "print this message"));
		options.addOption(new Option("expert",   "show expert options"));
        
        // work directories
		options.addOption(OptionBuilder.withArgName("hadoopBasePath").hasArg().withDescription("Base Hadoop output directory [required]").create("out"));
		options.addOption(OptionBuilder.withArgName("hadoopReadPath").hasArg().withDescription("Hadoop read directory [required]").create("in"));
		options.addOption(OptionBuilder.withArgName("workdir").hasArg().withDescription("Local work directory (default: " + localBasePath + ")").create("work"));

        // hadoop options
		options.addOption(OptionBuilder.withArgName("numSlots").hasArg().withDescription("Number of machine slots to use (default: " + HADOOP_MAPPERS + ")").create("slots"));
		options.addOption(OptionBuilder.withArgName("childOpts").hasArg().withDescription("Child Java Options (default: " + HADOOP_JAVAOPTS + ")").create("javaopts"));
		options.addOption(OptionBuilder.withArgName("millisecs").hasArg().withDescription("Hadoop task timeout (default: " + HADOOP_TIMEOUT + ")").create("timeout"));

        // job restart
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Starting stage").create("start"));
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Stop stage").create("stop"));
		options.addOption(OptionBuilder.withArgName("stage").hasArg().withDescription("Screening stage").create("screening"));
        
		
        // initial graph
        options.addOption(OptionBuilder.withArgName("read length").hasArg().withDescription("Read Length ").create("readlen"));
      
        // randomize threshold
        options.addOption(OptionBuilder.withArgName("random pass").hasArg().withDescription("Random Pass ").create("random"));
        
        // kmer status
        options.addOption(OptionBuilder.withArgName("kmer upper bound").hasArg().withDescription("max kmer cov (default: " + UP_KMER ).create("kmerup"));
		options.addOption(OptionBuilder.withArgName("kmer lower bound").hasArg().withDescription("min kmer cov (default: " + LOW_KMER).create("kmerlow"));
     
		
		
        CommandLineParser parser = new GnuParser();

	    try
	    {
            CommandLine line = parser.parse( options, args );
            
            if (line.hasOption("help") || line.hasOption("h") || line.hasOption("expert"))
	        {
	        	System.out.print( "Usage: hadoop jar ReadStackCorrector.jar [-in dir] [-out dir] [-readlen readlen] [options]\n" +
	        			         "\n" +
	    		                 "General Options:\n" +
	    		                 "===============\n" +
	    		                 "  -out <outdir>       : Output directory for corrected reads [required]\n" +
	    		                 "  -in <indir>         : Directory with reads [required]\n" + 
	    		                 "  -work <workdir>     : Local directory for log files [" + localBasePath + "]\n" +
	    		                 "  -slots <slots>      : Hadoop Slots to use [" + HADOOP_MAPPERS + "]\n" +
	    		                 "  -screening <on/off> : Switch of Screening Phase\n" +
	        	                 "  -expert             : Show expert options\n");
	        	
	        	
	        	if (line.hasOption("expert"))
	        	{
	        	System.out.print(
                                 "  -kmerup <coverage>  : Read stack upper bound [500]\n" +
                                 //"  -kmerlow <coveage>  : Kmer coverage lower bound [1]\n" +
	    		                 "  -random <pass rate> : Randomized pass message [100]\n" + 
                                 "\n" +
	        			         "Hadoop Options:\n" +
	        			         "===============\n" +
	    		                 "  -javaopts <opts>    : Hadoop Java Opts [" + HADOOP_JAVAOPTS + "]\n" +
	    		                 "  -timeout <usec>     : Hadoop task timeout [" + HADOOP_TIMEOUT + "]\n" +
	        			         "\n" 
	        					);
	        	}
	        	
	        	System.exit(0);
	        }
            if (line.hasOption("out"))   { hadoopBasePath = line.getOptionValue("out");  }
            if (line.hasOption("in")) { hadoopReadPath = line.getOptionValue("in"); }
            if (line.hasOption("work"))  { localBasePath  = line.getOptionValue("work"); }
            if (line.hasOption("slots"))    { HADOOP_MAPPERS  = Integer.parseInt(line.getOptionValue("slots")); HADOOP_REDUCERS = HADOOP_MAPPERS; }
	        if (line.hasOption("nodes"))    { HADOOP_LOCALNODES      = Integer.parseInt(line.getOptionValue("nodes")); }
	        if (line.hasOption("javaopts")) { HADOOP_JAVAOPTS = line.getOptionValue("javaopts"); }
	        if (line.hasOption("timeout"))  { HADOOP_TIMEOUT  = Long.parseLong(line.getOptionValue("timeout")); }
            if (line.hasOption("readlen"))     { READLEN     = Long.parseLong(line.getOptionValue("readlen")); }
            if (line.hasOption("kmerup"))       { UP_KMER      = Long.parseLong(line.getOptionValue("kmerup")); }  
            if (line.hasOption("random"))       { RANDOM_PASS      = Long.parseLong(line.getOptionValue("random")); }  
            if (line.hasOption("start")) { STARTSTAGE = line.getOptionValue("start"); }
	        if (line.hasOption("stop"))  { STOPSTAGE  = line.getOptionValue("stop");  }
	        if (line.hasOption("screening"))  { SCREENING  = line.getOptionValue("screening");  }
	    }
	    catch( ParseException exp )
	    {
	        System.err.println( "Parsing failed.  Reason: " + exp.getMessage() );
	        System.exit(1);
	    }

    }

}
