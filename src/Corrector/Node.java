/*
    Node.java
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

import java.io.*;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
/**
 *
 * @author rocky
 */
public class Node {
    public static final String NODEMSG           = "N";
    public static final String UPDATEMSG         = "U";
    public static final String CORRECTMSG       = "A";
	

    // Node msg field codes
	public static final String STR      = "s";
    public static final String QV      = "q";
	public static final String COVERAGE = "v";
    public static final String CONFIRMATION   = "i";
    public static final String CORRECTION   = "d";
    public static final String UNIQUE = "u";

    //\\// for find path
    public static enum Color {
        W, G, B, F
    };

    static String [] dnachars  = {"A", "C", "G", "T"};
    static String [] codechars = {"A", "C", "G", "T", "X", "N"};
    static String [] edgetypes = {"ff", "fr", "rf", "rr"};
    static String [] dirs      = {"f", "r"};


    static Map<String, String> str2dna_ = initializeSTR2DNA();
	static Map<String, String> dna2str_ = initializeDNA2STR();
    
    static Map<String, String> str2code_ = initializeSTR2CODE();
	static Map<String, String> code2str_ = initializeCODE2STR();

    // node members
    private String nodeid;
	private Map<String, List<String>> fields = new HashMap<String, List<String>>();

    // converts strings like A, GA, TAT, ACGT to compressed DNA codes (A,B,C,...,HA,HB)
	private static Map<String,String> initializeSTR2DNA()
	{
	   int num = 0;
	   int asciibase = 'A';

	   Map<String, String> retval = new HashMap<String, String>();

	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   retval.put(dnachars[xi],
				      Character.toString((char) (num + asciibase)));

		   num++;

		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   retval.put(dnachars[xi] + dnachars[yi],
					      Character.toString((char) (num + asciibase)));
			   num++;
		   }
	   }

	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   String m = retval.get(dnachars[xi] + dnachars[yi]);

			   for (int zi = 0; zi < dnachars.length; zi++)
			   {
				   retval.put(dnachars[xi]+dnachars[yi]+dnachars[zi],
						      m + retval.get(dnachars[zi]));

				   for (int wi = 0; wi < dnachars.length; wi++)
				   {
					   retval.put(dnachars[xi]+dnachars[yi]+dnachars[zi]+dnachars[wi],
							      m+retval.get(dnachars[zi]+dnachars[wi]));
				   }
			   }
		   }
	   }

	   return retval;
	}

	// converts single letter dna codes (A,B,C,D,E...) to strings (A,AA,GT,GA...)
	private static Map<String, String> initializeDNA2STR()
	{
	   int num = 0;
	   int asciibase = 65;

	   Map<String, String> retval = new HashMap<String, String>();

	   for (int xi = 0; xi < dnachars.length; xi++)
	   {
		   retval.put(Character.toString((char) (num + asciibase)),
				      dnachars[xi]);

		   num++;

		   for (int yi = 0; yi < dnachars.length; yi++)
		   {
			   retval.put(Character.toString((char) (num + asciibase)),
					      dnachars[xi] + dnachars[yi]);
			   num++;
		   }
	   }

	   /*
	   Set<String> keys = retval.keySet();
	   Iterator<String> it = keys.iterator();
	   while(it.hasNext())
	   {
		   String k = it.next();
		   String v = retval.get(k);

		   System.err.println(k + "\t" + v);
	   }
	   */

	   return retval;
	}

	// converts a tight encoding to a normal ascii string
	public static String dna2str(String dna)
	{
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < dna.length(); i++)
		{
			sb.append(dna2str_.get(dna.substring(i,i+1)));
		}

		return sb.toString();
	}

	public static String str2dna(String seq)
	{
		StringBuffer sb = new StringBuffer();

	    int l = seq.length();

	    int offset = 0;

	    while (offset < l)
	    {
	    	int r = l - offset;

	    	if (r >= 4)
	    	{
	    		sb.append(str2dna_.get(seq.substring(offset, offset+4)));
	    		offset += 4;
	    	}
	    	else
	    	{
	    		sb.append(str2dna_.get(seq.substring(offset, offset+r)));
	    		offset += r;
	    	}
	    }

        return sb.toString();
	}

    //\\\\\\\\\\\\\\\\\\\\\
	private static Map<String,String> initializeSTR2CODE()
	{
	   int num = 0;
	   int asciibase = 'A';

	   Map<String, String> retval = new HashMap<String, String>();

	   for (int xi = 0; xi < codechars.length; xi++)
	   {
		   retval.put(codechars[xi],
				      Character.toString((char) (num + asciibase)));

		   num++;

		   for (int yi = 0; yi < codechars.length; yi++)
		   {
			   retval.put(codechars[xi] + codechars[yi],
					      Character.toString((char) (num + asciibase)));
			   num++;
		   }
	   }

	   for (int xi = 0; xi < codechars.length; xi++)
	   {
		   for (int yi = 0; yi < codechars.length; yi++)
		   {
			   String m = retval.get(codechars[xi] + codechars[yi]);

			   for (int zi = 0; zi < codechars.length; zi++)
			   {
				   retval.put(codechars[xi]+codechars[yi]+codechars[zi],
						      m + retval.get(codechars[zi]));

				   for (int wi = 0; wi < codechars.length; wi++)
				   {
					   retval.put(codechars[xi]+codechars[yi]+codechars[zi]+codechars[wi],
							      m+retval.get(codechars[zi]+codechars[wi]));
                       //\\\\
                       for(int ui =0; ui < codechars.length; ui++){
                            retval.put(codechars[xi]+codechars[yi]+codechars[zi]+codechars[wi]+codechars[ui],
							           m+retval.get(codechars[zi]+codechars[wi])+retval.get(codechars[ui]));
                            for(int vi=0; vi < codechars.length; vi++){
                                 retval.put(codechars[xi]+codechars[yi]+codechars[zi]+codechars[wi]+codechars[ui]+codechars[vi],
							                m+retval.get(codechars[zi]+codechars[wi])+retval.get(codechars[ui]+codechars[vi]));
                            }
                       }
                       //\\\
				   }
			   }
		   }
	   }

	   return retval;
	}

	// converts single letter dna codes (A,B,C,D,E...) to strings (A,AA,GT,GA...)
	private static Map<String, String> initializeCODE2STR()
	{
	   int num = 0;
	   int asciibase = 65;

	   Map<String, String> retval = new HashMap<String, String>();

	   for (int xi = 0; xi < codechars.length; xi++)
	   {
		   retval.put(Character.toString((char) (num + asciibase)),
				      codechars[xi]);

		   num++;

		   for (int yi = 0; yi < codechars.length; yi++)
		   {
			   retval.put(Character.toString((char) (num + asciibase)),
					      codechars[xi] + codechars[yi]);
			   num++;
		   }
	   }

	   /*
	   Set<String> keys = retval.keySet();
	   Iterator<String> it = keys.iterator();
	   while(it.hasNext())
	   {
		   String k = it.next();
		   String v = retval.get(k);

		   System.err.println(k + "\t" + v);
	   }
	   */

	   return retval;
	}
    
    // converts a tight encoding to a normal ascii string
	public static String code2str(String dna)
	{
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < dna.length(); i++)
		{
			sb.append(code2str_.get(dna.substring(i,i+1)));
		}

		return sb.toString();
	}

	public static String str2code(String seq)
	{
		StringBuffer sb = new StringBuffer();

	    int l = seq.length();

	    int offset = 0;

	    while (offset < l)
	    {
	    	int r = l - offset;

	    	if (r >= 6)
	    	{
	    		sb.append(str2code_.get(seq.substring(offset, offset+6)));
	    		offset += 6;
	    	}
	    	else
	    	{
	    		sb.append(str2code_.get(seq.substring(offset, offset+r)));
	    		offset += r;
	    	}
	    }

        return sb.toString();
	}
   
    private List<String> getOrAddField(String field)
	{
		if (fields.containsKey(field))
		{
			return fields.get(field);
		}

		List<String> retval = new ArrayList<String>();
		fields.put(field, retval);

		return retval;
	}
    
    public void setCoverage(float cov)
	{
		List<String> l = getOrAddField(COVERAGE);
		l.clear();
		l.add(Float.toString(cov));
	}
    
    public boolean hasCustom(String key)
	{
		return fields.containsKey(key);
	}

	public void setCustom(String key, String value)
	{
		List<String> l = getOrAddField(key);
		l.clear();
		l.add(value);
	}

	public void addCustom(String key, String value)
	{
		List<String> l = getOrAddField(key);
		l.add(value);
	}

	public List<String> getCustom(String key)
	{
		return fields.get(key);
	}

    public void clearCustom(String key)
	{
		fields.remove(key);
	}
    
    public void addEdge(String et, String v)
	{
		List<String> l = getOrAddField(et);
		l.add(v);
	}

	public List<String> getEdges(String et) throws IOException
	{
		if (et.equals("ff") ||
		    et.equals("fr") ||
		    et.equals("rr") ||
		    et.equals("rf"))
		{
		  return fields.get(et);
		}

		throw new IOException("Unknown edge type: " + et);
	}

	public void setEdges(String et, List<String> edges)
	{
		if (edges == null || edges.size() == 0)
		{
			fields.remove(et);
		}
		else
		{
			fields.put(et, edges);
		}
	}

	public void clearEdges(String et)
	{
		fields.remove(et);
	}

    
    public boolean isUnique(){
        if (fields.containsKey(UNIQUE))
		{
			return fields.get(UNIQUE).get(0).equals("1");
		}

		return false;
    }

    public void setisUnique(boolean is){
        if (is)
		{
			List<String> l = getOrAddField(UNIQUE);
			l.clear();
			l.add("1");
		}
		else
		{
			fields.remove(UNIQUE);
		}
    }
  
    //Accessors
	public String str()
	{
		return dna2str(fields.get(STR).get(0));
	}
	
	public String str_raw()
	{
		return fields.get(STR).get(0);
	}
	
	public void setstr_raw(String rawstr)
	{
		List<String> l = getOrAddField(STR);
		l.clear();		
		l.add(rawstr);
	}
	
	public void setstr(String str)
	{
		List<String> l = getOrAddField(STR);
		l.clear();
		l.add(Node.str2dna(str));
	}
    
    public String Qscore()
	{
		return fields.get(QV).get(0);
	}
    
    public String Qscore_1()
	{
		return fields.get(QV).get(0).substring(1);
	}
	
	public void setQscore(String qv)
	{
		List<String> l = getOrAddField(QV);
		l.clear();		
		l.add("@" + qv);
	}
    
    public void clearQscore()
	{
		fields.remove(QV);
	}
	
	public int len()
	{
		return str().length();
	}
	
	public int degree(String dir)
	{
		int retval = 0;
		
		String fd = dir + "f";
		if (fields.containsKey(fd)) { retval += fields.get(fd).size(); }
		
		String rd = dir + "r";
		if (fields.containsKey(rd)) { retval += fields.get(rd).size(); }
		
		return retval;
	}
    
    public float cov()
	{
		return Float.parseFloat(fields.get(COVERAGE).get(0)); 
	}

    public Node(String n_id)
	{
		nodeid = n_id;
	}

	public Node()
	{

	}
     public void addCorrections(String node, String correction)
	{
		String msg = node + "|" + correction;

		List<String> l = getOrAddField(CORRECTION);
		l.add(msg);
	}
     
    public List<String> getCorrections()
	{
		if (fields.containsKey(CORRECTION))
		{
			return fields.get(CORRECTION);
		}

		return null;
	}

    public void clearCorrections()
	{
		fields.remove(CORRECTION);
	}
    
    public void addConfirmations(String node, String confirmation)
	{
		String msg = node + "|" + confirmation;

		List<String> l = getOrAddField(CONFIRMATION);
		l.add(msg);
	}
     
    public List<String> getConfirmations()
	{
		if (fields.containsKey(CONFIRMATION))
		{
			return fields.get(CONFIRMATION);
		}

		return null;
	}

    public void clearCnfirmations()
	{
		fields.remove(CONFIRMATION);
	}

    public String getNodeId() { return nodeid; }

	public void setNodeId(String nid) { nodeid = nid; }

    public String toNodeMsg()
	{
		return toNodeMsg(false);
	}

    public String toNodeMsg(boolean tagfirst)
	{
		StringBuilder sb = new StringBuilder();

		DecimalFormat df = new DecimalFormat("0.00");

		if (tagfirst)
		{
			sb.append(nodeid);
			sb.append("\t");
		}

		sb.append(NODEMSG);

		sb.append("\t*"); sb.append(STR);
		sb.append("\t"); sb.append(str_raw());
        
        if (fields.containsKey(QV))
		{
            sb.append("\t*"); sb.append(QV);
            sb.append("\t"); sb.append(Qscore());
        }

		sb.append("\t*"); sb.append(COVERAGE);
		sb.append("\t"); sb.append(df.format(cov()));

		for(String t : edgetypes)
		{
			if (fields.containsKey(t))
			{
				sb.append("\t*"); sb.append(t);

				for(String i : fields.get(t))
				{
					sb.append("\t"); sb.append(i);
				}
			}
		}

		char [] dirs = {'f', 'r'};


        if (fields.containsKey(UNIQUE))
        {
            sb.append("\t*"); sb.append(UNIQUE);
            sb.append("\t");  sb.append(fields.get(UNIQUE).get(0));
        }
        
        if (fields.containsKey(CORRECTION))
		{
			sb.append("\t*"); sb.append(CORRECTION);
			for(String t : fields.get(CORRECTION))
			{
				sb.append("\t"); sb.append(t);
			}
		}
        
        if (fields.containsKey(CONFIRMATION))
		{
			sb.append("\t*"); sb.append(CONFIRMATION);
			for(String t : fields.get(CONFIRMATION))
			{
				sb.append("\t"); sb.append(t);
			}
		}
		return sb.toString();
	}

    public void fromNodeMsg(String nodestr) throws IOException
	{
		fields.clear();

		String [] items = nodestr.split("\t");

		nodeid = items[0];
		parseNodeMsg(items, 1);
	}

	public void parseNodeMsg(String[] items, int offset) throws IOException
	{
		if (!items[offset].equals(NODEMSG))
		{
			throw new IOException("Unknown code: " + items[offset]);
		}

		List<String> l = null;

		offset++;

		while (offset < items.length)
		{
			if (items[offset].charAt(0) == '*')
			{
				String type = items[offset].substring(1);
				l = fields.get(type);

				if (l == null)
				{
					l = new ArrayList<String>();
					fields.put(type, l);
				}
			}
			else if (l != null)
			{
				l.add(items[offset]);
			}

			offset++;
		}
	}

    public void fromNodeMsg(String nodestr, Set<String> desired)
	{
		fields.clear();

		String [] items = nodestr.split("\t");
		List<String> l = null;

		// items[0] == nodeid
		// items[1] == NODEMSG

		for (int i = 2; i < items.length; i++)
		{
			if (items[i].charAt(0) == '*')
			{
				l = null;

				String type = items[i].substring(1);

				if (desired.contains(type))
				{
					l = fields.get(type);

					if (l == null)
					{
						l = new ArrayList<String>();
						fields.put(type, l);
					}
				}
			}
			else if (l != null)
			{
				l.add(items[i]);
			}
		}
	}

    public static String flip_dir(String dir) throws IOException
	{
		if (dir.equals("f")) { return "r"; }
		if (dir.equals("r")) { return "f"; }

		throw new IOException("Unknown dir type: " + dir);
	}

    public static String flip_link(String link) throws IOException
	{
		if (link.equals("ff")) { return "rr"; }
		if (link.equals("fr")) { return "fr"; }
		if (link.equals("rf")) { return "rf"; }
		if (link.equals("rr")) { return "ff"; }
		throw new IOException("Unknown link type: " + link);
	}

    public static String rc(String seq) //reverse complement
	{
		StringBuilder sb = new StringBuilder();

		for (int i = seq.length() - 1; i >= 0; i--)
		{
			if      (seq.charAt(i) == 'A') { sb.append('T'); }
			else if (seq.charAt(i) == 'T') { sb.append('A'); }
			else if (seq.charAt(i) == 'C') { sb.append('G'); }
			else if (seq.charAt(i) == 'G') { sb.append('C'); }
		}

		return sb.toString();
	}

    public static void main(String[] args) throws Exception
	{
        Node node = new Node();
     
        node.setstr_raw("ACGT");
        node.setCoverage(1);
        String str0 = "LHEMHCOIMEHRGIDIDCnullIHNA";
        String str1 = "EHIDHIRGIOHNGMHMIHRNHRLHDBMIIRIGMNIGQLIDEHOGHCNHTINTGMMEIDILCCLBJINJSHGHEMIDIDONSTGICIREIGRTID";
        String str2 = "DHIRGIOHNGMHMIHRNHRLHDBMIIRIGMNIGQLIDEHOGHCNHTINTGMMEIDILCCLBJINJSHGHEMIDIDONSTGICIREIGRTI";
        String words = "IIIIIIIIIIIIGIIGIIIHIIIIIHHIHHHFIIIIIIHIGIHGBGGEHHCHEEGEEEEE?BEBC@B@@D@B?@@######################!!!!";
        String reverse = new StringBuffer(words).reverse().toString();
        //System.out.println("SRR081522.30648655/1".replaceAll(".", "_").toString());
        System.out.println(words);
        System.out.println(reverse);
        System.out.println(Node.dna2str(str0));
        System.out.println(Node.dna2str(str1));
        System.out.println(Node.dna2str(str2));
        System.out.println(node.toNodeMsg());
        System.out.println(Node.str2code("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANAAAAAAAAAAANAAAAAAAAAAAAAAN"));
        System.out.println(Node.code2str("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbibbbbbibbbbbbc"));
        String tmp = "";
        for(int i=0; i < 3; i++) {
            tmp = tmp + "X";
        }
        StringBuffer sb = new StringBuffer(tmp);
        System.out.println("[" + sb.toString()+ "]");
        sb.setCharAt(2, 'A');
        System.out.println(sb.toString());
        
        //System.out.println("ATCGATCG".substring(3));
    }
}
