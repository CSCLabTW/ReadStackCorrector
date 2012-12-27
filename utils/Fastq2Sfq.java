import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Fastq2Sfq {

	public static void main(String[] args) throws IOException {
		BufferedReader Readfile = new BufferedReader(new FileReader(args[0]));
    BufferedWriter Writefile = new BufferedWriter(new FileWriter(args[1]));
    String temp="";
    String name="";
    String seq="";
    String score="";
    long count =0;
    while(Readfile.ready()){
        name = Readfile.readLine(); // name
        if (name.charAt(0) == '@') {
            name = name.substring(1);
            count = count + 1;
        } else {
            break;
        }
        seq = Readfile.readLine(); // seq                   
        temp = Readfile.readLine(); // quality separator
        if (temp.charAt(0)== '+'){
            // do nothing
        } else {
            break;
        }
        score = Readfile.readLine(); 
        Writefile.write(Long.toHexString(count) + "\t" + seq + "\t" + score + "\n");
        //Writefile.write(name + "\t" + seq + "\t" + score + "\n");
    }
    Readfile.close();
    Writefile.close();
  }
                
}