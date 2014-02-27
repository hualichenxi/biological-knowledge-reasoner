package preprocess;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
public class trigToTriple {

	public static void main(String[] args) {
				File file = new File("/home/chenxi/dailymed.trig");
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(file));
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				File outputfile = new File("/home/chenxi/daily_new");
				BufferedWriter bw = null;
				try {
					bw = new BufferedWriter(new FileWriter(outputfile));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String line = "";
				try {
					while((line=reader.readLine())!=null){
						String[] elements = line.split(" ");
						System.out.println(elements.length);
						if(elements.length!=5)
							continue;
						elements[0]=elements[1].substring(1, elements[1].length()-1);
						if(elements[2].substring(0,1).equals("<"))
							elements[1]=elements[2].substring(1, elements[2].length()-1);
						else
							elements[1]=elements[2];
						if(elements[3].substring(0,1).equals("<"))
							elements[2]=elements[3].substring(1, elements[3].length()-1);
						else
							elements[2]=elements[3];
						
/*						System.out.println(elements[0]);
						System.out.println(elements[1]);
						System.out.println(elements[2]);*/
						bw.write(elements[0]+"	"+elements[1]+"	"+elements[2]+"	");
						bw.newLine();
						}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
					try {
						bw.flush();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						bw.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
					try {
						reader.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			}

		}


