package utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class OWLRuleChainUtil {
	//private static String LOCALFILE="/user/ljx/rulechain";
	//private static String LOCALHDFS="hdfs://localhost:9000";
	
	/*public static String CLUSTERFILE0="/user/chenxi/rulechain1";
	public static String CLUSTERFILE1="/user/chenxi/rulechain2";
	public static String CLUSTERFILE2="/user/chenxi/rulechain3";
	public static String CLUSTERHDFS="hdfs://192.168.0.208:9000";*/
	public static String CLUSTERFILE0="/user/root/rulechain1";
	public static String CLUSTERFILE1="/user/root/rulechain2";
	//public static String CLUSTERFILE2="/user/root/rulechain3";
	public static String CLUSTERHDFS="hdfs://192.168.0.246:9000";
	
	public static String hdfsAddr=CLUSTERHDFS;
	public static String rulechainFileAddr0=CLUSTERFILE0;
	public static String rulechainFileAddr1=CLUSTERFILE1;
//	public static String rulechainFileAddr2=CLUSTERFILE2;
//	public static String FileAdd[]={rulechainFileAddr0,rulechainFileAddr1,rulechainFileAddr2};
	public static String FileAdd[]={rulechainFileAddr0,rulechainFileAddr1};
	// store the rule chain
	
	
	public static LinkedList<String> ruleChainLst0 = new LinkedList<String>();
	public static LinkedList<String> ruleChainLst1 = new LinkedList<String>();
//	public static LinkedList<String> ruleChainLst2 = new LinkedList<String>();
	public static LinkedList<LinkedList> list = new LinkedList<LinkedList>();
//	private static LinkedList<List> ruleChain=new LinkedList<List>();

	
	// tool class to read and write file in HDFS
	public static OWLHDFSUtil owlFSTool = new OWLHDFSUtil(hdfsAddr);

	// record the output result for deleting duplicates
	public static List<String> proLst = new ArrayList<String>();
	
	static{	
		/*
		ruleChainLst.add("http://purl.org/net/tcm/tcm.lifescience.ntu.edu.tw/treatment");
		ruleChainLst.add("http://www.w3.org/2002/07/owl#sameAs");
		ruleChainLst.add("http://www4.wiwiss.fu-berlin.de/diseasome/resource/diseasome/possibleDrug");
		ruleChainLst.add("http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/target");
		ruleChainLst.add("http://www4.wiwiss.fu-berlin.de/drugbank/resource/drugbank/swissprotId");
		ruleChainLst.add("http://purl.uniprot.org/core/classifiedWith");
		//ruleChainLst.add("http://www.w3.org/2000/01/rdf-schema#label");
		ruleChainLst.add("http://www.ccnt.org/symbol");
		*/
		
		
		list.add(ruleChainLst0);
		list.add(ruleChainLst1);
		//list.add(ruleChainLst2);
		for(int i=0;i<list.size();i++)
		{
		@SuppressWarnings("unchecked")
		LinkedList<String> tmplist = (LinkedList<String>) list.get(i);
		tmplist.clear();
		BufferedReader br=owlFSTool.readFile(FileAdd[i]);
		String tmp="";
		try {
			while((tmp=br.readLine())!=null)
				tmplist.add(tmp);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		tmp=null;
		br=null;	
	}
		}
	
	/***
	 * get the predicate index pindex[] . used by mappers
	 * @param pre
	 * @return index
	 */
	@SuppressWarnings("null")
	public static int[] getIndexByPredicate(String pre){
//		System.out.println("key:"+pre);
	int[] pindex = null;
	for(int i=0;i<list.size();i++){
	pindex[i]=ruleChainLst0.indexOf(pre);
	}		
	return pindex;
	}

	/***
	 * get the single predicate index. used in reducer
	 * @param pre, rule index
	 * @return index
	 */
	public static int getIndexByPredicate(String pre,int rule){
//		System.out.println("key:"+pre);
	//for(int i=0;i<2;i++){
			return list.get(rule).indexOf(pre);
	}
	
	
	/***
	 *  get the number of rules
	 *  @param rule index
	 * @return the rule chain length
	 */
	public static int getPredicateNum(int rule) {
		return list.get(rule).size();	
	}

	/***
	 *  update rule chain, merge the two ajacent rules
	 *  @param	rule index
	 * @return the new chain length
	 */
	/*public static int updatePredicateArr(int rule) {
		for(int i=0;i<list.size();i++)
		{
		@SuppressWarnings("unchecked")
		LinkedList<String> tmplist = (LinkedList<String>) list.get(i);
		LinkedList<String> newlist = new LinkedList<String>();
		newlist.clear();
		int size=tmplist.size();
		if(size==1)
			continue;
		for(int j=0;j<size;j=i+2){
			// get the odd rule
			String p1=tmplist.get(j);
			// add the last one if the rule size is odd
			if(j+1>=size){
				newlist.add(p1);
				break;
			}
			String p2=tmplist.get(i+1);
			newlist.add(p1+"$$"+p2);
		}
		list.get(i).removeAll(tmplist);
		list.get(i)=tmpLst;
//		System.out.println(ruleChainLst.size());
		return ruleChainLst0.size();
		
		}
	
*/
	
	
	
	/***
	 *  read rule chain file when beginning the next iteration
	 */
	public static void RefreshRuleList(){
		for(int i=0;i<list.size();i++)
		{
		@SuppressWarnings("unchecked")
		LinkedList<String> tmplist = (LinkedList<String>) list.get(i);
		tmplist.clear();
		BufferedReader br=owlFSTool.readFile(FileAdd[i]);
		String tmp="";
		try {
			while((tmp=br.readLine())!=null)
				tmplist.add(tmp);
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		tmp=null;
		br=null;	
	}
	}
	
	/***
	 * update rule chain file 
	 */
	public static void updatePredicateArrIntoFile() {
		for(int i=0;i<list.size();i++){	
			LinkedList<String> tmplist = (LinkedList<String>) list.get(i);
		int size=tmplist.size();
		// if the length of the rulechainlist is 1, we do not update the list;
		if(size==1)
			continue;
		String writeCon="";
		for(int j=0;j<size;j=j+2){
			String p1=tmplist.get(j);
			if(j+1>=size){
				writeCon+=p1+"\n";
				break;
			}
			String p2=tmplist.get(j+1);
			writeCon+=p1+"$$"+p2+"\n";
		}
		owlFSTool.writeFile(FileAdd[i], writeCon);
	}
	}
	/***
	 * get the predicate according to the index
	 * @param i,rule
	 * @return
	 */
	public static String getPredicateByIndex(int i,int rule) {
		return (String) list.get(rule).get(i);
	}

	public static void showRuleChain(){
		for(int i=0;i<list.size();i++){
			System.out.println("rule chains "+i+" is:");
			for(int j=0;j<list.get(i).size();j++)
				System.out.print(list.get(i).get(j)+"\t");
			System.out.println();
		}
	}
	
}
