package mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import utils.OWLRuleChainUtil;
import utils.Triple;
import utils.TripleTool;

public class GeneDiseaseMapper extends Mapper<Object, Text, Text, Text>{

	@Override
	protected void map(Object key, Text value,
			GeneDiseaseMapper.Context context)
			throws IOException, InterruptedException {
		Triple triple=TripleTool.parseLineToTriple(value.toString());
		//get the index of the predicate of the triple
		if(triple==null){
			return;
		}
		for(int i=0;i<2;i++){
		int pIndex=OWLRuleChainUtil.getIndexByPredicate(triple.getPredicate(),i);
		if(pIndex==-1)
			continue;
		if(pIndex%2==0){
			context.write(new Text(i+"&"+pIndex+"&"+triple.getObject()), value);
		}else{
			context.write(new Text(i+"&"+(pIndex-1)+"&"+triple.getSubject()), value);
		}
		}
	}
	

}