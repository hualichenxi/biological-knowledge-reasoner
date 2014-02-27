package jobs;

import java.io.IOException;

import mapper.GeneDiseaseMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import reducer.GeneDiseaseReducer;
import utils.OWLRuleChainUtil;


public class reasoner {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		/*if (otherArgs.length != 2) {
			System.err.println("Usage: GeneDiseaseReasoner <in> <out>");
			System.exit(2);
		}*/
		
		long startTime = System.currentTimeMillis();
		
		Job job = null;
		int iterationNum=0;
		//int predicateNum=OWLRuleChainUtil.getPredicateNum();
		int predicateNum=7;
		while(predicateNum>1){
			job = new Job(conf, "owl rule chain!");
			System.out.println("start the "+(int)(iterationNum+1)+"th iteration");
			job.setJarByClass(reasoner.class);
			job.setMapperClass(GeneDiseaseMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setReducerClass(GeneDiseaseReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			/*FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path("outputowl"+iterationNum++));
			job.waitForCompletion(true);
			System.exit(2);
			*/
			
			//job.setNumReduceTasks(3);
			
			String inputPath="";
			//when it is iterated by the first time, input file is our default hdfs input
			if(iterationNum==0){
				inputPath=otherArgs[0];
				System.out.println("first input file path is "+otherArgs[0]);
			}else{
				//when the first iteration is over, next input file is outputowl0
				//inputPath=OWLRuleChainUtil.hdfsAddr+"/user/chenxi/output/"+"outputowl"+(ierationNum-1);
				inputPath="outputowl"+(iterationNum-1);
				System.out.println("read intermediate file in "+inputPath);
			}
			FileInputFormat.setInputPaths(job, new Path(inputPath));

			String outputPath="";
			//when the last iteration is over, output the result to args[2]
			if(predicateNum==2){
				outputPath=otherArgs[1];
				System.out.println("last output is "+otherArgs[1]);
			}else{
				// otherwise,output the intermediate result
				//outputPath=OWLRuleChainUtil.hdfsAddr+"/user/chenxi/output/"+"outputowl"+iterationNum++;
				outputPath="outputowl"+iterationNum++;
				System.out.println("intermediate output is "+outputPath);
			}
			
			FileSystem.get(job.getConfiguration()).delete(new Path(outputPath), true);
			
			FileOutputFormat.setOutputPath(job, new Path(outputPath));
			job.waitForCompletion(true);

			OWLRuleChainUtil.updatePredicateArrIntoFile();
			predicateNum = (predicateNum+1)/2;
			OWLRuleChainUtil.RefreshRuleList();
			System.out.println("finish the "+(iterationNum+1)+"th iteration");
			System.out.println("this is the rule chains: ");
			OWLRuleChainUtil.showRuleChain();
			//System.exit(2);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("use time:" + (endTime - startTime));

	}

}

