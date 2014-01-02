package com.cdgore.ankus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.ClassUtils;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileValueIterator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.function.Functions;

import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * SubtractColumnMeans is a job for subtracting the column means from
 * each row in a matrix
 */
public class SubtractColumnMeans extends Configured implements Tool {
//public class SubtractColumnMeans extends AbstractJob {

	public static final String VECTOR_CLASS = "DistributedRowMatrix.columnMeans.vector.class";

	public SubtractColumnMeans() {
	}
	
	public SubtractColumnMeans(Configuration conf) {
		super();
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new SubtractColumnMeans(), args);
		System.exit(exitCode);
	}
	
	/**
	 * Job for calculating column-wise mean of a DistributedRowMatrix
	 * 
	 * @param arg0
	 * 		includes 3 Strings: inputPath, meanPath, and outputPath
	 * 
	 * @return job completion code (0 or 1)
	 */
	
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		Path inputPath = null;
		Path meanPath = null;
		Path outputPath = null;
		if (arg0.length == 3) {
			inputPath = new Path(arg0[0]);//getInputPath();
			meanPath = new Path(arg0[1]);
			outputPath = new Path(arg0[2]);//getOutputPath();
		} else {
			throw new IOException("Must specify input path, path to column means, and output path");
		}

		Job job = new Job(conf, "SubtractColumnMeans");
		job.setJarByClass(SubtractColumnMeans.class);

		FileInputFormat.addInputPath(job, inputPath);
		
		System.out.println("meanPath.toUri().toString(): " + meanPath.toUri().toString());

		System.out.println("Adding " + meanPath.toString() + " to distributed cache");
				
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(SubtractColumnMeansMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		
		DistributedCache.addCacheFile(meanPath.toUri(), job.getConfiguration());
		
		job.submit();
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Mapper for subtracting the column means from each row of a matrix
	 */
	public static class SubtractColumnMeansMapper extends Mapper<LongWritable, VectorWritable, IntWritable, VectorWritable> {
		private Vector columnMeans = null;

		@Override
		protected void setup(Context context) {
			Path[] cacheFiles = null;
//			Path[] cacheArchives = null;
			Configuration conf = context.getConfiguration();
			try {
				cacheFiles = DistributedCache.getLocalCacheFiles(conf);
//				cacheArchives = DistributedCache.getLocalCacheArchives(conf);
				System.out.println("cacheFiles.length: " + cacheFiles.length);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (cacheFiles != null && cacheFiles.length > 0) {
				for (Path cP : cacheFiles) {
					FileSystem fs;
					try {
						fs = FileSystem.getLocal(conf);
						SequenceFile.Reader reader = new SequenceFile.Reader(fs, cP, conf);
						IntWritable key = (IntWritable) reader.getKeyClass().newInstance();
						VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
						
						while (reader.next(key, value)) {
							columnMeans = (Vector) value.get();
						}
					} catch (Exception e) {
						System.err.println(e.toString());
					}
				}
			}
		}
		
		@Override
		public void map(LongWritable r, VectorWritable v, Context context) throws IOException {
			try {
				Vector newV = v.get().minus(columnMeans);
				context.write(new IntWritable((int)r.get()), new VectorWritable(newV));
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
