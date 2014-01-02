package com.cdgore.ankus;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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
 * MahoutDotProductDistributedCache is a job for determining the inner product of input vectors
 * with vectors stored in the distributed cache
 */
public class MahoutDotProductDistributedCache extends Configured implements Tool {
//public class SubtractColumnMeans extends AbstractJob {

	public static final String VECTOR_CLASS = "DistributedRowMatrix.mahoutDotProductDistributedCache.vector.class";

	public MahoutDotProductDistributedCache() {
	}
	
	public MahoutDotProductDistributedCache(Configuration conf) {
		super();
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new MahoutDotProductDistributedCache(), args);
		System.exit(exitCode);
	}
	
	/**
	 * Job for calculating column-wise mean of a DistributedRowMatrix
	 * 
	 * @param arg0
	 * 		includes 3 Strings: inputPath, cachePath, and outputPath
	 * 
	 * @return job completion code (0 or 1)
	 */
	
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		
		Path inputPath = null;
		Path cachePath = null;
		Path outputPath = null;
		if (arg0.length == 3) {
			inputPath = new Path(arg0[0]);//getInputPath();
			cachePath = new Path(arg0[1]);
			outputPath = new Path(arg0[2]);//getOutputPath();
		} else {
			throw new IOException("Must specify input path, path to column means, and output path");
		}

		Job job = new Job(conf, "MahoutDotProductDistributedCache");
		job.setJarByClass(MahoutDotProductDistributedCache.class);

		FileInputFormat.addInputPath(job, inputPath);
		
		System.out.println("cachePath.toUri().toString(): " + cachePath.toUri().toString());

		System.out.println("Adding " + cachePath.toString() + " to distributed cache");
				
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(DotProductMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		DistributedCache.addCacheFile(cachePath.toUri(), job.getConfiguration());
		
		job.submit();
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Mapper for subtracting the column means from each row of a matrix
	 */
	public static class DotProductMapper extends Mapper<IntWritable, VectorWritable, Text, DoubleWritable> {
		private Map<String, Vector> classWeights = new HashMap<String, Vector>();

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
			System.out.println("TEST0");
			if (cacheFiles != null && cacheFiles.length > 0) {
				System.out.println("TEST1");
				for (Path cP : cacheFiles) {
					try {
						System.out.println("TEST15");
						System.out.println("cP.toString(): " + cP.toString());
						File cDirectory = new File(cP.toString());
//						File[] cFiles = cDirectory.listFiles();//new FilenameFilter() {
						File[] cFiles = cDirectory.listFiles(new FilenameFilter() {
							public boolean accept(File dir, String name) {
								return name.startsWith("part-");
							}
						});
						
						for (File cF : cFiles) {
							System.out.println("Loading: " + cF.toString());
							try {
								FileSystem fs = FileSystem.get(cF.toURI(), conf);
								SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(cF.toURI()), conf);
								Text key = (Text) reader.getKeyClass().newInstance();
								VectorWritable value = (VectorWritable) reader.getValueClass().newInstance();
								
								while (reader.next(key, value)) {
									System.out.println("TEST5");
									classWeights.put(key.toString(), (Vector)value.get());
								}
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					} catch (Exception e) {
						System.err.println(e.toString());
					}
				}
			}
		}
		
		@Override
		public void map(IntWritable r, VectorWritable v, Context context) throws IOException {
			try {
				for (Entry<String, Vector> w : classWeights.entrySet()) {
					context.write(new Text(String.valueOf(r.get())+"_"+w.getKey()), new DoubleWritable(v.get().dot(w.getValue())));
				}
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
