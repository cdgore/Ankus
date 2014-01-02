package com.cdgore.ankus;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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

/**
 * MahoutColumnMeans is a job for calculating the column-wise mean of a
 * DistributedRowMatrix. This job can be accessed using
 * DistributedRowMatrix.columnMeans()
 */
public class MahoutColumnMeans extends Configured implements Tool {
//public class MahoutColumnMeans extends AbstractJob {

	public static final String VECTOR_CLASS = "DistributedRowMatrix.columnMeans.vector.class";

	public MahoutColumnMeans() {
	}
	
	public MahoutColumnMeans(Configuration conf) {
		super();
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int exitCode = ToolRunner.run(new MahoutColumnMeans(), args);
		System.exit(exitCode);
	}

//	public static Vector run(Configuration conf, Path inputPath, Path outputPath)
//			throws IOException {
//		return run(conf, inputPath, outputPath, null);
//	}

	/**
	 * Job for calculating column-wise mean of a DistributedRowMatrix
	 * 
	 * @param initialConf
	 * @param inputPath
	 *            path to DistributedRowMatrix input
	 * @param outputPath
	 *            path for VectorWritable output
	 * @param vectorClass
	 *            String of desired class for returned vector e.g. DenseVector,
	 *            RandomAccessSparseVector (may be null for {@link DenseVector}
	 *            )
	 * @return Vector containing column-wise mean of DistributedRowMatrix
	 */
//	public Vector run(Configuration initialConf, Path inputPath,
//			Path outputPath, String vectorClass) throws IOException {
	public int run(String[] arg0) throws Exception {
//		addInputOption();
//		addOutputOption();
//		addOption("inputPath", "ip", "input path", true);
//		addOption("outputPath" , "op", "output path", true);
//		addOption("vectorClass", "vc",
//				"Class name of preferred output class (default is DenseVector)",
//				false);

		String vectorClass = null;
//		if (hasOption("vectorClass"))
//			vectorClass = getOption("vectorClass");

		Configuration conf = getConf();
		Configuration.dumpConfiguration(conf, new PrintWriter(System.out));

		conf.set(VECTOR_CLASS,
				vectorClass == null ? DenseVector.class.getName() : vectorClass);

		Path inputPath = null;
		Path outputPath = null;
		if (arg0.length == 2) {
			inputPath = new Path(arg0[0]);//getInputPath();
			outputPath = new Path(arg0[1]);//getOutputPath();
		} else {
			throw new IOException("Must specify input path and output path");
		}

		Job job = new Job(conf, "MahoutColumnMeans");
		job.setJarByClass(MahoutColumnMeans.class);

		FileOutputFormat.setOutputPath(job, outputPath);

		outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,
				true);
		job.setNumReduceTasks(1);
		FileOutputFormat.setOutputPath(job, outputPath);
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job, outputPath);

		job.setMapperClass(MatrixColumnMeansMapper.class);
		job.setReducerClass(MatrixColumnMeansReducer.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(VectorWritable.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(VectorWritable.class);
		job.submit();
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * Mapper for calculation of column-wise mean.
	 */
	public static class MatrixColumnMeansMapper extends
			Mapper<Writable, VectorWritable, NullWritable, VectorWritable> {

		private Vector runningSum;
		private String vectorClass;

		@Override
		public void setup(Context context) {
			vectorClass = context.getConfiguration().get(VECTOR_CLASS);
		}

		/**
		 * The mapper computes a running sum of the vectors the task has seen.
		 * Element 0 of the running sum vector contains a count of the number of
		 * vectors that have been seen. The remaining elements contain the
		 * column-wise running sum. Nothing is written at this stage
		 */
		@Override
		public void map(Writable r, VectorWritable v, Context context)
				throws IOException {
			if (runningSum == null) {
				/*
				 * If this is the first vector the mapper has seen, instantiate
				 * a new vector using the parameter VECTOR_CLASS
				 */
				runningSum = ClassUtils.instantiateAs(vectorClass,
						Vector.class, new Class<?>[] { int.class },
						new Object[] { v.get().size() + 1 });
				runningSum.set(0, 1);
				runningSum.viewPart(1, v.get().size()).assign(v.get());
			} else {
				runningSum.set(0, runningSum.get(0) + 1);
				runningSum.viewPart(1, v.get().size()).assign(v.get(),
						Functions.PLUS);
			}
		}

		/**
		 * The column-wise sum is written at the cleanup stage. A single reducer
		 * is forced so null can be used for the key
		 */
		@Override
		public void cleanup(Context context) throws InterruptedException,
				IOException {
			if (runningSum != null) {
				context.write(NullWritable.get(),
						new VectorWritable(runningSum));
			}
		}

	}

	/**
	 * The reducer adds the partial column-wise sums from each of the mappers to
	 * compute the total column-wise sum. The total sum is then divided by the
	 * total count of vectors to determine the column-wise mean.
	 */
	public static class MatrixColumnMeansReducer extends
			Reducer<NullWritable, VectorWritable, IntWritable, VectorWritable> {

		private static final IntWritable ONE = new IntWritable(1);

		private String vectorClass;
		private Vector outputVector;
		private final VectorWritable outputVectorWritable = new VectorWritable();

		@Override
		public void setup(Context context) {
			vectorClass = context.getConfiguration().get(VECTOR_CLASS);
		}

		@Override
		public void reduce(NullWritable n, Iterable<VectorWritable> vectors,
				Context context) throws IOException, InterruptedException {

			/**
			 * Add together partial column-wise sums from mappers
			 */
			for (VectorWritable v : vectors) {
				if (outputVector == null) {
					outputVector = v.get();
				} else {
					outputVector.assign(v.get(), Functions.PLUS);
				}
			}

			/**
			 * Divide total column-wise sum by count of vectors, which
			 * corresponds to the number of rows in the DistributedRowMatrix
			 */
			if (outputVector != null) {
				outputVectorWritable.set(outputVector.viewPart(1,
						outputVector.size() - 1).divide(outputVector.get(0)));
				context.write(ONE, outputVectorWritable);
			} else {
				Vector emptyVector = ClassUtils.instantiateAs(vectorClass,
						Vector.class, new Class<?>[] { int.class },
						new Object[] { 0 });
				context.write(ONE, new VectorWritable(emptyVector));
			}
		}
	}
}
