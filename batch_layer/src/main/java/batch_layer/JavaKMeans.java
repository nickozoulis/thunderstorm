package batch_layer;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.regex.Pattern;

import javax.script.ScriptException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

/**
 * Example using MLlib KMeans from Java.
 */
public final class JavaKMeans {

	private static class ParsePoint implements Function<String, Vector> {
		private static final Pattern SPACE = Pattern.compile(",");

		@Override
		public Vector call(String line) {
			String[] tok = SPACE.split(line);
			double[] point = new double[tok.length];
			for (int i = 0; i < tok.length; ++i) {
				point[i] = Double.parseDouble(tok[i]);
			}

			return Vectors.dense(point);

		}
	}

	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Usage: JavaKMeans <input_file> <max_iterations> [<runs>]");
			System.exit(1);
		}
		String inputFile = args[0];
		int iterations = Integer.parseInt(args[1]);
		int runs = 1;

		if (args.length >= 4) {
			runs = Integer.parseInt(args[2]);
		}
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("JavaKMeans");
		sparkConf.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = sc.textFile(inputFile);
		JavaRDD<Vector> points = lines.map(new ParsePoint());
		
		{
		
			JavaRDD<Vector> filter = points.filter(new Function<Vector, Boolean>() {

				DataFilter d = new DataFilter("x1<50");

				@Override
				public Boolean call(Vector v1) throws Exception {
					double[] point = v1.toArray();
					Point p = new Point(point);
					return d.run(p);
				}
			});

			KMeansModel model = KMeans.train(filter.rdd(), 4, iterations, runs, KMeans.K_MEANS_PARALLEL());

			System.out.println("Cluster centers:");
			for (Vector center : model.clusterCenters()) {
				System.out.println(" " + center);
			}
			double cost = model.computeCost(points.rdd());
			System.out.println("Cost: " + cost);
		}

		{
			KMeansModel model = KMeans.train(points.rdd(), 6, iterations, runs, KMeans.K_MEANS_PARALLEL());

			System.out.println("Cluster centers:");
			for (Vector center : model.clusterCenters()) {
				System.out.println(" " + center);
			}
			double cost = model.computeCost(points.rdd());
			System.out.println("Cost: " + cost);
		}
		sc.stop();
	}
}