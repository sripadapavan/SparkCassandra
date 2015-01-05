import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

import org.apache.hadoop.util.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SchemaRDD;
import org.apache.spark.sql.api.java.JavaSQLContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.api.java.Row;
import org.apache.spark.sql.cassandra.CassandraSQLContext;

import scala.Tuple2;

import com.clearspring.analytics.util.Lists;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.SparkContextJavaFunctions;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;

public class Spark_Cassandra implements Serializable {

	public void loadPriceAndPromo() {

		SparkConf conf = new SparkConf().setAppName("MyApp")
		// .setMaster("spark://platform-ad-1:7077")
				.setMaster("local[2]").set("spark.ui.port", "4040").set("spark.cassandra.connection.host", "10.66.136.204");

		JavaSparkContext sc = new JavaSparkContext(conf);

		long start = System.currentTimeMillis();
		JavaRDD<String> priceRowsRDD = javaFunctions(sc).cassandraTable("priceks", "itemprice").select("partnumber", "offerprice").map(new Function<CassandraRow, String>() {
			@Override
			public String call(CassandraRow cassandraRow) throws Exception {
				return cassandraRow.toString();
			}
		});
		JavaRDD<String> promoRowsRDD = javaFunctions(sc).cassandraTable("promotionks", "itempromotion").select("partnumber", "promotionid").map(new Function<CassandraRow, String>() {
			@Override
			public String call(CassandraRow cassandraRow) throws Exception {
				return cassandraRow.toString();
			}
		});
		StringBuffer pricesb = new StringBuffer();
		pricesb.append(StringUtils.join("\n", priceRowsRDD.collect()));

		StringBuffer promosb = new StringBuffer();
		promosb.append(StringUtils.join("\n", promoRowsRDD.collect()));

		System.out.println("time spent : " + (System.currentTimeMillis() - start));
		System.out.println("price size : " + priceRowsRDD.collect().size());
		System.out.println("promo size : " + promoRowsRDD.collect().size());

		try {
			Files.write(Paths.get("file.txt"), pricesb.toString().getBytes(), StandardOpenOption.CREATE);
			Files.write(Paths.get("file.txt"), promosb.toString().getBytes(), StandardOpenOption.APPEND);
		} catch (IOException e) {

			e.printStackTrace();
		}

		sc.stop();
	}

	public void joinPriceAndPromo() {

		SparkConf conf = new SparkConf().setAppName("MyApp")
		// .setMaster("spark://platform-ad-1:7077")
				.setMaster("local[2]").set("spark.ui.port", "4040").set("spark.cassandra.connection.host", "10.66.136.204");

		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaSQLContext sqlCtx = new JavaSQLContext(sc);

		CassandraJavaRDD<CassandraRow> priceRDD = CassandraJavaUtil.javaFunctions(sc).cassandraTable("priceks", "itemprice");
		CassandraJavaRDD<CassandraRow> promoRDD = CassandraJavaUtil.javaFunctions(sc).cassandraTable("promotionks", "itempromotion");
		priceRDD.cache();
		promoRDD.cache();
		System.out.println(priceRDD.first().toString());
		System.out.println(promoRDD.first().toString());

		long start = System.currentTimeMillis();

		JavaSchemaRDD schemaPrice = sqlCtx.applySchema(priceRDD, Price.class);
		schemaPrice.registerTempTable("price");
		schemaPrice.printSchema();

		JavaSchemaRDD schemaPromo = sqlCtx.applySchema(promoRDD, Promo.class);
		schemaPromo.registerTempTable("promotion");
		schemaPromo.printSchema();

		JavaSchemaRDD items = sqlCtx.sql("SELECT partnumber FROM price");
		sqlCtx.sql("SELECT a.partnumber,a.offerprice,b.promotionid " + " FROM itemprice a JOIN itempromotion b ON a.partnumber = b.partnumber");

		List<String> itemPriceRows = items.map(new Function<Row, String>() {
			public String call(Row row) {
				return "partnumber: " + row.getString(0);
			}
		}).collect();
		System.out.println("Item rows : " + itemPriceRows.size());
		for (String item : itemPriceRows)
			System.out.println(item);

		sc.stop();
	}

	public void groupByPartNumber() {
		SparkConf conf = new SparkConf().setAppName("MyApp")
		// .setMaster("spark://platform-ad-1:7077")
				.setMaster("local[2]").set("spark.ui.port", "4040").set("spark.cassandra.connection.host", "10.66.136.204");

		SparkContext sc = new SparkContext(conf);
		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(sc);

		JavaPairRDD<String, Integer> pricePairRDD = getRDD(functions, "priceks", "itemprice", "partnumber", "offerprice");
		pricePairRDD.cache();
		List<Tuple2<String, Integer>> priceResults = pricePairRDD.collect();

		for (Tuple2<String, Integer> tuple : priceResults) {
			System.out.println(tuple.toString());
		}

		JavaPairRDD<String, Integer> promoPairRDD = getRDD(functions, "promotionks", "itempromotion", "partnumber", "promotionid");
		pricePairRDD.cache();
		List<Tuple2<String, Integer>> promoResults = promoPairRDD.collect();

		for (Tuple2<String, Integer> tuple : promoResults) {
			System.out.println(tuple.toString());
		}
		sc.stop();
	}

	public void joinPricePromo() {
		SparkConf conf = new SparkConf().setAppName("MyApp")
		// .setMaster("spark://platform-ad-1:7077")
				.setMaster("local[2]").set("spark.ui.port", "4040").set("spark.cassandra.connection.host", "10.66.136.204");

		SparkContext sc = new SparkContext(conf);
		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(sc);

		JavaPairRDD<String, Integer> pricePairRDD = getRDD(functions, "priceks", "itemprice", "partnumber", "offerprice");
		pricePairRDD.cache();
		List<Tuple2<String, Integer>> priceResults = pricePairRDD.collect();

		/*
		 * for(Tuple2<String,Integer> tuple : priceResults){
		 * System.out.println(tuple.toString()); }
		 */

		JavaPairRDD<String, Integer> promoPairRDD = getRDD(functions, "promotionks", "itempromotion", "partnumber", "promotionid");
		pricePairRDD.cache();
		List<Tuple2<String, Integer>> promoResults = promoPairRDD.collect();

		/*
		 * for(Tuple2<String,Integer> tuple : promoResults){
		 * System.out.println(tuple.toString()); }
		 */

		JavaRDD<Tuple2<Integer, Integer>> joinedRDD = pricePairRDD.join(promoPairRDD).values();

		List<Tuple2<Integer, Integer>> joineList = joinedRDD.collect();

		for (Tuple2<Integer, Integer> tuple : joineList) {
			System.out.println(tuple.toString());
		}
		sc.stop();
	}

	private JavaPairRDD<String, Integer> getRDD(SparkContextJavaFunctions functions, String keyspace, String columnFamily, String column1, String column2) {
		JavaRDD<CassandraRow> rdd = functions.cassandraTable(keyspace, columnFamily).select(column1, column2);
		rdd.cache();
		JavaPairRDD<String, Integer> pairRDD = rdd.groupBy(new Function<CassandraRow, String>() {
			@Override
			public String call(CassandraRow row) throws Exception {
				return row.getString(0);
			}
		}).mapToPair(new PairFunction<Tuple2<String, Iterable<CassandraRow>>, String, Integer>() {
			@Override
			public Tuple2<String, Integer> call(Tuple2<String, Iterable<CassandraRow>> t) throws Exception {
				return new Tuple2<String, Integer>(t._1(), Lists.newArrayList(t._2()).size());
			}
		});
		return pairRDD;
	}

	public void joinPricePromouUsingCassandraJavaUtil() {
		SparkConf conf = new SparkConf().setAppName("MyApp")
		// .setMaster("spark://platform-ad-1:7077")
				.setMaster("local[2]").set("spark.ui.port", "4040").set("spark.cassandra.connection.host", "10.66.136.204");

		SparkContext sc = new SparkContext(conf);
		SparkContextJavaFunctions functions = CassandraJavaUtil.javaFunctions(sc);

		JavaRDD<CassandraRow> priceRDD = functions.cassandraTable("priceks", "itemprice").select("partnumber", "offerprice");
		priceRDD.cache();

		JavaPairRDD<String, Price> pricePairRDD = priceRDD.mapToPair(new PairFunction<CassandraRow, String, Price>() {
			@Override
			public Tuple2<String, Price> call(CassandraRow row) throws Exception {
				Price price = new Price();
				price.setPartnumber(row.getString(0));
				price.setOfferprice(row.getDouble(1));
				return new Tuple2<String, Price>(price.getPartnumber(), price);
			}
		});
		pricePairRDD.cache();
		List<Tuple2<String, Price>> priceResults = pricePairRDD.collect();
		/*
		 * for (Tuple2<String, Price> tuple : priceResults) {
		 * System.out.println(tuple.toString()); }
		 */
		JavaRDD<CassandraRow> promoRDD = functions.cassandraTable("promotionks", "itempromotion").select("partnumber", "promotionid");
		promoRDD.cache();

		JavaPairRDD<String, Promo> promoPairRDD = promoRDD.mapToPair(new PairFunction<CassandraRow, String, Promo>() {
			@Override
			public Tuple2<String, Promo> call(CassandraRow row) throws Exception {
				Promo promo = new Promo();
				promo.setPartnumber(row.getString(0));
				promo.setPromotionid(row.getString(1));
				return new Tuple2<String, Promo>(promo.getPartnumber(), promo);
			}
		});
		promoPairRDD.cache();
		List<Tuple2<String, Promo>> promoResults = promoPairRDD.collect();
		/*
		 * for (Tuple2<String, Promo> tuple : promoResults) {
		 * System.out.println(tuple.toString()); }
		 */

		JavaRDD<Tuple2<Price, Promo>> joinedRDD = pricePairRDD.join(promoPairRDD).values();

		List<Tuple2<Price, Promo>> joineList = joinedRDD.collect();

		for (Tuple2<Price, Promo> tuple : joineList) {
			System.out.println(tuple.toString());
		}

		sc.stop();
	}

	public void joinPricePromoUsingCassandraContext() {
		
		String master = System.getProperty("env");
		if(master==null) master="local[2]";
		SparkConf conf = new SparkConf().setAppName("MyApp")
				//.setMaster("spark://platform-ad-1:7077")

		.setMaster(master)
				.set("spark.ui.port", "4040").set("spark.cassandra.connection.host", "10.66.136.204");

		SparkContext sc = new SparkContext(conf);
		CassandraSQLContext csc = new CassandraSQLContext(sc);
		csc.setKeyspace("priceks");
		JavaSQLContext jsc = new JavaSQLContext(csc);

		long start = System.currentTimeMillis();
		SchemaRDD priceSchema = csc.sql("SELECT distinct partnumber FROM priceks.itemprice");
		priceSchema.registerTempTable("price");

		csc.setKeyspace("promotionks");
		SchemaRDD promoSchema = csc.sql("SELECT partnumber,promotionid FROM promotionks.itempromotion");
		promoSchema.registerTempTable("promo");

		JavaSchemaRDD joinRDD = jsc.sql("SELECT a.partnumber,b.promotionid FROM price a LEFT OUTER JOIN promo b ON a.partnumber=b.partnumber");

		List<Row> list = joinRDD.collect();
		long end = System.currentTimeMillis();

		System.out.println("total records : " + list.size());
		for (Row row : list)
			try {
				System.out.println("Partnumber : " + row.getString(0) + "[Promotionid : " + row.getString(1) + "]");
			} catch (Exception e) {

				System.out.println("Partnumber : " + row.getString(0) + "[No promotion]");

			}
		System.out.println("Time taken : " + (end - start));
		sc.stop();
	}

	public static void main(String[] args) throws IOException {
		// new Spark_Cassandra().loadPriceAndPromo();
		new Spark_Cassandra().joinPricePromoUsingCassandraContext();
	}

}
