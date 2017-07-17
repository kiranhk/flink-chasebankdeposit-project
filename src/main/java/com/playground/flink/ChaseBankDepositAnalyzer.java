package com.playground.flink;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple20;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;


public class ChaseBankDepositAnalyzer {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);

		DataSet<Tuple20<String, Boolean, String, Integer, String, String, String, String, String, String, String, 
				Float, Float, Long, Long, Long, Long, Long, Long, Long>> deposits = env.readCsvFile(params.get("input"))
				.ignoreFirstLine().parseQuotedStrings('"').ignoreInvalidLines()
				.types(String.class, Boolean.class, String.class, Integer.class, String.class,
						String.class, String.class, String.class, String.class, String.class, String.class, Float.class,
						Float.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class,
						Long.class);
		


		DataSet<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> depositsByCity = deposits
				.map(new DepositCreator()).groupBy(0)
				.reduceGroup(new CityDepositAggregator());
		
		if (params.has("output")) {
			deposits.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			depositsByCity.print();
		}

	}

	public static final class DepositCreator implements
			MapFunction<Tuple20<String, Boolean, String, Integer, String, String, String, String, String, String, String, Float, Float, Long, Long, Long, Long, Long, Long, Long>, Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple8<String, Long, Long, Long, Long, Long, Long, Long> map(
				Tuple20<String, Boolean, String, Integer, String, String, String, String, String, String, String, Float, Float, Long, Long, Long, Long, Long, Long, Long> value)
				throws Exception {
			ChaseBankDeposit deposit = ChaseBankDeposit.fromTuple(value);
			if (deposit != null) {
				return new Tuple8<String, Long, Long, Long, Long, Long, Long, Long>(deposit.city, deposit.deposits2010,
						deposit.deposits2011, deposit.deposits2012, deposit.deposits2013, deposit.deposits2014,
						deposit.deposits2015, deposit.deposits2016);
			}
			return new Tuple8<String, Long, Long, Long, Long, Long, Long, Long>("Empty", 0l, 0l, 0l, 0l, 0l, 0l, 0l);
		}
	}

	public static class CityDepositAggregator implements
			GroupReduceFunction<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>, Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> {

		private static final long serialVersionUID = -3547544639251730804L;

		@Override
		public void reduce(Iterable<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> arg0,
				Collector<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> collector) throws Exception {
			Long deposits2010 = 0l;
			Long deposits2011 = 0l;
			Long deposits2012 = 0l;
			Long deposits2013 = 0l;
			Long deposits2014 = 0l;
			Long deposits2015 = 0l;
			Long deposits2016 = 0l;
			String city = "";
			for (Tuple8<String, Long, Long, Long, Long, Long, Long, Long> tuple : arg0) {
				deposits2010 += tuple.f1;
				deposits2011 += tuple.f2;
				deposits2012 += tuple.f3;
				deposits2013 += tuple.f4;
				deposits2014 += tuple.f5;
				deposits2015 += tuple.f6;
				deposits2016 += tuple.f7;
				city = tuple.f0;
			}

			collector.collect(new Tuple8<String, Long, Long, Long, Long, Long, Long, Long>(city, deposits2010,
					deposits2011, deposits2012, deposits2013, deposits2014, deposits2015, deposits2016));

		}
	}

}