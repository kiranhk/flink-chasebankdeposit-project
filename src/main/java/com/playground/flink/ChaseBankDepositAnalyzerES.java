package com.playground.flink;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;


public class ChaseBankDepositAnalyzerES {

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setGlobalJobParameters(params);

		DataStream<String> deposits = null;

		if (params.has("input")) {
			deposits = env.readTextFile(params.get("input"));
		} else {
			System.out.println("Use --input to specify file input.");
			System.exit(5);
		}

		DataStream<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> depositsByCity = deposits
				.map(new DepositCreator()).keyBy(0)
				.timeWindow(Time.milliseconds(10))
				.apply(new CityDepositAggregator())
				.filter(new InvalidCityFilter());

		if (params.has("output")) {
			deposits.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			depositsByCity.print();
		}
		Map<String, String> config = new HashMap<>();
		config.put("bulk.flush.max.actions", "10");
		config.put("cluster.name", "elasticsearch");

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("localhost"), 9300));

		depositsByCity.addSink(new ElasticsearchSink<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>>(config,
				transports, new ChaseDepositsByCity()));

		env.execute("City Deposits to Elasticsearch");
	}

	public static final class DepositCreator
			implements MapFunction<String, Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> {

		private static final long serialVersionUID = -4850355046078392805L;

		@Override
		public Tuple8<String, Long, Long, Long, Long, Long, Long, Long> map(String value) throws Exception {
			ChaseBankDeposit deposit = ChaseBankDeposit.fromString(value);
			if (deposit != null) {
				return new Tuple8<String, Long, Long, Long, Long, Long, Long, Long>(deposit.city, deposit.deposits2010,
						deposit.deposits2011, deposit.deposits2012, deposit.deposits2013, deposit.deposits2014,
						deposit.deposits2015, deposit.deposits2016);
			}
			return new Tuple8<String, Long, Long, Long, Long, Long, Long, Long>("Empty", 0l, 0l, 0l, 0l, 0l, 0l, 0l);
		}

	}

	public static class CityDepositAggregator implements
			WindowFunction<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>, // input
																						// type
					Tuple8<String, Long, Long, Long, Long, Long, Long, Long>, // output
																				// type
					Tuple, // key type
					TimeWindow> {
		private static final long serialVersionUID = -3547544639251730804L;

		@Override
		public void apply(Tuple arg0, TimeWindow arg1,
				Iterable<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> arg2,
				Collector<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> collector) throws Exception {
			Long deposits2010 = 0l;
			Long deposits2011 = 0l;
			Long deposits2012 = 0l;
			Long deposits2013 = 0l;
			Long deposits2014 = 0l;
			Long deposits2015 = 0l;
			Long deposits2016 = 0l;
			String city = "";
			for (Tuple8<String, Long, Long, Long, Long, Long, Long, Long> tuple : arg2) {
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

	public static class ChaseDepositsByCity
			implements ElasticsearchSinkFunction<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> {

		private static final long serialVersionUID = 9017712384512484220L;

		public IndexRequest createIndexRequest(String element) {
	        Map<String, String> json = new HashMap<>();
	        json.put("data", element);

	        return Requests.indexRequest()
	                .index("chase-deposits")
	                .type("deposits-bycity")
	                .source(json);
	    }
		
		@Override
		public void process(Tuple8<String, Long, Long, Long, Long, Long, Long, Long> record, RuntimeContext ctx,
				RequestIndexer indexer) {

			Map<String, String> input = new HashMap<>();
			input.put("city", record.f0);
			input.put("deposits2010", record.f1.toString());
			input.put("deposits2011", record.f2.toString());
			input.put("deposits2012", record.f3.toString());
			input.put("deposits2013", record.f4.toString());
			input.put("deposits2014", record.f5.toString());
			input.put("deposits2015", record.f6.toString());
			input.put("deposits2016", record.f7.toString());

			IndexRequest req = Requests.indexRequest().index("chase-deposits") // name
					.type("deposits-bycity").source(input);

			indexer.add(req);
		}
	}

	
	public static class InvalidCityFilter implements FilterFunction<Tuple8<String, Long, Long, Long, Long, Long, Long, Long>> {
		private static final long serialVersionUID = 2230966282557389669L;

		@Override
		public boolean filter(Tuple8<String, Long, Long, Long, Long, Long, Long, Long> deposit) throws Exception {

			return !deposit.f0.equalsIgnoreCase("Empty");
		}
	}

	
}