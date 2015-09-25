package test;

import api.StreamTestUtil;
import api.input.After;
import api.input.EventTimeInputBuilder;
import api.output.ExpectedOutput;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import scala.Tuple2;
import java.util.concurrent.TimeUnit;


public class Test {

	public static void main(String[] args) throws Exception {
		System.out.println("start");

		//-------------- input
		EventTimeInputBuilder<Tuple2<Integer, String>> input = EventTimeInputBuilder
				.addFirst(new Tuple2<>(1, "test"))
				.add(new Tuple2<>(2, "foo"), After.period(10, TimeUnit.SECONDS))
				.add(new Tuple2<>(3, "bar"), After.period(10, TimeUnit.SECONDS));

		//-------------- expected output
		ExpectedOutput<Tuple2<String, Integer>> expectedOutput = new ExpectedOutput<Tuple2<String, Integer>>()
				.add(new Tuple2<>("test", 1))
				.add(new Tuple2<>("bar", 3))
				.add(new Tuple2<>("fo", 2));
		expectedOutput.expect.only();

		//------- pipeline definition
		StreamTestUtil testUtil = new StreamTestUtil();
		DataStreamSource<Tuple2<Integer, String>> stream = testUtil.createDataSource(input);

		stream
				.map(new MapFunction<Tuple2<Integer, String>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> map(Tuple2<Integer, String> tuple) throws Exception {
						return tuple.swap();
					}
				})
				.addSink(testUtil.createTestSink(expectedOutput));

		testUtil.executeTest();
		//--------------

		System.exit(0);
	}
}
