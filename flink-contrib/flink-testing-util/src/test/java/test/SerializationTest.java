package test;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;

public class SerializationTest extends Test {

	@org.junit.Test
	public void serialize() {
		Tuple2<Integer,String> tuple = new Tuple2<>(1,"test");
//		TypeInformation<Tuple2<Integer,String>> type = new TypeExtractor(tuple);
	}


}
