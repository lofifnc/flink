package output;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import util.SerializeUtil;

import java.io.IOException;


public class TestSink<IN> extends RichSinkFunction<IN> {

	private Logger LOG = LoggerFactory.getLogger(RichSinkFunction.class);

	private transient Context context;
	private transient ZMQ.Socket publisher;
	private TypeSerializer<IN> serializer;
	private int port;

	public TestSink(int port) {
		this.port = port;
	}


	@Override
	public void open(Configuration configuration) {
		context = ZMQ.context(1);
		publisher = context.socket(ZMQ.PUSH);
		publisher.connect("tcp://localhost:" + port);
	}

	/**
	 * Called when new data arrives to the test.sink, and forwards it to Kafka.
	 *
	 * @param next The incoming data
	 */
	@Override
	public void invoke(IN next) {
		if (serializer == null) {
			/*
			create serializer
			 */
			TypeInformation<IN> typeInfo = TypeExtractor.getForObject(next);
			serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
			/*
			send serializer to output receiver
			 */
			try {
				publisher.send(SerializeUtil.serialize(serializer));
			} catch (IOException e) {
				LOG.error("Could not serialize TypeSerializer",e);
				return;
			}
		}
		System.out.println("out " + next);
		/*
		serialize output and send
		 */
		byte[] bytes;
		try {
			bytes = SerializeUtil.serialize(next, serializer);
		} catch (IOException e) {
			LOG.error("Could not serialize input",e);
			return;
		}
		publisher.send(bytes);
	}



	@Override
	public void close() {
		/*
		signal close to output receiver
		 */
		publisher.send("END".getBytes());
		publisher.close();
		context.term();
	}

}
