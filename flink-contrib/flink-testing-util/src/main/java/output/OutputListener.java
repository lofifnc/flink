package output;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.zeromq.ZMQ;
import util.SerializeUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class OutputListener<OUT> {

	private ExecutorService executorService = Executors.newSingleThreadExecutor();
	private FutureTask<ArrayList<OUT>> outputFuture;

	public OutputListener(Integer port) {
		outputFuture = startListening(port);
	}

	private FutureTask<ArrayList<OUT>> startListening(int port) {
		final int publisherPort = port;
		FutureTask<ArrayList<OUT>> future =
				new FutureTask<>(new Callable<ArrayList<OUT>>() {
					public ArrayList<OUT>  call() throws IOException {
						ArrayList<byte[]> byteArray = new ArrayList<byte[]>();
						ZMQ.Context context = ZMQ.context(1);
						// Socket to receive from sink
						ZMQ.Socket subscriber = context.socket(ZMQ.PULL);
						subscriber.bind("tcp://*:" + publisherPort);
						// Receive serializer
						TypeSerializer<OUT> typeSerializer = SerializeUtil.deserialize(subscriber.recv());
						// Receive output from sink until END message;
						byte[] out = subscriber.recv();
						while (!Arrays.equals(out, "END".getBytes())) {
							byteArray.add(out);
							out = subscriber.recv();
						}

						ArrayList<OUT> sinkInput = new ArrayList<>();
						// Deserialize messages received from sink
						for(byte[] b: byteArray) {
							sinkInput.add(SerializeUtil.deserialize(b, typeSerializer));
						}
						return sinkInput;
					}});
		//listen for output
		executorService.execute(future);
		return future;
	}

	/**
	 * Waits for sink to close and returns output
	 * @return output from sink
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public ArrayList<OUT> getOutput() throws ExecutionException, InterruptedException {
		return outputFuture.get();
	}

}
