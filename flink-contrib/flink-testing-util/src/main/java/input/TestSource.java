//package input;
//
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
//import org.apache.flink.api.java.typeutils.TypeExtractor;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.lang.reflect.ParameterizedType;
//import java.lang.reflect.Type;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.LinkedBlockingQueue;
//
//
//public class TestSource<OUT> extends RichSourceFunction<OUT> implements ResultTypeQueryable<OUT>, Serializable {
//
//    private static final long serialVersionUID = 1L;
//    private static final Logger LOG = LoggerFactory.getLogger(TestSource.class);
//
//    protected BlockingQueue<EmitOutputTactic<OUT>> queue;
//    protected int queueSize = 1000;
//
//    protected ArrayList<OUT> outputList;
//    protected volatile int outputOffset = 0;
//
//    protected transient volatile boolean isRunning;
//
//    public TestSource() {
//        queue = new LinkedBlockingQueue<EmitOutputTactic<OUT>>(queueSize);
//        outputList = new ArrayList<OUT>();
//    }
//
//    @Override
//    public TypeInformation<OUT> getProducedType() {
//        try {
//            typeInfo = TypeExtractor.getForObject(data[0]);
//        }
//        catch (Exception e) {
//            throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName()
//                    + "; please specify the TypeInformation manually via "
//                    + "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)");
//        }
//        return typeInfo;
//    }
//
//    protected interface EmitOutputTactic<T> extends Serializable {
//        Boolean hasNextStep();
//
//        T getNextStep();
//    }
//
//    protected void addTactic(EmitOutputTactic<OUT> tactic) {
//        try {
//            queue.put(tactic);
//        } catch (InterruptedException e) {
//            //TODO handle
//            e.printStackTrace();
//        }
//    }
//
//    public void addOutput(List<OUT> output) {
//        outputList.addAll(output);
//    }
//
//    public boolean hasNextStep() {
//        return outputOffset < outputList.size();
//    }
//
//    public void emitAll() {
//
//        EmitOutputTactic<OUT> tactic = new EmitOutputTactic<OUT>() {
//            private static final long serialVersionUID = 1L;
//
//            @Override
//            public Boolean hasNextStep() {
//                return (outputOffset < outputList.size());
//            }
//
//            @Override
//            public OUT getNextStep() {
//                OUT out = outputList.get(outputOffset);
//                outputOffset++;
//                return out;
//            }
//        };
//        addTactic(tactic);
//    }
//
//    public void emitStep() throws IllegalStateException {
//        EmitOutputTactic<OUT> tactic = new EmitOutputTactic<OUT>() {
//            private static final long serialVersionUID = 1L;
//            private Boolean didStep = false;
//
//            @Override
//            public Boolean hasNextStep() {
//                if (!didStep) {
//                    didStep = true;
//                    return true;
//                } else {
//                    return false;
//                }
//            }
//
//            @Override
//            public OUT getNextStep() {
//                OUT out = outputList.get(outputOffset);
//                outputOffset++;
//                return out;
//            }
//        };
//        addTactic(tactic);
//    }
//
//    public void emitSteps(final int steps) {
//
//        EmitOutputTactic<OUT> tactic = new EmitOutputTactic<OUT>() {
//            private static final long serialVersionUID = 1L;
//            private int stepsTaken = 0;
//
//            @Override
//            public Boolean hasNextStep() {
//                return (outputOffset < outputList.size() && stepsTaken < steps);
//            }
//
//            @Override
//            public OUT getNextStep() {
//                OUT out = outputList.get(outputOffset);
//                outputOffset++;
//                stepsTaken++;
//                return out;
//            }
//        };
//        addTactic(tactic);
//    }
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        isRunning = true;
//    }
//
//    @Override
//    public void run(SourceContext<OUT> sourceContext) throws Exception {
//        while (isRunning) {
//            EmitOutputTactic<OUT> currentTactic = queue.take();
//            while (currentTactic.hasNextStep()) {
//                sourceContext.collect(currentTactic.getNextStep());
//            }
//        }
//    }
//
//    @Override
//    public void cancel() {
//        isRunning = false;
//    }
//}
