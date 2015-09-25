package util;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.InputViewDataInputStreamWrapper;
import org.apache.flink.core.memory.OutputViewDataOutputStreamWrapper;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

//TODO test this class
public class SerializeUtil {

	/**
	 * Serialize an object using a TypeSerializer.
	 * @param object to serialize
	 * @param serializer to use;
	 * @param <IN> type of the object
	 * @return serialized object
	 * @throws IOException
	 */
	public static <IN> byte[] serialize(IN object, TypeSerializer<IN> serializer) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		OutputViewDataOutputStreamWrapper wrapper = new OutputViewDataOutputStreamWrapper(new DataOutputStream(baos));
		serializer.serialize(object, wrapper);
		return baos.toByteArray();
	}

	/**
	 * Serialize a TypeSerializer
	 * @param serializer to serialize
	 * @param <T> type of the serializer
	 * @return serialized TypeSerializer
	 * @throws IOException
	 */
	public static <T> byte[] serialize(TypeSerializer<T> serializer) throws IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutput out = null;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(serializer);
			return bos.toByteArray();
		} finally {
			try {
				if (out != null) {
					out.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				bos.close();
			} catch (IOException ex) {
				// ignore close exception
			}
		}
	}

	/**
	 * Deserializes an byte array using the provided TypeSerializer.
	 * @param bytes byte array containing the serialized object
	 * @param serializer to use
	 * @param <OUT> type of the serialized object
	 * @return deserialized object
	 * @throws IOException
	 */
	public static <OUT> OUT deserialize(byte[] bytes, TypeSerializer<OUT> serializer) throws IOException {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		final DataInputView input = new InputViewDataInputStreamWrapper(new DataInputStream(bais));
		return serializer.deserialize(input);
	}

	/**
	 * Desializes a serialized TypeSerializer
	 * @param bytes serialized TypeSerializer
	 * @param <T> type of the serializer
	 * @return deserialized TypeSerializer
	 * @throws IOException
	 */
	public static <T> TypeSerializer<T> deserialize(byte[] bytes) throws IOException {
		ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
		ObjectInput in = null;
		try {
			in = new ObjectInputStream(bis);
			return (TypeSerializer<T>) in.readObject();
		} catch (ClassNotFoundException e) {
			throw new IOException("Could not deserialize class");
		} finally {
			try {
				bis.close();
			} catch (IOException ex) {
				// ignore close exception
			}
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException ex) {
				// ignore close exception
			}
		}
	}
}
