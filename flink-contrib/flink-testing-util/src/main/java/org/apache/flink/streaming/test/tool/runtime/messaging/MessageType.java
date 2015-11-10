package org.apache.flink.streaming.test.tool.runtime.messaging;

import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Enumeration of message types used for the protocol of transmitting
 * output from the sinks.
 */
public enum MessageType {

	START("START".getBytes()),
	END("END".getBytes()),
	ELEM("ELEM".getBytes()),
	SER("SER".getBytes());

	/** byte representation of the message identifier */
	public final byte[] bytes;
	/** number of bytes of the message encoding */
	public final int length;

	MessageType(byte[] bytes) {
		this.bytes = bytes;
		this.length = bytes.length;
	}

	/**
	 * Get the message type for a received message.
	 *
	 * @param message byte array representing the message.
	 * @return type of the message.
	 */
	public static MessageType getMessageType(byte[] message) {
		for (MessageType type : MessageType.values()) {
			if (isType(message, type)) {
				return type;
			}
		}
		throw new UnsupportedOperationException("could not find type for message");
	}

	/**
	 * Gets the payload of message.
	 *
	 * @param message byte array representing the message.
	 * @return byte array containing the payload.
	 */
	public byte[] getPayload(byte[] message) {
		return ArrayUtils.subarray(message, length, message.length);
	}

	/**
	 * Checks if a byte array has a certain message type.
	 *
	 * @param message byte array containing the message.
	 * @param type    to be checked against.
	 * @return true if message has provided type.
	 */
	public static Boolean isType(byte[] message, MessageType type) {
		byte[] subArray = Arrays.copyOfRange(message, 0, type.length);
		return ArrayUtils.isEquals(subArray, type.bytes);
	}
}
