/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.streaming.test.tool.runtime.messaging;

import org.apache.commons.lang.ArrayUtils;

import java.util.Arrays;

/**
 * Enumeration of message types used for the protocol of transmitting
 * output from the sinks.
 */
public enum MessageType {

	OPEN("OPEN".getBytes()),
	CLOSE("CLOSE".getBytes()),
	REC("REC".getBytes()),
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
