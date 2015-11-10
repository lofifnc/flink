/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.tool.runtime.messaging;

import io.netty.channel.ChannelOutboundBuffer;
import org.apache.commons.lang.ArrayUtils;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//
//  Weather update client in Java
//  Connects SUB socket to tcp://localhost:5556
//  Collects weather updates and finds avg temp in zipcode
//
public class wuclient {

	final static Pattern start = Pattern.compile("START (\\d+) (\\d+)");
	final static Pattern end = Pattern.compile("END (\\d+)");

	enum MessageType {
		START("START".getBytes()),
		END("END".getBytes()),
		ELEM("ELEM".getBytes()),
		SER("SER".getBytes());

		private final byte[] bytes;
		private final int lenght;

		MessageType(byte[] bytes) {
			this.bytes = bytes;
			this.lenght = bytes.length;
		}

		private byte[] getBytes() {
			return getBytes();
		}

		public static MessageType getMessageType(byte[] message) {
			for(MessageType type : MessageType.values()) {
				if(isType(message,type)) {
					return type;
				}
			}
			throw new UnsupportedOperationException("could not find type for message");
		}


		public static Boolean isType(byte[] message, MessageType type) {
			byte[] subArray = Arrays.copyOfRange(message, 0, type.lenght);
			return ArrayUtils.isEquals(subArray, type.bytes);
		}

	}

	public static void main(String[] args) throws UnsupportedEncodingException {


		class MessageProcessor {

			Set<Integer> participatingSinks = new HashSet<>();
			Set<Integer> finishedSinks = new HashSet<>();
			int parallelism = -1;
			String serializer = null;

			public void processMessage(byte[] bytes) throws UnsupportedEncodingException {

				MessageType type = MessageType.getMessageType(bytes);

				switch (type) {
					case START:
						String msg = new String(bytes);
						System.out.println(msg);
						String[] values = msg.split(" ");
						participatingSinks.add(Integer.parseInt(values[1]));
						parallelism = Integer.parseInt(values[2]);
						System.out.println("parallelism = " + parallelism);
						break;
					case SER:
						if(serializer == null) {
							serializer = new String(ArrayUtils.subarray(bytes, type.lenght, bytes.length));
							System.out.println("serializer = " + serializer);
						}
						break;
					case ELEM:
						String elem = new String(ArrayUtils.subarray(bytes, type.lenght, bytes.length));
						System.out.println("elem = " + elem);
						break;
					case END:
						msg = new String(bytes);
						System.out.println(msg);
						int sinkIndex = Integer.parseInt(msg.split(" ")[1]);
						finishedSinks.add(sinkIndex);
						break;
				}
				System.out.println("all started? " + (participatingSinks.size() == parallelism));
				System.out.println("all finished?" + (finishedSinks.size() == parallelism));
			}
		}

		MessageProcessor proc = new MessageProcessor();

		byte[] bytes;
		bytes =  String.format("START %d %d", 1, 2).getBytes();
		proc.processMessage(bytes);

		bytes =  "SERblablabab".getBytes();
		proc.processMessage(bytes);

		bytes =  String.format("START %d %d", 2, 2).getBytes();
		proc.processMessage(bytes);

		bytes =  "ELEMELEM".getBytes();
		proc.processMessage(bytes);

		bytes =  "SERblablabab".getBytes();
		proc.processMessage(bytes);

		bytes =  "ELEMELEM".getBytes();
		proc.processMessage(bytes);

		bytes =  "ELEMELEM".getBytes();
		proc.processMessage(bytes);

		bytes =  "END 2".getBytes();
		proc.processMessage(bytes);

		bytes =  "ELEMELEM".getBytes();
		proc.processMessage(bytes);

		bytes =  "END 1".getBytes();
		proc.processMessage(bytes);

	}

//	public static void main(String[] args) {
//		ZMQ.Context context = ZMQ.context(1);
//
//		//  Socket to talk to server
//		System.out.println("Collecting updates from weather server");
//		ZMQ.Socket subscriber = context.socket(ZMQ.SUB);
//		subscriber.connect("tcp://localhost:5556");
//
//		//  Subscribe to zipcode, default is NYC, 10001
//		String filter = (args.length > 0) ? args[0] : "10001 ";
//		subscriber.subscribe("".getBytes());
//
//		String serializer;
//		int parrelism;
//		Set<Integer> participatingSinks = new HashSet<>();
//		Set<Integer> finishedSinks = new HashSet<>();
//
//		Pattern start = Pattern.compile("START (\\d+) (\\d+)");
//		Pattern end = Pattern.compile("END (\\d+)");
//		//  Process 100 updates
//		int update_nbr;
//		long total_temp = 0;
//		for (update_nbr = 0; update_nbr < 100; update_nbr++) {
//			//  Use trim to remove the tailing '0' character
//			byte[] bytes = subscriber.recv();
//
//
//
//
//
////			StringTokenizer sscanf = new StringTokenizer(string, " ");
////			int zipcode = Integer.valueOf(sscanf.nextToken());
////			int temperature = Integer.valueOf(sscanf.nextToken());
////			int relhumidity = Integer.valueOf(sscanf.nextToken());
////
////			total_temp += temperature;
//
//		}
//		System.out.println("Average temperature for zipcode '"
//				+ filter + "' was " + (int) (total_temp / update_nbr));
//
//		subscriber.close();
//		context.term();
//		System.out.println("finished");
//


}

