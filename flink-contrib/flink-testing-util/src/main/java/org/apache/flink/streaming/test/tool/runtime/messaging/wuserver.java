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

import java.util.Random;
import org.zeromq.ZMQ;

//
//  Weather update server in Java
//  Binds PUB socket to tcp://*:5556
//  Publishes random weather updates
//
public class wuserver {

	public static void main (String[] args) throws Exception {
		//  Prepare our context and publisher
		ZMQ.Context context = ZMQ.context(1);

		ZMQ.Socket publisher = context.socket(ZMQ.PUB);
		publisher.bind("tcp://*:5556");
		publisher.bind("ipc://weather");

		//  Initialize random number generator
		Random srandom = new Random(System.currentTimeMillis());
		while (!Thread.currentThread ().isInterrupted ()) {
//			//  Get values that will fool the boss
//			int zipcode, temperature, relhumidity;
//			zipcode = 10000 + srandom.nextInt(10000) ;
//			temperature = srandom.nextInt(215) - 80 + 1;
//			relhumidity = srandom.nextInt(50) + 10 + 1;
//
//			//  Send message to all subscribers
//			String update = String.format("%05d %d %d", zipcode, temperature, relhumidity);
			publisher.send("test", 0);
		}

		publisher.close ();
		context.term ();
	}
}
