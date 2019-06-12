/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.kafka.test.utils;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Test;

import org.springframework.kafka.test.rule.EmbeddedKafkaRule;

/**
 * @author Gary Russell
 * @since 2.2.7
 *
 */
public class KafkaTestUtilsTests {

	@ClassRule
	public static EmbeddedKafkaRule broker = new EmbeddedKafkaRule(1, true, 1, "singleTopic1", "singleTopic2");

	@Test
	public void testGetSingleWithMoreThatOneTopic() {
		Map<String, Object> producerProps = KafkaTestUtils.producerProps(broker.getEmbeddedKafka());
		KafkaProducer<Integer, String> producer = new KafkaProducer<>(producerProps);
		producer.send(new ProducerRecord<>("singleTopic1", 1, "foo"));
		producer.send(new ProducerRecord<>("singleTopic2", 1, "foo"));
		producer.close();
		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("ktuTests", "false",
				broker.getEmbeddedKafka());
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		KafkaConsumer<Integer, String> consumer = new KafkaConsumer<>(consumerProps);
		broker.getEmbeddedKafka().consumeFromAllEmbeddedTopics(consumer);
		KafkaTestUtils.getSingleRecord(consumer, "singleTopic1");
		KafkaTestUtils.getSingleRecord(consumer, "singleTopic2");
		consumer.close();
	}

}
