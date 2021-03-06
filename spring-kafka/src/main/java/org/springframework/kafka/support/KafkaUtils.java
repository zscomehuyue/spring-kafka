/*
 * Copyright 2018-2019 the original author or authors.
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

package org.springframework.kafka.support;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

import org.springframework.messaging.Message;

/**
 * Utility methods.
 *
 * @author Gary Russell
 *
 * @since 2.2
 *
 */
public final class KafkaUtils {

	private static ThreadLocal<String> groupIds = new ThreadLocal<>();

	/**
	 * Return true if the method return type is {@link Message} or
	 * {@code Collection<Message<?>>}.
	 * @param method the method.
	 * @return true if it returns message(s).
	 */
	public static boolean returnTypeMessageOrCollectionOf(Method method) {
		Type returnType = method.getGenericReturnType();
		if (returnType.equals(Message.class)) {
			return true;
		}
		if (returnType instanceof ParameterizedType) {
			ParameterizedType prt = (ParameterizedType) returnType;
			Type rawType = prt.getRawType();
			if (rawType.equals(Message.class)) {
				return true;
			}
			if (rawType.equals(Collection.class)) {
				Type collectionType = prt.getActualTypeArguments()[0];
				if (collectionType.equals(Message.class)) {
					return true;
				}
				return collectionType instanceof ParameterizedType
						&& ((ParameterizedType) collectionType).getRawType().equals(Message.class);
			}
		}
		return false;

	}

	/**
	 * Set the group id for the consumer bound to this thread.
	 * @param groupId the group id.
	 * @since 2.3
	 */
	public static void setConsumerGroupId(String groupId) {
		KafkaUtils.groupIds.set(groupId);
	}

	/**
	 * Get the group id for the consumer bound to this thread.
	 * @return the group id.
	 * @since 2.3
	 */
	public static String getConsumerGroupId() {
		return KafkaUtils.groupIds.get();
	}

	/**
	 * Clear the group id for the consumer bound to this thread.
	 * @since 2.3
	 */
	public static void clearConsumerGroupId() {
		KafkaUtils.groupIds.remove();
	}

	private KafkaUtils() {
		super();
	}

}
