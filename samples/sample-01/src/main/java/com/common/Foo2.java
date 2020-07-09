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

package com.common;

import org.apache.kafka.common.utils.ByteBufferOutputStream;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

/**
 * @author Gary Russell
 * @since 2.2.1
 */
public class Foo2 {

    private String foo;
    private Person person;

    public Foo2(Person person) {
        this.person = person;
    }

    public Foo2() {
        super();
    }

    public Foo2(String foo) {
        this.foo = foo;
    }

    public Person getPerson() {
        return person;
    }

    public String getFoo() {
        return this.foo;
    }

    public void setFoo(String foo) {
        this.foo = foo;
    }

    @Override
    public String toString() {
        return "Foo2 [foo=" + this.foo + "]";
    }

    public static void main(String[] args) {
        ByteBuffer buffer = MappedByteBuffer.wrap("9999990".getBytes());
        ByteBufferOutputStream stream = new ByteBufferOutputStream(buffer);
        buffer.clear();
        buffer = null;

        Person person = new Person();
        Foo2 f2 = new Foo2(person);
        person = null;
        System.out.println("==" + f2.getPerson());
        System.out.println(new String(stream.buffer().array()));

    }
}
