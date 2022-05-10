package org.kitasan.util;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

abstract class PairWritable<K extends Writable, V extends Writable> implements Writable {

    protected K key;
    protected V value;

    public PairWritable(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public K getKey() {
        return key;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public V getValue() {
        return value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        key.write(dataOutput);
        value.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        key.readFields(dataInput);
        value.readFields(dataInput);
    }

}