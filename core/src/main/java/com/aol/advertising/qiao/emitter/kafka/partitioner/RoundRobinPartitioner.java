package com.aol.advertising.qiao.emitter.kafka.partitioner;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class RoundRobinPartitioner implements Partitioner {

	int idx = 0;
	
	public  RoundRobinPartitioner (VerifiableProperties props) {
	}

	@Override
	public int partition(Object key, int numPartitions) {
		int i = idx;
		idx++;
		if (idx >= numPartitions) idx = 0;
		return i;
	}
}
