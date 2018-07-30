package org.eu.mmacedo.mysql.log.sink.service;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class BatchAggregatorService {

	private static final Logger LOGGER = LoggerFactory.getLogger(BatchAggregatorService.class);

	@Value("${batchSize}")
	private Integer batchSize;

	@Autowired
	private LinkedTransferQueue<Optional<List<Map<Integer, CompletableFuture<?>>>>> batchqueue;

	private static List<Map<Integer, CompletableFuture<?>>> innerBatch = new LinkedList<>();

	private static ReentrantLock lock = new ReentrantLock();

	public void run(Optional<Map<Integer, CompletableFuture<?>>> futureRow) {
		while (!lock.tryLock()) {
		}
		// non blocking aggregation
		try {
			futureRow.ifPresent(innerBatch::add);
			if (!futureRow.isPresent() || innerBatch.size() >= batchSize) {
				LOGGER.debug("sending batch with size: " + innerBatch.size());
				// List<Map<Integer, CompletableFuture<?>>> cloned = new
				// LinkedList<>(innerBatch);
				batchqueue.put(Optional.of(innerBatch));
				// innerBatch.clear(); // prepare for new batch
				innerBatch = new LinkedList<>();
			}
			if (!futureRow.isPresent()) {
				batchqueue.put(Optional.empty());
			}
		} finally {
			lock.unlock();
		}

	}

}
