package org.eu.mmacedo.mysql.log.sink.service;

import static pl.touk.throwing.ThrowingFunction.unchecked;

import java.sql.Types;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.eu.mmacedo.mysql.log.sink.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

@Service
public class OutboundMysqlSink {

	private static final Logger LOGGER = LoggerFactory.getLogger(OutboundMysqlSink.class);

	private static UUID startId = UUID.randomUUID();

	private static int[] argTypes = new int[] { Types.VARBINARY, Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR,
			Types.INTEGER, Types.VARCHAR };

	private static AtomicLong sequence = new AtomicLong();

	@Resource(name = "theExecutor")
	private ExecutorService theExecutor;

	@Autowired
	private Environment env;

	@Autowired
	private LinkedTransferQueue<Optional<List<Map<Integer, CompletableFuture<?>>>>> batchqueue;

	@Autowired
	private MetricRegistry metrics;

	@Autowired
	private CountDownLatch countDownLatch;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private Optional<Object[]> getLogArray(final Map<Integer, CompletableFuture<?>> row) {
		final CompletableFuture<?>[] waitArray = new CompletableFuture<?>[row.size()];
		row.values().toArray(waitArray);
		try {
			CompletableFuture.allOf(waitArray).get(); // gather all futures in row
			final LinkedList<Object> c = row.entrySet().stream().map(unchecked(s -> s.getValue().get()))
					// get value from future
					.collect(Collectors.toCollection(LinkedList::new));
			final byte[] id = ByteUtils.generatePK(startId, sequence);
			// LOGGER.trace(ByteUtils.bytesToHex(id));
			c.addFirst(id);
			return Optional.of(c.toArray());
		} catch (final Throwable e) {
			LOGGER.error(e.getMessage());
			// handle exception generating empty
			return Optional.empty();
		}
	}

	@Transactional
	public Void run() throws InterruptedException {
		boolean running = true;
		final Timer timer = metrics.timer("Batch insert");
		while (running || !batchqueue.isEmpty()) {
			final Optional<List<Map<Integer, CompletableFuture<?>>>> batch = batchqueue.take();
			batch.ifPresent(r -> {
				// filter empties generated from handling exceptions
				final List<Object[]> b = r.stream().map(this::getLogArray).filter(Optional::isPresent)
						.map(Optional::get).collect(Collectors.toCollection(LinkedList::new));
				if (!b.isEmpty()) {
					try {
						final Timer.Context bulk = timer.time();
						final String sql = env.getProperty("sql.insert");
						jdbcTemplate.batchUpdate(sql, b, argTypes); // batch insert
						bulk.stop();
					} catch (final Exception e) {
						LOGGER.error(e.getMessage());
					}
				}
			});
			if (!batch.isPresent()) {
				running = false;
			}
		}
		LOGGER.info("Sink has ended");
		countDownLatch.countDown(); // notify end of sink
		return null;
	}
}
