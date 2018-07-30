package org.eu.mmacedo.mysql.log.sink.service;

import static pl.touk.throwing.ThrowingSupplier.unchecked;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Resource;

import org.eu.mmacedo.mysql.log.sink.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class InboundFileSource {

	private static final Logger LOGGER = LoggerFactory.getLogger(InboundFileSource.class);

	private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

	private final Pattern pattern = Pattern.compile("\\|");

	@Value("${accesslog}")
	private String accesslog;

	@Resource(name = "theExecutor")
	private ExecutorService theExecutor;

	@Autowired
	private CountDownLatch countDownLatch;

	@Autowired
	BatchAggregatorService aggregator;

	private CompletableFuture<LocalDateTime> getDate(final String input) {
		return CompletableFuture.supplyAsync(() -> {
			return LocalDateTime.parse(input, formatter);
		}, theExecutor);
	}

	public CompletableFuture<Integer> getIpV4(final String input) {
		return CompletableFuture.supplyAsync(unchecked(() -> {
			return ByteUtils.getIntFromIp(input);
		}), theExecutor);
	}

	private CompletableFuture<Integer> getResponse(final String input) {
		return CompletableFuture.supplyAsync(() -> {
			return Integer.parseInt(input);
		}, theExecutor);
	}

	private Optional<Map<Integer, CompletableFuture<?>>> getRowAsync(final String input) {
		final AtomicInteger index = new AtomicInteger();
		return Optional.of(pattern.splitAsStream(input).map(f -> {
			switch (index.incrementAndGet()) {
			case 1: // Date
				return new AbstractMap.SimpleEntry<>(1, getDate(f));
			case 2: // IPv4
				return new AbstractMap.SimpleEntry<>(2, getIpV4(f));
			case 3: // Method
				return new AbstractMap.SimpleEntry<>(3,
						CompletableFuture.completedFuture(f));
			case 4: // Response
				return new AbstractMap.SimpleEntry<>(4, getResponse(f));
			case 5: // Agent
				return new AbstractMap.SimpleEntry<>(5,
						CompletableFuture.completedFuture(f));
			default: // Undefined
				return new AbstractMap.SimpleEntry<>(index.get(),
						CompletableFuture.completedFuture(null));
			}
		}).collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue(), (a, b) -> b, LinkedHashMap::new)));
		// preserve order with LinkedHashMap
	}

	public void run() throws Exception {
		LOGGER.info("Reading " + accesslog);
		Files.lines(Paths.get(accesslog)).map(this::getRowAsync).forEach(r -> {
			CompletableFuture.runAsync(() -> {
				aggregator.run(r);
			}, theExecutor);
		});
		CompletableFuture.runAsync(() -> {
			aggregator.run(Optional.empty()); // EOF
		}, theExecutor);
		countDownLatch.await(); // wait sinking end
	}
}
