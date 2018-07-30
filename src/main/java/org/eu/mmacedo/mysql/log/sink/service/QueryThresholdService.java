package org.eu.mmacedo.mysql.log.sink.service;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.sql.Types;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.eu.mmacedo.mysql.log.sink.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import dnl.utils.text.table.TextTable;
import pl.touk.throwing.ThrowingFunction;

@Service
public class QueryThresholdService {

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryThresholdService.class);

	public static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH:mm:ss");

	private static final String warning = "Violated threshold {0,number,integer} {1} between {2} and {3}.";

	private static int[] argTypes = new int[] { Types.VARBINARY, Types.TIMESTAMP, Types.INTEGER, Types.LONGVARCHAR };

	private static String[] columnNames = { "IP" };

	private static UUID startId = UUID.randomUUID();

	private static AtomicLong sequence = new AtomicLong();

	private final ByteBuffer buffer = ByteBuffer.allocate(4);

	@Autowired
	private MetricRegistry metrics;

	@Autowired
	private Environment env;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	private Object[] getRow(final Integer ip, final LocalDateTime startDate, final LocalDateTime endDate,
			final Integer threshold, final String duration) {
		final String comment = MessageFormat.format(warning, threshold, duration, formatter.format(startDate),
				formatter.format(endDate));
		final Object[] row = { ByteUtils.generatePK(startId, sequence), LocalDateTime.now(), ip, comment };
		return row;
	}

	@Transactional
	public void block(final Object[][] blocked, final LocalDateTime startDate, final LocalDateTime endDate,
			final Integer threshold) {
		final Timer blk_timer = metrics.timer("Block");
		final String duration = env.getProperty("duration");
		final List<Object[]> toblock = Stream.of(blocked)
				.map(ThrowingFunction.unchecked(i -> ByteUtils.getIntFromIp((String) i[0])))
				.map(i -> getRow(i, startDate, endDate, threshold, duration))
				.collect(Collectors.toCollection(LinkedList::new));
		final String sql = env.getProperty("sql.blocked");
		final Timer.Context blk_t = blk_timer.time();
		jdbcTemplate.batchUpdate(sql, toblock, argTypes); // batch insert
		blk_t.stop();
	}

	@Transactional
	public void clear() {
		LOGGER.info("About to clear logs");
		final String sql = env.getProperty("sql.clear");
		jdbcTemplate.execute(sql);
		LOGGER.info("logs empty");
	}

	public void run(final LocalDateTime startDate, final LocalDateTime endDate, final Integer threshold)
			throws UnsupportedEncodingException {
		final Timer qry_timer = metrics.timer("Query threshold");
		final Object[] args = new Object[] { startDate, endDate, threshold };
		final String sql = env.getProperty("sql.threshold");
		final Timer.Context qry_t = qry_timer.time();
		final Object[][] result = jdbcTemplate.query(sql, args, (ResultSetExtractor<Object[][]>) rs -> {
			// ResultSetMetaData meta = rs.getMetaData();

			final List<String> l = new LinkedList<>();
			while (rs.next()) {
				l.add(ByteUtils.getIPfromInteger(rs.getInt(1), buffer));
			}

			final Object[][] f = l.stream().map(s -> new Object[] { s }).toArray(Object[][]::new);

			return f;
		});
		qry_t.stop();
		LOGGER.info("Threshold result:");
		if (result.length > 0) {
			final TextTable tt = new TextTable(columnNames, result);
			tt.setAddRowNumbering(true);
			// tt.addSeparatorPolicy(new LastRowSeparatorPolicy());
			tt.printTable();
			LOGGER.info("Blocking IPs");
			block(result, startDate, endDate, threshold);
		} else {
			LOGGER.info("Nothing found !");
		}

	}
}
