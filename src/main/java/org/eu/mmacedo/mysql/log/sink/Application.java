package org.eu.mmacedo.mysql.log.sink;

import static pl.touk.throwing.ThrowingSupplier.unchecked;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eu.mmacedo.mysql.log.sink.service.InboundFileSource;
import org.eu.mmacedo.mysql.log.sink.service.OutboundMysqlSink;
import org.eu.mmacedo.mysql.log.sink.service.QueryThresholdService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

@SpringBootApplication
public class Application {

	private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

	private static final String DISABLE = "ALTER TABLE log DISABLE KEYS";

	private static final String ENABLE = "ALTER TABLE log ENABLE KEYS";

	private static final String[] DURATIONS = { "hourly", "daily" };

	private static ConsoleReporter startReport(final MetricRegistry metrics) {
		final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics).convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.SECONDS).build();
		reporter.start(60, TimeUnit.SECONDS);
		return reporter;
	}

	/**
	 * Used to gather metrics
	 *
	 * @return
	 */
	@Bean
	public MetricRegistry metrics() {
		return new MetricRegistry();
	}

	/**
	 * User for signaling end of sink processing
	 *
	 * @return
	 */
	@Bean
	public CountDownLatch countDownLatch() {
		return new CountDownLatch(1);
	}

	/**
	 * Main thread pool
	 *
	 * @return
	 */
	@Bean
	public ExecutorService theExecutor() {
		return Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}

	/**
	 * Queue for batch inserts
	 *
	 * @return
	 */
	@Bean
	public LinkedTransferQueue<Optional<List<Map<Integer, CompletableFuture<?>>>>> batchQueue() {
		return new LinkedTransferQueue<>();
	}

	private static void disableKeys(final JdbcTemplate jdbcTemplate) {
		jdbcTemplate.execute(DISABLE);
	}

	private static void enableKeys(final JdbcTemplate jdbcTemplate) {
		jdbcTemplate.execute(ENABLE);
	}

	public static void main(final String[] args) throws Exception {
		final Options options = new Options();
		final Option startDateOption = Option.builder().longOpt("startDate").hasArg().desc("Start Date in ").build();
		final Option duration = Option.builder().longOpt("duration").hasArg().required()
				.desc("Duration interval (hourly or daily)").build();
		final Option thresholdOption = Option.builder().longOpt("threshold").hasArg().required()
				.desc("Threshold limit (integer >0) ").build();
		final Option url = Option.builder().longOpt("spring.datasource.url").hasArg().desc("database url").build();
		final Option file = Option.builder().longOpt("accesslog").hasArg().desc("Access file to process").build();
		final Option ansi = Option.builder().longOpt("spring.output.ansi.enabled").hasArg().build();
		options.addOption("c", "clear", false, "empty database before processing");
		options.addOption("q", "query", false, "no processing sink, just query");
		options.addOption("h", "help", false, "show help.");
		options.addOption(startDateOption);
		options.addOption(duration);
		options.addOption(thresholdOption);
		options.addOption(url);
		options.addOption(file);
		options.addOption(ansi);

		final Options optionsHelp = new Options();
		final Option durationHelp = Option.builder().longOpt("duration").hasArg()
				.desc("Duration interval (hourly or daily)").build();
		final Option thresholdHelp = Option.builder().longOpt("threshold").hasArg()
				.desc("Threshold limit (integer >0) ").build();
		optionsHelp.addOption("c", "clear", false, "clear logs database before processing");
		optionsHelp.addOption("q", "query", false, "no processing sink, just query");
		optionsHelp.addOption("h", "help", false, "show help.");
		optionsHelp.addOption(startDateOption);
		optionsHelp.addOption(durationHelp);
		optionsHelp.addOption(thresholdHelp);
		optionsHelp.addOption(url);
		optionsHelp.addOption(file);
		optionsHelp.addOption(ansi);
		final CommandLineParser help = new DefaultParser();

		try {
			final CommandLine lineHelp = help.parse(optionsHelp, args);
			if (lineHelp.hasOption("h")) {
				final HelpFormatter formater = new HelpFormatter();
				formater.printHelp("Mysql.Log.Sink", options);
				System.exit(0);
			}

			final CommandLineParser parser = new DefaultParser();
			final CommandLine line = parser.parse(options, args);
			String dstring = "hourly";
			if (line.hasOption("duration")) {
				dstring = line.getOptionValue("duration").trim().toLowerCase();
				if (!Arrays.asList(DURATIONS).contains(dstring)) {
					LOGGER.error("Duration should be either hourly or daily: " + dstring);
					final HelpFormatter formater = new HelpFormatter();
					formater.printHelp("Mysql.Log.Sink", options);
					System.exit(0);
				}
				LOGGER.info("Duration: \t {}", dstring);
			}
			int threshold = 0;
			if (line.hasOption("threshold")) {
				final String tstring = line.getOptionValue("threshold");
				try {
					threshold = Integer.parseInt(tstring);
				} catch (final NumberFormatException e) {
					LOGGER.error("Threshold should be integer: " + tstring);
					final HelpFormatter formater = new HelpFormatter();
					formater.printHelp("Mysql.Log.Sink", options);
					System.exit(0);
				}
				LOGGER.info("Threshold: \t {}", tstring);
			}

			final ConfigurableApplicationContext ctx = SpringApplication.run(Application.class, args);
			final Environment env = ctx.getBean(Environment.class);
			LocalDateTime startDate = null;
			if (line.hasOption("startDate")) {
				try {
					startDate = LocalDateTime.parse(line.getOptionValue("startDate"), QueryThresholdService.formatter);
				} catch (final DateTimeParseException e) {
					LOGGER.error("Parsing failed.  Reason: " + e.getMessage());
					final HelpFormatter formater = new HelpFormatter();
					formater.printHelp("Mysql.Log.Sink", options);
					System.exit(0);
				}
			} else {
				try {
					startDate = LocalDateTime.parse(env.getProperty("startDate"), QueryThresholdService.formatter);
				} catch (final DateTimeParseException e) {
					LOGGER.error("Parsing failed.  Reason: " + e.getMessage());
					final HelpFormatter formater = new HelpFormatter();
					formater.printHelp("Mysql.Log.Sink", options);
					System.exit(0);
				}
			}

			final MetricRegistry metrics = ctx.getBean(MetricRegistry.class);
			final ConsoleReporter reporter = startReport(metrics);

			final JdbcTemplate jdbcTemplate = ctx.getBean(JdbcTemplate.class);
			final QueryThresholdService qry = ctx.getBean(QueryThresholdService.class);

			if (line.hasOption("c")) {
				qry.clear();
			}

			String accesslog = env.getProperty("accesslog");
			if (line.hasOption("accesslog")) {
				final String fname = line.getOptionValue("accesslog");
				final File f = new File(fname);
				if (!f.exists() || f.isDirectory() || !f.canRead()) {
					LOGGER.error("invalid file: " + fname);
					final HelpFormatter formater = new HelpFormatter();
					formater.printHelp("Mysql.Log.Sink", options);
					System.exit(0);
				} else {
					accesslog = fname;
				}
			} else {
				final File f = new File(accesslog);
				if (!f.exists() || f.isDirectory() || !f.canRead()) {
					LOGGER.error("invalid file: " + accesslog);
					final HelpFormatter formater = new HelpFormatter();
					formater.printHelp("Mysql.Log.Sink", options);
					System.exit(0);
				}
			}

			if (!line.hasOption("q")) {
				final Timer timer = metrics.timer("Bulk insert");
				disableKeys(jdbcTemplate);
				LOGGER.info("disabled keys for faster bulk insert");
				final ExecutorService theExecutor = ctx.getBean(ExecutorService.class);
				final OutboundMysqlSink sink = ctx.getBean(OutboundMysqlSink.class);
				CompletableFuture.supplyAsync(unchecked(() -> {
					return sink.run();
				}), theExecutor);

				final InboundFileSource in = ctx.getBean(InboundFileSource.class);
				LOGGER.info("bulk insert started");
				final Timer.Context bulk = timer.time();
				in.run(accesslog);
				bulk.stop();
				LOGGER.info("bulk insert finished");
				enableKeys(jdbcTemplate);
				LOGGER.info("keys for table log re-enabled");
			} else {
				LOGGER.info("By passing bulk insert");
			}

			LocalDateTime endDate;
			if ("hourly".equals(dstring)) {
				endDate = startDate.plusHours(1);
			} else {
				endDate = startDate.plusDays(1);
			}
			qry.run(startDate, endDate, threshold);
			LOGGER.info("Stats: \n");
			reporter.report();
			ctx.close();
		} catch (final ParseException exp) {
			LOGGER.error("Parsing failed.  Reason: " + exp.getMessage());
			final HelpFormatter formater = new HelpFormatter();
			formater.printHelp("Mysql.Log.Sink", options);
			System.exit(0);
		}
	}

}
