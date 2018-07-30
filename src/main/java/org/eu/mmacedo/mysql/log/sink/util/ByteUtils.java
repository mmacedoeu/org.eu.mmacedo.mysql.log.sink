package org.eu.mmacedo.mysql.log.sink.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.dao.DataRetrievalFailureException;

public class ByteUtils {
	private static ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 2);

	public static String getIPfromInteger(final int ip, final ByteBuffer buffer) {
		buffer.putInt(ip);
		final byte[] b = buffer.array();
		InetAddress inetAddressRestored;
		try {
			inetAddressRestored = InetAddress.getByAddress(b);
		} catch (final UnknownHostException e) {
			throw new DataRetrievalFailureException("Invalid IP", e);
		} finally {
			buffer.clear();
		}
		return inetAddressRestored.getHostAddress();
	}

	public static Integer getIntFromIp(final String input) throws UnknownHostException {
		final InetAddress inetAddressOrigin = InetAddress.getByName(input);
		final int intRepresentation = ByteBuffer.wrap(inetAddressOrigin.getAddress()).getInt();
		return Integer.valueOf(intRepresentation);
	}

	public static byte[] generatePK(final UUID startId, final AtomicLong sequence) {
		final long msb = startId.getMostSignificantBits();
		final long lsb = startId.getLeastSignificantBits();
		final byte[] id = ByteUtils.longToBytes(msb, lsb + sequence.incrementAndGet());
		return id;
	}

	public static byte[] longToBytes(final long ms, final long ls) {
		buffer.putLong(0, ms);
		buffer.putLong(8, ls);
		return buffer.array().clone();
	}

	public static long getmostsignficant(final byte[] bytes) {
		buffer.put(bytes, 0, 4);
		buffer.flip();// need flip
		return buffer.getLong();
	}

	public static long getleastsignficant(final byte[] bytes) {
		buffer.put(bytes, 4, 4);
		buffer.flip();// need flip
		return buffer.getLong();
	}

	public static byte[] longToBytes(long l) {
		final byte[] result = new byte[8];
		for (int i = 7; i >= 0; i--) {
			result[i] = (byte) (l & 0xFF);
			l >>= 8;
		}
		return result;
	}

	public static long bytesToLong(final byte[] b) {
		long result = 0;
		for (int i = 0; i < 8; i++) {
			result <<= 8;
			result |= b[i] & 0xFF;
		}
		return result;
	}

	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();

	public static String bytesToHex(final byte[] bytes) {
		final char[] hexChars = new char[bytes.length * 2];
		for (int j = 0; j < bytes.length; j++) {
			final int v = bytes[j] & 0xFF;
			hexChars[j * 2] = hexArray[v >>> 4];
			hexChars[j * 2 + 1] = hexArray[v & 0x0F];
		}
		return new String(hexChars);
	}
}
