package gov.pentagon.utils;

import java.util.HashSet;
import java.util.Set;

public final class Utils {
	public static byte[] joinByteArrays(byte[]... arrays) {
		int capacity = 0;
		for (byte[] array : arrays) {
			capacity += array.length;
		}
		byte[] result = new byte[capacity];
		capacity = 0;
		for (byte[] array : arrays) {
			for (int i = 0; i < array.length; i++)
				result[capacity + i] = array[i];
			capacity += array.length;
		}
		return result;
	}

	public static <T> boolean doSetsOverlap(Set<? extends T> t1, Set<? extends T> t2) {
		if (t1.isEmpty() || t2.isEmpty())
			return false;

		HashSet<T> a = new HashSet<T>(t1);
		a.retainAll(t2);
		return a.isEmpty();
	}
}
