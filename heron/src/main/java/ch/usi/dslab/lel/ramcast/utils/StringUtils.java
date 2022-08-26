package ch.usi.dslab.lel.ramcast.utils;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class StringUtils {

  // calculate checksum of buffer from [position - limit]
  public static long calculateCrc32(ByteBuffer buffer) {
    CRC32 checksum = new CRC32();
    checksum.update(buffer);
    return checksum.getValue();
  }

  private static int getMaxLength(String... strings) {
    int len = Integer.MIN_VALUE;
    for (String str : strings) {
      len = Math.max(str.length(), len);
    }
    return len;
  }

  private static String padString(String str, int len) {
    StringBuilder sb = new StringBuilder(str);
    return sb.append(fill(' ', len - str.length())).toString();
  }

  private static String fill(char ch, int len) {
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) {
      sb.append(ch);
    }
    return sb.toString();
  }

  public static String formatMessage(String... strings) {
    String ret = "\n";
    int maxBoxWidth = getMaxLength(strings);
    ret += "╔" + fill('═', maxBoxWidth) + "╗\n";
    for (String str : strings) {
      ret += String.format("║%s║%n", padString(str, maxBoxWidth));
    }
    ret += "╚" + fill('═', maxBoxWidth) + "╝\n";
    return ret;
  }
}
