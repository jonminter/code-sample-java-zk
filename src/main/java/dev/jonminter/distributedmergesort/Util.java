package dev.jonminter.distributedmergesort;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class Util {
  public static class Tuple<X,Y> {
    private final X t1;
    private final Y t2;

    public Tuple(X t1, Y t2) {
      this.t1 = t1;
      this.t2 = t2;
    }

    public X getT1() {
      return t1;
    }

    public Y getT2() {
      return t2;
    }
  }
  public static byte[] serializeAsBytes(List<String> stringList) {
    Gson gson = new Gson();
    String json = gson.toJson(stringList);
    return serializeStringToBytes(json);
  }

  public static List<String> deserializeBytes(byte[] b) {
    String json = deserializeBytesToString(b);
    Gson gson = new Gson();
    return gson.fromJson(json, new TypeToken<List<String>>(){}.getType());
  }

  public static byte[] serializeStringToBytes(String s) {
    Charset utf8 = StandardCharsets.UTF_8;
    return utf8.encode(s).array();
  }

  public static String deserializeBytesToString(byte[] b) {
    Charset utf8 = StandardCharsets.UTF_8;
    return utf8.decode(ByteBuffer.wrap(b)).toString();
  }
}
