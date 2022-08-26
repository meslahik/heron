package ch.usi.dslab.lel.ramcast.endpoint;

import java.nio.ByteBuffer;

public interface CustomHandler {
  default void handleReceive(Object data) {}

  default void handleSendComplete(Object data) {}

  default void handleReceiveMessage(Object data) {}

  default void handlePermissionError(ByteBuffer buffer) {}
}
