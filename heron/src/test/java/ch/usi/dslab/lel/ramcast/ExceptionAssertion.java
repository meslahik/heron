package ch.usi.dslab.lel.ramcast;

public class ExceptionAssertion {
  public static void assertDoesNotThrow(FailingRunnable action) {
    try {
      action.run();
    } catch (Exception ex) {
      throw new Error("expected action not to throw, but it did!", ex);
    }
  }
}

@FunctionalInterface
interface FailingRunnable {
  void run() throws Exception;
}
