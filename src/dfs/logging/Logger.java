package dfs.logging;

import java.io.IOException;
import java.io.PrintStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

public abstract class Logger {
   protected static final String ERROR_LOG_MSG_SUFFIX = "ERROR: ";
   private static final Path LOG_DIR = Path.of("var", "logs");
   protected final LoggingType loggingType;
   protected PrintStream ps;

   protected Logger(LoggingType loggingType) {
      this.loggingType = loggingType;
   }

   protected abstract String getLogFileSuffix();

   protected synchronized PrintStream getPrintStream() throws IOException {
      if (this.ps == null) {
         Files.createDirectories(LOG_DIR);
         this.ps = new PrintStream(LOG_DIR.resolve(
             this.getLogFileSuffix() + "_" + System.currentTimeMillis() + ".log").toFile());
      }

      return this.ps;
   }

   protected boolean logToFile() {
      return this.loggingType == LoggingType.ON_FILE_ONLY || this.loggingType == LoggingType.ON_FILE_AND_TERMINAL;
   }

   protected boolean logToTerminal() {
      return this.loggingType == LoggingType.ON_TERMINAL_ONLY || this.loggingType == LoggingType.ON_FILE_AND_TERMINAL;
   }

   public void log(String message) {
      if (this.logToFile()) {
         try {
            this.getPrintStream().println(message);
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

      if (this.logToTerminal()) {
         System.out.println(message);
      }

   }

   public void connectionAccepted(Socket socket) {
      this.log("Connection accepted from port ".concat(String.valueOf(socket.getPort())));
   }

   public void connectionEstablished(Socket socket) {
      this.log("Connection established to port ".concat(String.valueOf(socket.getPort())));
   }

   public void messageSent(Socket destination, String message) {
      this.log("Message sent to port " + destination.getPort() + " from " + destination.getLocalPort() +
          " : " + message);
   }

   public void messageReceived(Socket source, String message) {
      this.log("Message received at " + source.getLocalPort() + " from port " + source.getPort() + ": " +
          message);
   }

   public void timeoutExpiredWhileReading(Socket socket) {
      this.log("Timeout expired while reading from port ".concat(String.valueOf(socket.getPort())));
   }

   public void error(String message) {
      this.log("ERROR: ".concat(String.valueOf(message)));
   }

   public static enum LoggingType {
      NO_LOG,
      ON_TERMINAL_ONLY,
      ON_FILE_ONLY,
      ON_FILE_AND_TERMINAL
   }
}
