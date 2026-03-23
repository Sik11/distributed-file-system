import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

public abstract class Connection implements Runnable {
  Socket input;
  Socket output;

  BufferedReader bufferedReader;
  PrintWriter printWriter;

  protected Connection(Socket input, Socket output) throws Exception {
    this.input = input;
    this.output = output;
    this.bufferedReader = new BufferedReader(new InputStreamReader(input.getInputStream()));
    this.printWriter = new PrintWriter(output.getOutputStream());
  }

  public void close() throws Exception {
    this.input.close();
    this.output.close();
  }

  public String read() {
    String message = null;

    try {
      message = bufferedReader.readLine();
    } catch (Exception e) {}

    return message;
  }

  public String read(Integer timeout) {
    String message = null;

    try {
      this.input.setSoTimeout(timeout);
      message = bufferedReader.readLine();
    } catch (Exception e) {}

    try {
      this.input.setSoTimeout(0);
    } catch (Exception e) {}

    return message;
  }

  public void send(String message) {
    this.printWriter.println(message);
    this.printWriter.flush();
  }

  @Override
  public void run() {
    try {
      String message = "";
      do {
        message = bufferedReader.readLine();
        if (message != null)
          handleMessage(message);
      } while (message != null);
      close();
    } catch (Exception e) {
    }

    onDisconnect();
  }

  public Socket getInputSocket() {
    return input;
  }
  
  public Socket getOutputSocket() {
    return output;
  }

  protected abstract void handleMessage(String message);

  protected abstract void onDisconnect();
}
