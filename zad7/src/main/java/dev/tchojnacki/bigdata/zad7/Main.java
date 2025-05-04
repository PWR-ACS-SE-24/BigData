public class Main {
  public static void main(String[] args) throws Exception {
    if (args[0].equals("ChartsFmt")) {
      ChartsFmt.run(args[1], args[2]);
    } else {
      System.err.println("INVALID JOB");
    }
  }
}
