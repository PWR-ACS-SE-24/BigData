public class Main {
  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "ChartsFmt":
        ChartsFmt.run(args[1], args[2]);
        break;
      case "ChartsDailySum":
        ChartsDailySum.run(args[1], args[2]);
        break;
      default:
        System.err.println("INVALID JOB");
    }
  }
}
