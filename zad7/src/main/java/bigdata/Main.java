public class Main {
  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "ChartsFmt":
        System.exit(ChartsFmt.run(args[1], args[2]));
        break;
      case "ChartsDailySum":
        System.exit(ChartsDailySum.run(args[1], args[2]));
        break;
      case "DailyCountryWeather":
        String tmpDir = String.format("/tmp-%d", System.currentTimeMillis());
        int r1 = DailyCountryWeather1.run(args[1], args[2], tmpDir);
        if (r1 != 0) System.exit(r1);
        System.exit(DailyCountryWeather2.run(tmpDir, args[3]));
        break;
      case "DailyCountryWeather1":
        System.exit(DailyCountryWeather1.run(args[1], args[2], args[3]));
        break;
      case "DailyCountryWeather2":
        System.exit(DailyCountryWeather2.run(args[1], args[2]));
        break;
      default:
        System.err.println("INVALID JOB");
        System.err.println("ChartsFmt <charts> <charts_fmt>");
        System.err.println("ChartsDailySum <charts_fmt> <charts_daily_sum>");
        System.err.println("DailyCountryWeather <daily_weather> <cities> <daily_country_weather>");
        System.err.println("\tDailyCountryWeather1 <daily_weather> <cities> <tmp>");
        System.err.println("\tDailyCountryWeather2 <tmp> <daily_country_weather>");
        System.exit(1);
        break;
    }
  }
}
