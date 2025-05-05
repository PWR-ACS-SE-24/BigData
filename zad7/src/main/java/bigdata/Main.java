public class Main {
  public static void main(String[] args) throws Exception {
    switch (args[0]) {
      case "ChartsFmt":{
        String charts = args.length >= 2 ? args[1] : "/input/charts_2017.csv";
        String chartsFmt = args.length >= 3 ? args[2] : "/charts_fmt";
        System.exit(ChartsFmt.run(charts, chartsFmt));
        break;}
      case "ChartsDailySum":{
        String chartsFmt = args.length >= 2 ? args[1] : "/charts_fmt";
        String chartsDailySum = args.length >= 3 ? args[2] : "/charts_daily_sum";
        System.exit(ChartsDailySum.run(chartsFmt, chartsDailySum));
        break;}
      case "DailyCountryWeather":{
        String dailyWeather = args.length >= 2 ? args[1] : "/input/daily_weather_2017.csv";
        String cities = args.length >= 3 ? args[2] : "/input/cities.csv";
        String dailyCountryWeather = args.length >= 4 ? args[3] : "/daily_country_weather";
        String tmpDir = String.format("/tmp-%d", System.currentTimeMillis());
        int r1 = DailyCountryWeather1.run(dailyWeather, cities, tmpDir);
        if (r1 != 0) System.exit(r1);
        System.exit(DailyCountryWeather2.run(tmpDir, dailyCountryWeather));
        break;}
      case "DailyCountryWeather1":{
        String dailyWeather = args.length >= 2 ? args[1] : "/input/daily_weather_2017.csv";
        String cities = args.length >= 3 ? args[2] : "/input/cities.csv";
        String dailyCountryWeather1 = args.length >= 4 ? args[3] : "/daily_country_weather_1";
        System.exit(DailyCountryWeather1.run(dailyWeather, cities, dailyCountryWeather1));
        break;}
      case "DailyCountryWeather2":{
        String dailyCountryWeather1 = args.length >= 2 ? args[1] : "/daily_country_weather_1";
        String dailyCountryWeather2 = args.length >= 3 ? args[2] : "/daily_country_weather_2";
        System.exit(DailyCountryWeather2.run(dailyCountryWeather1, dailyCountryWeather2));
        break;}
      default:{
        System.err.println("INVALID JOB");
        System.err.println("ChartsFmt <charts> <charts_fmt>");
        System.err.println("ChartsDailySum <charts_fmt> <charts_daily_sum>");
        System.err.println("DailyCountryWeather <daily_weather> <cities> <daily_country_weather>");
        System.err.println("\tDailyCountryWeather1 <daily_weather> <cities> <tmp>");
        System.err.println("\tDailyCountryWeather2 <tmp> <daily_country_weather>");
        System.exit(1);
        break;}
    }
  }
}
