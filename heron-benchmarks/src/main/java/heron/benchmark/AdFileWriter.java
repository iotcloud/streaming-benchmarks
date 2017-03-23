package heron.benchmark;

import benchmark.common.Utils;
import org.apache.commons.cli.*;
import redis.clients.jedis.Jedis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class AdFileWriter {
  private String redisServerHost;
  private Jedis jedis;
  private String saveFile;

  public AdFileWriter(String redisServerHost, String saveFile) {
    this.redisServerHost = redisServerHost;
  }

  public List<String> readCampaigns(String redisServerHost) {
    Set<String> campaigns = jedis.smembers("campaigns");
    Map<String, String> adsToCampaigns = new HashMap<String, String>();
    int total = campaigns.size() * 10;
    for (String c : campaigns) {
      for (int i = 0; i < 10; i++) {
        String ad = UUID.randomUUID().toString();
        jedis.set(ad, c);
        adsToCampaigns.put(ad, c);
      }
    }
    return null;
  }

  private void writeAdsToCampaigns(Map<String, String> adsToCampaigns) {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(new FileWriter(saveFile));
      for (Map.Entry<String, String> e : adsToCampaigns.entrySet()) {
        writer.println(e.getKey() + "," + e.getValue());
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws ParseException {
    Options opts = new Options();
    opts.addOption("conf", true, "Path to the config file.");

    CommandLineParser parser = new DefaultParser();
    CommandLine cmd = parser.parse(opts, args);
    String configPath = cmd.getOptionValue("conf");
    Map commonConfig = Utils.findAndReadConfigFile(configPath, true);

    String redisServerHost = (String)commonConfig.get("redis.host");
  }
}
