package heron.benchmark;

import benchmark.common.Utils;
import org.apache.commons.cli.*;
import redis.clients.jedis.Jedis;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class AdFileWriter {
  private Jedis jedis;
  private String saveFile;

  public AdFileWriter(String redisServerHost, String saveFile) {
    this.jedis = new Jedis(redisServerHost);
    this.saveFile = saveFile;
  }

  public Map<String, String> newSetup() {
    String[] campaigns = new String[100];
    for (int i = 0; i < 100; i++) {
      campaigns[i] = UUID.randomUUID().toString();
    }
    jedis.sadd("campaigns", campaigns);

    Set<String> campaignSet = jedis.smembers("campaigns");
    Map<String, String> adsToCampaigns = new HashMap<String, String>();
    for (String c : campaignSet) {
      for (int i = 0; i < 10; i++) {
        String ad = UUID.randomUUID().toString();
        jedis.set(ad, c);
        adsToCampaigns.put(ad, c);
      }
    }

    return adsToCampaigns;
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
    String adsFile = (String) commonConfig.get("ads.file");

    AdFileWriter adFileWriter = new AdFileWriter(redisServerHost,adsFile);
    Map<String,String> adsToCampaigns = adFileWriter.newSetup();
    adFileWriter.writeAdsToCampaigns(adsToCampaigns);
  }
}
