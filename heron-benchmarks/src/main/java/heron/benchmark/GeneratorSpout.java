package heron.benchmark;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class GeneratorSpout extends BaseRichSpout{
  private static Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
  private Map<String, Long> emitTimes = new HashMap<String, Long>();
  private List<Long> times = new ArrayList<Long>();
  private int maxSend;
  private int messagesSendCount = 0;
  private int ackCount = 0;
  private int messagesPerSecond = 0;
  private String saveFile, addsFile;
  private int increasingGapCount = 0;

  private String[] ad_types = {"banner", "modal", "sponsored-search", "mail", "mobile"};
  private String[] event_types = {"view", "click", "purchase"};
  private String[] userIds = new String[100];
  private String[] pageIds = new String[100];
  private String[] ads;
  private boolean debug;
  private int printInterval;

  private long sendGap = 0;

  private Random random;
  private SpoutOutputCollector collector;
  private long lastSendTime;

  private void generateIds() {
    for (int i = 0; i < userIds.length; i++) {
      userIds[i] = UUID.randomUUID().toString();
    }
    for (int i = 0; i < pageIds.length; i++) {
      pageIds[i] = UUID.randomUUID().toString();
    }
  }

  private String getRandom(String []list) {
    int r = random.nextInt(list.length);
    return list[r];
  }

  @Override
  public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    this.collector = spoutOutputCollector;
    maxSend = (Integer) map.get("message.count");
    messagesPerSecond = (Integer) map.get("message.rate");
    saveFile = (String) map.get("save.file");
    addsFile = (String) map.get("ads.file");
    debug = (Boolean) map.get("debug");
    printInterval = (Integer) map.get("print.interval");

    List<String> adsList = readAdsFile(addsFile);
    ads = adsList.toArray(new String[adsList.size()]);

    sendGap = 1000000000 / messagesPerSecond;
    lastSendTime = System.nanoTime();
    random = new Random();
    generateIds();
  }

  @Override
  public void nextTuple() {
    long now = System.nanoTime();
    if (messagesSendCount < maxSend && (sendGap < now - lastSendTime)) {
      lastSendTime = now;
      if (sendGap * 2 < (now - lastSendTime)) {
        increasingGapCount++;
        if (increasingGapCount > 10) {
          LOG.warn("Increasing gap between the messages");
        }
      } else {
        increasingGapCount = 0;
      }

      String user_id = getRandom(userIds);
      String page_id = getRandom(pageIds);
      String addType = getRandom(ad_types);
      String eventType = getRandom(event_types);
      String value = "{\"user_id\": \"" + user_id +
      "\", \"page_id\": \"" + page_id +
      "\", \"ad_id\": \"" + getRandom(ads) +
      "\", \"ad_type\": \"" + addType +
      "\", \"event_type\": \"" + eventType +
      "\", \"event_time\": \"" + System.currentTimeMillis() +
      "\", \"ip_address\": \"1.2.3.4\"}";
      messagesSendCount++;

      String id = UUID.randomUUID().toString();
      long emitTime = System.nanoTime();

      if (eventType.equals("view")) {
        emitTimes.put(id, emitTime);
      }

      collector.emit(new Values(value), id);

      if (debug && messagesSendCount % printInterval == 0) {
        LOG.info("Message count: " + messagesSendCount);
      }
    }
  }

  private void writeResults() {
    PrintWriter writer = null;
    try {
      writer = new PrintWriter(new FileWriter(saveFile));
      for(Long str: times) {
        writer.println(str);
      }
      writer.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private List<String> readAdsFile(String file) {
    List<String> ads = new ArrayList<String>();
    try (BufferedReader br = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = br.readLine()) != null) {
        // process the line.
        String[] split = line.split(",");
        ads.add(split[0]);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ads;
  }

  @Override
  public void ack(Object o) {
    ackCount++;

    if (debug && ackCount % printInterval == 0) {
      LOG.info("Ack count: " + ackCount);
    }

    Long time = emitTimes.remove(o.toString());
    if (time != null) {
      times.add(System.nanoTime() - time);
    }

    if (messagesSendCount == maxSend && ackCount == maxSend) {
      writeResults();
    }
    super.ack(o);
  }

  @Override
  public void fail(Object o) {
    ackCount++;
    System.out.println("Tuple failed: " + o.toString());

    Long time = emitTimes.remove(o.toString());
    if (time != null) {
      times.add(System.nanoTime() - time);
    }

    if (messagesSendCount == maxSend && ackCount == maxSend) {
      writeResults();
    }
    super.fail(o);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("data"));
  }
}
