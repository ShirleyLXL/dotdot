package DSPPCode.storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;
import java.util.Map.Entry;

/**
 * Tweeter的热门话题Top N
 * -
 * 输入：
 * 该Bolt接收的每个tuple即为一条tweet，tweet的形式如下
 * Good Morning #monday #Saturday, Hello!
 * 其中紧跟着"#"的单词为话题，不包含逗号，即这条话题包含monday和Saturday两条话题
 * 当且仅当接收的tuple内容为字符串stop!时，输出Top N结果，即执行reportTopNToPrinter
 * 否则处理对接收到的tweet进行话题抽取与统计，即执行processTweet
 * -
 * 输出：
 * Top N个话题以及该话题出现的次数(区分大小写)，譬如
 * Saturday,1
 * monday,1
 * 每个记录占据一行
 * 输出的结果
 *
 * PS:
 * TweetSpout与PrinterBolt均已实现好
 */

public class TopicCountBolt extends BaseRichBolt {
    private int topN;
    private OutputCollector outputCollector;
    private Map<String, Integer> counter;
    

    public TopicCountBolt(int topN){
        this.topN = topN;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector=outputCollector;
        counter = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String tweet = tuple.getString(0);
        // 如果接收到stop，则计算Top N个topic，并将结果按照格式要求发送给Printer
        if(tweet.equals("stop!")){
            reportTopNToPrinter();
        }
        // 否则，正常处理tweet，即提取话题并更新统计
        else {
            processTweet(tweet);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("result"));
    }


    private void processTweet(String tweet){
        // TODO 请完成该函数
    	String[] tokens = tweet.split(" ");
    	for(String token: tokens) { 
    		if(token.length() < 1)	continue;
    		if(token.substring(0,1).equals("#")) {
    			String topic = token.substring(1,token.length()).trim().replaceAll("\\pP","");
    			if(counter.containsKey(topic)) {
    	    		counter.put(topic, counter.get(topic)+1);
    	    	}
    			else {
    				counter.put(topic, 1);
    			}
    		}
    	}
    	
    		
    }

    private void reportTopNToPrinter(){
        // TODO 请完成该函数
    	//List<Tuple> result = new ArrayList<Tuple>();
    	
//    	Collection<Integer> c = counter.values();
//    	Object[] obj = c.toArray();
//    	Arrays.sort(obj);
    	
    	System.out.println(counter);
    	ArrayList<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(counter.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            @Override
            public int compare(Entry<java.lang.String, Integer> arg0,
                    Entry<java.lang.String, Integer> arg1) {
                return arg1.getValue() - arg0.getValue();
            }
        });
        int i = 0;
        String result = "";
        for(Map.Entry<String,Integer> mapping: list){ 
            System.out.println(mapping.getKey()+":"+mapping.getValue()); 
            i++;
            result += mapping.getKey() + "," + mapping.getValue() + "\n";
            if(i==topN)	break;
            
        }
        outputCollector.emit(new Values(result));

        
    	
    	
    }
}
