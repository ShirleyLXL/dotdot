import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class RollingCountBolt extends BaseRichBolt{
	private OutputCollector collector;
	private static Map<String, Integer> objToCounter = new HashMap<String, Integer>();
	public RollingCountBolt() {
	}
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	public void execute(Tuple tuple) {
		String obj = tuple.getString(0);
		synchronized(objToCounter) {
			Integer curr = objToCounter.get(obj);
			if(curr == null) {
				curr = 0;
			}
			curr++;
			objToCounter.put(obj, curr);
			collector.emit(new Values(obj, curr));
			//collector.ack(tuple);
		}
		//Set<String> cSet = objToCounter.keySet();
		//for(String i:cSet) {
			//System.out.println("*************************"+i.toString()+objToCounter.get(i)+"************************");
		//}
	}
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("obj", "count"));
	}

}
