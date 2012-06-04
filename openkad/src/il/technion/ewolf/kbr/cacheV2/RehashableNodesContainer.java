package il.technion.ewolf.kbr.cacheV2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RehashableNodesContainer{

	private HashMap<String, List<KadCacheV2Node>> nodesHash = new HashMap<String, List<KadCacheV2Node>>();

	public void insertNode(KadCacheV2Node node){
		String[] hashKeys = node.getRehashValuesAsStrings();
		for(int i=0 ; i<hashKeys.length ; i++){
			List<KadCacheV2Node> nodesList = nodesHash.get(hashKeys[i]);
			if(nodesList==null){
				List<KadCacheV2Node> newList = new ArrayList<KadCacheV2Node>();
				newList.add(node);
				nodesHash.put(hashKeys[i], newList);
			}else{
				if(!nodesList.contains(node)){
					nodesList.add(node);
				}
			}
		}
	}

	public void removeNode(KadCacheV2Node node){
		String[] hashKeys = node.getRehashValuesAsStrings();
		for(int i=0 ; i<hashKeys.length ; i++){
			List<KadCacheV2Node> nodesList = nodesHash.get(hashKeys[i]);
			if(nodesList!=null){
				nodesList.remove(node);
			}
		}
	}
}
