package il.technion.ewolf.kbr.cacheV2;

import il.technion.ewolf.kbr.Node;
import il.technion.ewolf.kbr.openkad.KadNode;

public class KadCacheV2Node extends KadNode{
	
	private RehashManager rehashManager;

	KadCacheV2Node() {
		super();
	}
	
	/**
	 * Sets the contained node. 
	 * Do not re-set the node after you start using this class
	 * 
	 * 
	 * @param node the wrapped node
	 * @return this for fluent interface
	 */
	public KadNode setNode(Node node) {
		this.node = node;
		rehashManager = new RehashManager(node.getKey());
		return this;
	}
	
	public String[] getRehashValuesAsStrings(){
		return rehashManager.getRehashValuesAsStrings();
	}

}
