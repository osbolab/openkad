package il.technion.ewolf.kbr.openkad;

import il.technion.ewolf.kbr.Node;

import java.util.List;


public interface NodeStorage {
	
	public void registerIncomingMessageHandler();
	public List<Node> getAllNodes();

}
