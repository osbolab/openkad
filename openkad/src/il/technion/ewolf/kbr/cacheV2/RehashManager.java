package il.technion.ewolf.kbr.cacheV2;

import il.technion.ewolf.kbr.Key;

public class RehashManager {
	private static final int[] moduloValues = {20, 20, 2, 2};
	private int[] rehashValues;
	
	public RehashManager(Key key){
		rehashValues = new int[moduloValues.length];
		for(int i=0 ; i<moduloValues.length ; i++){
			//TODO: rehash the key and only then modulo the value
			rehashValues[i] = ((int)key.getBytes()[0]) % moduloValues[i];
		}		
	}
	
	public String[] getRehashValuesAsStrings(){
		String[] results = new String[moduloValues.length];
		
		for(int i = moduloValues.length ; i>0 ; i--){
			String result = new String();
			for(int j = 0 ; j<i ; j++){
				result.concat(String.valueOf(rehashValues[j]));
				if(j!=i-1){
					result.concat(",");
				}				
			}
			results[moduloValues.length-i] = result;
		}
		return results;
	}
}
