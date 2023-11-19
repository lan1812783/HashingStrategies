package hashingstrategies;

import java.util.HashMap;
import java.util.Map;

public class Server<KType, VType> {

	private String _ip;
	private Integer _weight;
	private Map<KType, VType> _db;

	public Server(String ip, Integer weight) {
		this(ip, weight, null);
	}

	public Server(String ip, Integer weight, Map<KType, VType> db) {
		this._ip = ip;
		this._weight = weight;
		this._db = db == null ? new HashMap<>() : db;
	}

	public void put(KType key, VType value) {
		this._db.put(key, value);
	}

	public VType get(KType key) {
		return this._db.get(key);
	}

	public String getIp() {
		return this._ip;
	}

	public Integer getWeight() {
		return this._weight;
	}

	public Map<KType, VType> getDB() {
		return this._db;
	}
}
