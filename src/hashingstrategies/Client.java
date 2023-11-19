package hashingstrategies;

import hashingstrategies.utils.LoggerFactory;
import java.lang.ref.WeakReference;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import org.apache.log4j.Logger;

public class Client<KType, VType> {

	private static final Class _ThisClass = Client.class;
	private static final Logger _Logger = LoggerFactory.getLogger(_ThisClass);

	public static final int RENDEZVOUS_HASH = 4; // defualt, server unordering tolerant
	public static final int JUMP_CONSISTENT_HASH = 5; // server order sensitive
	public static final int MAGLEV_HASH = 6; // server order minorly sensitive

	private List<Server<KType, VType>> _servers;
	private int _hashAlgo;
	private int _totalKeys;
	private List<List<Integer>> _maglevPermutation;
	private List<WeakReference<Server<KType, VType>>> _maglevTable;
	private static final int _DEF_MAGLEV_TABLE_SIZE = 65537; // 65537, 655373
	private static ThreadLocal<MessageDigest> MD5 = new ThreadLocal<MessageDigest>() {
		@Override
		protected final MessageDigest initialValue() {
			try {
				return MessageDigest.getInstance("MD5");
			} catch (NoSuchAlgorithmException e) {
				throw new IllegalStateException("++++ no md5 algorythm found");
			}
		}
	};

	public Client(Map<String, Integer> ips2Weights) {
		this(ips2Weights, RENDEZVOUS_HASH);
	}

	public Client(Map<String, Integer> ips2Weights, int hashAlgo) {
		assert (!ips2Weights.isEmpty());

		this._hashAlgo = hashAlgo;
		this._servers = new ArrayList<>();
		this._totalKeys = 0;
		for (Map.Entry<String, Integer> ipWeight : ips2Weights.entrySet()) {
			this._servers.add(new Server<>(ipWeight.getKey(), ipWeight.getValue()));
		}

		if (hashAlgo == MAGLEV_HASH) {
			this.populateMaglevPermutation();
			this.populateMaglevTable();
		}
	}

	public void put(KType key, VType value) {
		this._totalKeys += 1;
		Server<KType, VType> server = this.getServer(key);
		if (server == null) {
			_Logger.error("Not found any server associating with key " + key);
			return;
		}
		server.put(key, value);
	}

	public VType get(KType key) {
		Server<KType, VType> server = this.getServer(key);
		if (server == null) {
			_Logger.error("Not found any server associating with key " + key);
			return null;
		}
		return server.get(key);
	}

	public Server<KType, VType> addServer(String ip, Integer weight) {
		Server<KType, VType> server = new Server<>(ip, weight);
		this.addServer(server);
		return server;
	}

	public Server<KType, VType> addServer(String ip, Integer weight, Map<KType, VType> db) {
		Server<KType, VType> server = new Server<>(ip, weight, db);
		this.addServer(server);
		return server;
	}

	private void addServer(Server<KType, VType> server) {
		this._servers.add(server);

		if (this._hashAlgo == MAGLEV_HASH) {
			this.populateMaglevPermutation();
			this.populateMaglevTable();
		}

		_Logger.info(
				"[Add server]"
				+ " ip: " + server.getIp()
				+ " - weight: " + server.getWeight()
				+ " - db size: " + server.getDB().size());
	}

	public Server<KType, VType> removeServer(String ip) {
		for (int i = 0; i < this._servers.size(); i++) {
			Server<KType, VType> server = this._servers.get(i);
			if (server.getIp() == ip) {
				Server<KType, VType> removedServer = this._servers.remove(i);

				if (this._hashAlgo == MAGLEV_HASH) {
					this._maglevPermutation.remove(i);
					this.populateMaglevTable();
				}

				_Logger.info(
						"[Remove server]"
						+ " ip: " + server.getIp()
						+ " - weight: " + server.getWeight()
						+ " - db size: " + server.getDB().size());

				return removedServer;
			}
		}
		return null;
	}

	public void printServer() {
		this.printServer(false);
	}

	public void printServer(boolean keyDistribution) {
		int i = 0;
		String msg = "Server info:";
		double stdDeviation = 0;
		for (Server<KType, VType> server : this._servers) {
			msg
					+= "\n\t"
					+ "Index: " + i++
					+ " - ip: " + server.getIp()
					+ " - weight: " + server.getWeight()
					+ " - db size: " + server.getDB().size();
			if (keyDistribution) {
				int dbKeyCount = server.getDB().size();
				stdDeviation += Math.pow(Math.abs(dbKeyCount - (double) this._totalKeys / this._servers.size()), 2);
				msg
						+= " - key fraction: "
						+ Math.round(dbKeyCount / (double) this._totalKeys * 10000) / 100.0 + "%";
			}
		}
		stdDeviation = Math.sqrt(stdDeviation / this._servers.size());
		double deviationPercentage = Math.round(stdDeviation / this._totalKeys * 10000) / 100.0;
		if (keyDistribution) {
			msg
					+= "\n\t-> Standard deviation: " + stdDeviation
					+ "\n\t-> Deviation percentage: " + deviationPercentage + "%";
		}
		_Logger.info(msg);
	}

	public void printMaglevPermutation() {
		String info = "Maglev permuation table:";
		for (int i = 0; i < this._maglevPermutation.size(); i++) {
			Server<KType, VType> server = this._servers.get(i);
			List<Integer> serverPermutation = this._maglevPermutation.get(i);

			info += "\n\t" + server.getIp() + (serverPermutation.size() > 0 ? " -" : "");
			for (int j = 0; j < serverPermutation.size(); j++) {
				info += " " + serverPermutation.get(j);
			}
		}
		_Logger.debug(info);
	}

	public void printMaglevTable() {
		String info = "Maglev lookup table:";
		for (int i = 0; i < this._maglevTable.size(); i++) {
			WeakReference<Server<KType, VType>> server = this._maglevTable.get(i);
			String serverInfo = "";
			if (server == null) {
				serverInfo = "not populated yet";
			} else if (server.get() == null) {
				serverInfo = "server at this position has been destroyed";
			} else {
				serverInfo = server.get().getIp();
			}
			info += "\n\t" + i + ": " + serverInfo;
		}
		_Logger.debug(info);
	}

	private Server<KType, VType> getServer(KType key) {
		switch (this._hashAlgo) {
			case RENDEZVOUS_HASH:
				return rendezvousHash(key);
			case JUMP_CONSISTENT_HASH:
				return jumpConsistentHash(key);
			case MAGLEV_HASH:
				return maglevHash(key);
			default:
				_Logger.error("Unknown hashing algorithm");
				return null;
		}
	}

	private Server<KType, VType> maglevHash(KType key) {
		Long hash = md5HashingAlg((String) key);
		WeakReference<Server<KType, VType>> chosenServer = this._maglevTable.get((int) (hash % this._maglevTable.size()));
		assert (chosenServer.get() != null);
		return chosenServer.get();
	}

	private void populateMaglevPermutation() {
		this._maglevPermutation = new ArrayList<>(this._servers.size());

		for (int i = 0; i < this._servers.size(); i++) {
			String ip = this._servers.get(i).getIp();
			int offset = (int) (this.newCompatHashingAlg(ip) % _DEF_MAGLEV_TABLE_SIZE);
			int skip = (int) (this.md5HashingAlg(ip) % (_DEF_MAGLEV_TABLE_SIZE - 1) + 1);

			this._maglevPermutation.add(new ArrayList<>(_DEF_MAGLEV_TABLE_SIZE));

			for (int j = 0; j < _DEF_MAGLEV_TABLE_SIZE; j++) {
				int permutation = (int) ((offset + (long) j * skip) % _DEF_MAGLEV_TABLE_SIZE); // (long)j for prevent (offset + j * size) from overflowing int's range
				this._maglevPermutation.get(i).add(permutation);
			}
		}

//			this.printMaglevPermutation();
	}

	private void populateMaglevTable() {
		int nServers = this._servers.size();
		List<Integer> next = new ArrayList<Integer>(nServers);
		for (int i = 0; i < nServers; i++) {
			next.add(0);
		}

		this._maglevTable = new ArrayList<>(_DEF_MAGLEV_TABLE_SIZE);
		for (int i = 0; i < _DEF_MAGLEV_TABLE_SIZE; i++) {
			this._maglevTable.add(null);
		}

		int n = 0;
		while (true) {
			for (int i = 0; i < nServers; i++) {
				int c = this._maglevPermutation.get(i).get(next.get(i));
				while (this._maglevTable.get(c) != null) {
					next.set(i, next.get(i) + 1);
					c = this._maglevPermutation
							.get(i)
							.get(next.get(i));
				}
				this._maglevTable.set(c, new WeakReference<>(this._servers.get(i)));
				next.set(i, next.get(i) + 1);
				n += 1;
				if (n == _DEF_MAGLEV_TABLE_SIZE) {
//						this.printMaglevTable();
					return;
				}
			}
		}
	}

	private Server<KType, VType> jumpConsistentHash(KType key) {
		// TODO: add weight support

		Long h = md5HashingAlg((String) key);
		BigInteger hash = new BigInteger(h.toString());

		long b = -1, j = 0;
		String msg = "Key: " + key + " - ";
		while (j < this._servers.size()) {
			b = j;
			msg += b;

			hash = hash
					.multiply(new BigInteger("2862933555777941757"))
					.add(new BigInteger("1"))
					.mod(new BigInteger("18446744073709551616")); // clip to unsigned int range
			j = (long) ((b + 1) * ((double) (1l << 31) / (hash.shiftRight(33).add(new BigInteger("1")).doubleValue())));

			if (j < this._servers.size()) {
				msg += " --> ";
			}
		}
		_Logger.debug(msg);
		return this._servers.get((int) b);
	}

	private Server<KType, VType> rendezvousHash(KType key) {
		double maxWeight = 0;
		Server<KType, VType> chosenServer = this._servers.get(0);
		for (Server<KType, VType> server : this._servers) {
			Long hash = md5HashingAlg((String) key + "@" + server.getIp()); // md5HashingAlg: inteprete all byte as unsigned => max 4 bytes = Integer.MAX_VALUE * 2 + 1
			double weightedHash = -server.getWeight() / Math.log(hash / (double) ((long) Integer.MAX_VALUE * 2 + 1));
			if (weightedHash > maxWeight) {
				maxWeight = weightedHash;
				chosenServer = server;
			}
		}
		_Logger.debug("Key: " + key + " --> ip: " + chosenServer.getIp() + " - weight: " + chosenServer.getWeight());
		return chosenServer;
	}

	private long md5HashingAlg(String key) {
		MessageDigest md5 = MD5.get();
		md5.reset();
		md5.update(key.getBytes());
		byte[] bKey = md5.digest();
		long res = ((long) (bKey[3] & 0xFF) << 24) | ((long) (bKey[2] & 0xFF) << 16) | ((long) (bKey[1] & 0xFF) << 8)
				| (long) (bKey[0] & 0xFF);
		return res;
	}

	private long newCompatHashingAlg(String key) {
		CRC32 checksum = new CRC32();
		checksum.update(key.getBytes());
		long crc = checksum.getValue();
		return (crc >> 16) & 0x7fff;
	}
}
