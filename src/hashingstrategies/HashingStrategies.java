package hashingstrategies;

import hashingstrategies.utils.ActionStack;
import hashingstrategies.utils.LoggerFactory;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.log4j.Logger;

public class HashingStrategies {
	
	private static final Class _ThisClass = HashingStrategies.class;
	private static final Logger _Logger = LoggerFactory.getLogger(_ThisClass);

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {
		Map<String, Integer> ips2Weights = new LinkedHashMap<>();
		ips2Weights.put("127.0.0.1", 1);
		ips2Weights.put("127.0.0.2", 1);
		ips2Weights.put("127.0.0.3", 1);
		ips2Weights.put("127.0.0.4", 1);
		Client<String, Byte> cli = new Client<>(ips2Weights, Client.RENDEZVOUS_HASH);
		cli.printServer();
		
		ActionStack actionStack = new ActionStack(cli);
		
		final Integer LOOP = 100000;
		final boolean linearKeys = true;
		
		// Put
		_Logger.info("Putting...");
		List<String> keys = new ArrayList<String>();
		List<Byte> values = new ArrayList<Byte>();
		for (int i = 0; i < LOOP; i++) {
			String key =
				linearKeys
				? Integer.toString(i + 1)
				: RandomStringUtils.random(64, 33, 127, false, false);
			
			keys.add(key);
			values.add((byte) (i + 2));
			cli.put(key, values.get(values.size() - 1));
		}
		
		cli.printServer(true);
		
		// Get
		while (true) {
			// Pause
//			java.util.Scanner scanner = new Scanner(System.in);
//			System.out.println("Enter to continue...");
//			if (scanner.nextLine().equals("q")) {
//				break;
//			}
			actionStack.removeServer("127.0.0.4");
			actionStack.addServer("127.0.0.5", 1);
			cli.printServer();
			// Get
			_Logger.info("Getting...");
			int nullCount = 0;
			for (int i = 0; i < keys.size(); i++) {
				Byte value = cli.get(keys.get(i));
//				assert(value == values.get(i));
				nullCount += value == null ? 1 : 0;
			}
			// Info
			_Logger.info("Miss ratio = " + ((double)nullCount / LOOP) * 100 + "%");
			
			actionStack.undo();
			cli.printServer();
			// Get
			_Logger.info("Getting...");
			nullCount = 0;
			for (int i = 0; i < keys.size(); i++) {
				Byte value = cli.get(keys.get(i));
//				assert(value == values.get(i));
				nullCount += value == null ? 1 : 0;
			}
			// Info
			_Logger.info("Miss ratio = " + ((double)nullCount / LOOP) * 100 + "%");
			
			break;
		}
	}
	
}
