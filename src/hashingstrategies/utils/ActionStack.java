package hashingstrategies.utils;

import hashingstrategies.Client;
import hashingstrategies.Server;
import java.util.Map;
import java.util.Stack;
import org.apache.log4j.Logger;

public class ActionStack {
	
	private static final Class _ThisClass = ActionStack.class;
	private static final Logger _Logger = LoggerFactory.getLogger(_ThisClass);

	static class Action {

		private Server _server;
		private int _action;
		private static final int ADD_SERVER = 0;
		private static final int REMOVE_SERVER = 1;

		public Action(Server server, int action) {
			assert (action == Action.ADD_SERVER || action == Action.REMOVE_SERVER);

			this._server = server;
			this._action = action;
		}

		public String getIp() {
			return this._server.getIp();
		}

		public int getWeight() {
			return this._server.getWeight();
		}

		public Map getDB() {
			return this._server.getDB();
		}

		public int getAction() {
			return this._action;
		}

	}

	private Client _cli;
	private Stack<Action> _actionStack;

	public ActionStack(Client cli) {
		this._cli = cli;
		this._actionStack = new Stack<>();
	}

	public void addServer(String ip, Integer weight) {
		Server server = this._cli.addServer(ip, weight);
		this._actionStack.add(new Action(server, Action.ADD_SERVER));
	}

	public void removeServer(String ip) {
		Server server = this._cli.removeServer(ip);
		if (server == null) {
			_Logger.warn("Remove server fail: ip " + ip + " not found --> not push to action stack");
			return;
		}
		this._actionStack.add(new Action(server, Action.REMOVE_SERVER));
	}

	public void undo() {
		_Logger.info("Undoing all actions...");
		while (!this._actionStack.empty()) {
			Action action = this._actionStack.pop();
			switch (action.getAction()) {
				case Action.ADD_SERVER:
					this._cli.removeServer(action.getIp());
					break;
				case Action.REMOVE_SERVER:
					this._cli.addServer(action.getIp(), action.getWeight(), action.getDB());
					break;
				default:
					_Logger.error("Unknown action");
			}
		}
		_Logger.info("Undo all actions done!");
	}
}
