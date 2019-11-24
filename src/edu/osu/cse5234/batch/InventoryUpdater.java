package edu.osu.cse5234.batch;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class InventoryUpdater {
	public static void main(String[] args) {

		System.out.println("Starting Inventory Update ...");
		try {
			Connection conn = createConnection();
			Collection<Integer> newOrderIds = getNewOrders(conn);
			Map<Integer, Integer> orderedItems = getOrderedLineItems(newOrderIds, conn);
			updateInventory(orderedItems, conn);
			updateOrderStatus(newOrderIds, conn);
			conn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static Connection createConnection() throws SQLException, ClassNotFoundException {
		Class.forName("org.h2.Driver");
		Connection conn = DriverManager.getConnection("jdbc:h2:C:\\Users\\yucha\\Documents\\workspace\\cse5234\\h2db\\GumShopV2DB;AUTO_SERVER=TRUE", "sa", "");
		return conn;
	}

	private static Collection<Integer> getNewOrders(Connection conn) throws SQLException {
		Collection<Integer> orderIds = new ArrayList<Integer>();
		ResultSet rset = conn.createStatement().executeQuery(
                     "select ID from CUSTOMER_ORDER where STATUS = 'New'");
		while (rset.next()) {
			orderIds.add(new Integer(rset.getInt("ID")));
		}
		return orderIds;
	}

	private static Map<Integer, Integer> getOrderedLineItems(Collection<Integer> newOrderIds,
                Connection conn)  throws SQLException {
		// TODO Auto-generated method stub
		// This method returns a map of two integers. The first Integer is item ID, and 
                 // the second is cumulative requested quantity across all new orders
		Map<Integer, Integer> orderedItems = new HashMap<Integer, Integer>();
		for (int orderId :  newOrderIds) {
			ResultSet rset = conn.createStatement().executeQuery(
	                "select ITEM_ID, QUANTITY from CUSTOMER_ORDER_LINE_ITEM where CUSTOMER_ORDER_ID_FK = " + orderId);
			while (rset.next()) {
				int itemId = rset.getInt("ITEM_ID");
				int quant = rset.getInt("QUANTITY");
				if (orderedItems.containsKey(rset.getInt("ITEM_ID"))) {
					orderedItems.replace(itemId, orderedItems.get(itemId) + quant);
				} else {
					orderedItems.put(itemId, quant);
				}
			}	
		}

		return orderedItems;
	}

	private static void updateInventory(Map<Integer, Integer> orderedItems, 
                Connection conn) throws SQLException {
		// TODO Auto-generated method stub

		orderedItems.remove(0); //get rid of all items that have a 0 lol
		for (Map.Entry<Integer, Integer> item : orderedItems.entrySet()) {
			System.out.println(item.getKey() + " " + item.getValue());
		} 
		int updatedInventory = 0;
		for (Map.Entry<Integer, Integer> item : orderedItems.entrySet()) {
			ResultSet rset = conn.createStatement().executeQuery("select AVAILABLE_QUANTITY from ITEM where ITEM_NUMBER = " + item.getKey());
			while (rset.next()) {
				 updatedInventory = rset.getInt("AVAILABLE_QUANTITY") - item.getValue();
			}
			conn.createStatement().executeUpdate("update ITEM set AVAILABLE_QUANTITY = " + updatedInventory + " where ID = " + item.getKey());
		}

	}

	private static void updateOrderStatus(Collection<Integer> newOrderIds, 
                Connection conn) throws SQLException {
		// TODO Auto-generated method stub
		for (int orderId :  newOrderIds) {
			conn.createStatement().executeUpdate("update CUSTOMER_ORDER set STATUS = 'Processed' WHERE ID = " + orderId);
		}
	}

}
