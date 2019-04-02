package ru.evenx.hills;

import java.io.File;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.mail.EmailException;
import org.jasypt.util.text.BasicTextEncryptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.core.FileAppender;

/**
 * Класс инкапсулирует функциональность по приему и передаче информации 
 * между БД СКАТ и сервисом Hills
 *
 */
class Hills {
	
	private final static String MASTERKEY = "masterkey";
	private final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";
	private final String DB_CONNECTION_STRING = "jdbc:oracle:thin:@localhost:1521:ODB";
	private final String OPERATION_DOWLOAD = "download";
	private final String OPERATION_UPLOAD = "upload";
	
	private static final Logger log = LoggerFactory.getLogger("ru.evenx.logback");
	
	private Settings settings = null;
	private Connection dbcon = null;
	private HillsGateway gateway = null;	
	private Mailer logMailer = null;	
	
	/**
	 * Разбор файла настроек
	 * @param settingsFile - файл настроек
	 * @throws HillsException 
	 * 
	 */
	public void readSettings(File settingsFile) throws HillsException {
		
        try {
        	JAXBContext jc = JAXBContext.newInstance(Settings.class);
            Unmarshaller unmarshaller = jc.createUnmarshaller();
			settings = (Settings) unmarshaller.unmarshal(settingsFile);
		} catch (JAXBException e) {
			throw new HillsException("Read settings exception", e);
		}
	}

	/**
	 * Подключение к БД СКАТ
	 */
	public void logOn() throws HillsException {		
		try {
			Class.forName(JDBC_DRIVER);
			dbcon = DriverManager.getConnection(DB_CONNECTION_STRING, settings.db.login, 
					Hills.decryptPass(settings.db.password));
		} catch (ClassNotFoundException | SQLException e) {
			throw new HillsException("DB connection exception", e);
		}
	}
	
	/**
	 * Отключение от БД СКАТ
	 */
	public void logOff() throws HillsException {
		try {
			dbcon.close();
		} catch (SQLException e) {
			throw new HillsException("DB close connection exception", e);
		}
	}
	
	/**
	 * Выполняет алгоритм загрузки заявок или выгрузки прайс-листа
	 */
	public void doWork() throws HillsException {
		
		gateway = new HillsGateway(settings.gateway);
		logMailer = new Mailer();
		try {
			logMailer.setSettings(settings.logmail);
		} catch (EmailException e) {
			throw new HillsException("Configurate mailer exception");
		}
		
		switch (settings.operation.value.toLowerCase()) {
			case OPERATION_DOWLOAD: 
				download();
				break;
			case OPERATION_UPLOAD: 
				upload();
				break;
			default:
				throw new HillsException("Unknown operation in settings file");
		}
	}

	private void download() throws HillsException {
		
		String orderList = getOrderList();
		processOrders(orderList);
		defineOrderStatus();
	}
	
	private String getOrderList() throws HillsException {
		
		GatewayResult res = null;
		try {
			res = gateway.orderList(getLastOrderDate(), getTomorrowDate());
			log.info(res.responseString);
			if (!res.success) {
				throw new HillsException("Get orders list exception : " + res.responseString);
			}
		} catch (IOException e) {
			throw new HillsException(e);
		}				
		return res.responseString;
	}
	
	private void processOrders(String orderList) throws HillsException {		
		try {
			CallableStatement stmt = dbcon.prepareCall("{ call hills.process_orders(?) }");
			Clob clob = dbcon.createClob();
			clob.setString(1, orderList);
			stmt.setClob(1, clob);		
			stmt.execute();
			clob.free();
		} catch (SQLException e) {
			throw new HillsException("Call process_orders() exception", e);
		}		
	}
	
	private void defineOrderStatus() throws HillsException {
		
		final String qry = "SELECT "
				+ "           ho.tradedoc_status, "
				+ "           ho.tradedoc_code, "
				+ "           ho.status, "
				+ "           ho.order_number, "
				+ "           TO_CHAR(ho.shipping_date, 'YYYY-MM-DD') shipping_date "
				+ "         FROM "
				+ "           evad.hills_orders ho";
		
		try {
			Statement stmt = dbcon.createStatement();
			ResultSet rs = stmt.executeQuery(qry);
			
			while(rs.next()) {				
				String tradedocStatus = rs.getString("tradedoc_status");
				String orderStatus = rs.getString("status");
				String orderNumber = rs.getString("order_number"); 
				String shippingDate = rs.getString("shipping_date");
				int docCode = rs.getInt("tradedoc_code");
				
				if (tradedocStatus == null) {
					continue;
				}
				
				if (tradedocStatus.equals("registed")) {
					orderStatusUpdate("done", orderNumber, shippingDate);
				} else if (tradedocStatus.equals("not_exists")) { 
					orderStatusUpdate("cancelled", orderNumber, shippingDate);
					if (docCode == -1) {
						log.info("Error creating document!");					
					}
				} else if (orderStatus.equals("created")) 
					orderStatusUpdate("done", orderNumber, shippingDate);
					orderStatusUpdate("awaiting_delivery", orderNumber, shippingDate);
				}
		} catch (SQLException e) {
			throw new HillsException("Select from hills_orders exception", e);
		} 
	}
	
	private void orderStatusUpdate(String newStatus, String orderNumber, String shippingDate) throws HillsException {
		try {
			log.info(String.format("[newStatus, orderNumber, shippingDate] in [%1$s, %2$s, %3$s]", 
					newStatus, orderNumber, shippingDate));
			GatewayResult res = gateway.ordersPartialUpdate(orderNumber, shippingDate, newStatus);
			log.info(res.responseString);
			if (!res.success) {
				throw new HillsException("Orders status changing exception");
			}
		} catch (Exception e) {
			throw new HillsException(e.getMessage(), e);
		}
	}
	
	private String getLastOrderDate() throws HillsException {
		String retVal = null;
		try {
			CallableStatement stmt = null;
			stmt = dbcon.prepareCall("{ ? = call hills.get_last_order_date }");
			stmt.registerOutParameter(1, Types.VARCHAR);
			stmt.execute();
			retVal = stmt.getString(1);
		} catch (SQLException e) {
			throw new HillsException("Call get_last_order_date() exception", e);
		}		
		return retVal;
	}
	
	private void upload() throws HillsException {
		String priceList = getPriceList();
		stockRecordsCreate(priceList);
	}
	
	private String getPriceList() throws HillsException {
		String retVal = null;
		try {
			Clob clob = dbcon.createClob();
			CallableStatement stmt = dbcon.prepareCall("{ call ? := hills.get_upload_request() }");
			stmt.registerOutParameter(1, Types.CLOB);
			stmt.execute();
			clob = stmt.getClob(1);
			retVal = clob.getSubString(1, (int) clob.length());
			clob.free();
		} catch (SQLException e) {
			throw new HillsException("Call get_upload_request() exception", e);
		}	
		return retVal;
	}
	
	private void stockRecordsCreate(String request) throws HillsException {
		try {
			log.info("request = " + request);
			GatewayResult res = gateway.stockRecordsCreate(request);			
			log.info("response = " +  res.responseString);
			if (!res.success) {
				throw new HillsException("Send price list exception : " + res.responseString);
			}
		} catch (Exception e) {
			throw new HillsException(e.getMessage(), e);
		}
	}
	
	public static String decryptPass(String pass) throws HillsException {
		
		BasicTextEncryptor encryptor = new BasicTextEncryptor();
		encryptor.setPassword(MASTERKEY);
		return encryptor.decrypt(pass);
	}
	
	private String getTomorrowDate() {
		
		SimpleDateFormat formattedDate = new SimpleDateFormat("yyyy-MM-dd");            
		Calendar c = Calendar.getInstance();        
		c.add(Calendar.DATE, 1); 
		
		return formattedDate.format(c.getTime());
	}

	/**
	 * Отправляет файл лога на почту разработчику, указанную в файле настроек
	 * @param subjSuffix - Добавляеся в конец темы. Обычно - статус ошибки.
	 */
	public void sendLog(String subjSuffix) {
		
		FileAppender<?> fa = (FileAppender<?>) ((ch.qos.logback.classic.Logger) log).getAppender("file");
		String fileName = fa.getFile();
		try {
			logMailer.addAttachment(new File(fileName));
			logMailer.send(subjSuffix);
		} catch (EmailException e) {
			log.error("Send mail exception", e);
		}
	}
}























