package ru.evenx.hills;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Результат доступа к API Hills
 *
 */
class GatewayResult {
	public boolean success;
	public int responseCode;
	public String responseString;
}

/**
 * Класс реализует доступ к сервису Hills Staff Feeding
 *
 */
class HillsGateway {
	
	private final String contentTypeHeader = "application/json;charset=UTF-8";
	private final String apiOrders = "/distributors_api/v3/orders/";
	private final String apiCreateRecords = "/distributors_api/v3/stock_records/";
	private final long CONNECTION_TIMEOUT = 30;
	private final int HTTP_OK = 200;
	private final int HTTP_CREATED = 201;
	
	private GatewaySettings settings = null;
	private HttpClient client = null;
	
	public HillsGateway(GatewaySettings settings) {
		
		this.settings = settings;
		
		List<Header> headers = new ArrayList<Header>();
		headers.add(new BasicHeader(HttpHeaders.CONTENT_TYPE, contentTypeHeader));
		headers.add(new BasicHeader(HttpHeaders.AUTHORIZATION, "Token " + this.settings.token));
		client = HttpClients.custom()
				            .setDefaultHeaders(headers)
				            .setConnectionTimeToLive(CONNECTION_TIMEOUT, TimeUnit.SECONDS)
				            .build();
	}
	
	/**
	 * Получение порции (страницы) заказов на доставку
	 * @param page - номер страницы
	 * @param begDate - начальная дата заказов ("YYYY-MM-DD")
	 * @param endDate - конечная дата заказов ("YYYY-MM-DD")
	 * @return результат обращения к сервису (содержит список заказов (JSON))
	 * @throws IOException 
	 * @throws ClientProtocolException 
	 */
	private GatewayResult orderListPaged(String page, String begDate, String endDate) throws ClientProtocolException, IOException {
		
		HttpUriRequest request = RequestBuilder.get()
				.setUri(settings.url + apiOrders)
				.addParameter("page", page)
				.addParameter("later_than", begDate)
				.addParameter("earlier_than", endDate)
				.build();
		HttpResponse response = client.execute(request);

	    GatewayResult retVal = new GatewayResult();
	    retVal.responseCode = response.getStatusLine().getStatusCode();
        retVal.success = (retVal.responseCode == HTTP_OK);
        retVal.responseString = EntityUtils.toString(response.getEntity());
		
		return retVal;	
	}
	
	/**
	 * Получение всех заказов на доставку в диапазоне дат
	 * @param begDate - начальная дата заказов 
	 * @param endDate - конечная дата заказов
	 * @return результат обращения к сервису в виде списка заказов
	 * @throws IOException 
	 * @throws ClientProtocolException 
	 */
	public GatewayResult orderList(String begDate, String endDate) throws ClientProtocolException, IOException {
		
		JsonParser parser = new JsonParser();
		
		int page = 1;		
		GatewayResult res = orderListPaged(String.valueOf(page), begDate, endDate);
		
		if (!res.success) {
    		return res;
		}
		
        JsonObject itemObject = parser.parse(res.responseString).getAsJsonObject();
        JsonObject itemObjectAccum = itemObject.deepCopy();
        itemObjectAccum.remove("next");
        itemObjectAccum.remove("previous");
        
        while (!itemObject.get("next").isJsonNull()) {        	
        	
        	res = orderListPaged(String.valueOf(page++), begDate, endDate);
        	
        	if (!res.success) {
        		return res;    
        	}
        	
        	itemObject = parser.parse(res.responseString).getAsJsonObject();
        	for (JsonElement item : itemObject.getAsJsonArray("results"))
        		itemObjectAccum.getAsJsonArray("results").add(item);
        }
        
        res.responseString = itemObjectAccum.toString();
		return res;
	}
	
	private JsonObject getUpdateRequest(String shippingDate, String status) {
		
		JsonObject retVal = new JsonObject();
		if (shippingDate != null && !shippingDate.isEmpty()) {
			retVal.addProperty("shipping_date", shippingDate);	
		}
		if (status != null && !status.isEmpty()) {
			retVal.addProperty("status", status);	
		}
		return retVal;
	}
	
	/**
	 * Обновление статуса заказа
	 * @param number - номер заказа
	 * @param shippingDate - дата доставки
	 * @param status - новый статус
	 * Последовательность статусов:
     * created -> processing, cancelled
     * processing -> awaiting_payment, awaiting_delivery, cancelled
     * awaiting_payment -> awaiting_delivery, cancelled
     * awaiting_delivery -> done, cancelled     
	 * @return результат обращения к сервису в виде списка обновленных статусов (JSON)
	 * @throws IOException
	 */
	public GatewayResult ordersPartialUpdate(String number, String shippingDate, String status) throws IOException {
		
		JsonObject itemObject = getUpdateRequest(shippingDate, status);
		StringEntity entity = new StringEntity(itemObject.toString());
		String patchUrl = settings.url + apiOrders + number + "/";
		
		HttpUriRequest request = RequestBuilder.patch()
				.setUri(patchUrl)
				.setEntity(entity)
				.build();
		HttpResponse response = client.execute(request);

	    GatewayResult retVal = new GatewayResult();
	    retVal.responseCode = response.getStatusLine().getStatusCode();
        retVal.success = (retVal.responseCode == HTTP_OK || retVal.responseCode == HTTP_CREATED);
        retVal.responseString = response.getStatusLine().getReasonPhrase();
		
		return retVal;	
	}

	/**
	 * Выгрузка прайс-листа в Hills
	 * @param priceList - список товаров и цен в формате JSON
	 * @return результат обращения к сервису в виде списка созданных и не созданных записей (JSON)
	 * @throws ClientProtocolException
	 * @throws IOException
	 */
	public GatewayResult stockRecordsCreate(String priceList) throws ClientProtocolException, IOException {
		
		String postUrl = settings.url + apiCreateRecords;
		
		HttpUriRequest request = RequestBuilder.post()
				.setUri(postUrl)
				.setEntity(new StringEntity(priceList))
				.build();
		HttpResponse response = client.execute(request);

	    GatewayResult retVal = new GatewayResult();
	    retVal.responseCode = response.getStatusLine().getStatusCode();
        retVal.success = (retVal.responseCode == HTTP_OK || retVal.responseCode == HTTP_CREATED);
        retVal.responseString = response.getStatusLine().getReasonPhrase();
        
		return retVal;	
	}
}
