/*
  Пакет предназначен для интеграции с производителем Hills.
  Создал: Прилуков П.А.
  ГК ЭВЭНКС, 2018
*/

CREATE OR REPLACE PACKAGE Hills
AS
  /* Возвращает дату последнего принятого заказа в формате 'YYYY-MM-DD' */
  FUNCTION get_last_order_date RETURN VARCHAR2;
  
  /* Возвращает прайс-лист в формате JSON */
  FUNCTION get_upload_request RETURN CLOB;
  
  /* Осуществляет обработку принятых заказов и создание товарных документов
     json_str - строка в формате JSON */
  PROCEDURE process_orders(json_str IN CLOB);
END;
/
CREATE OR REPLACE PACKAGE BODY Hills
AS
  c_created_status CONSTANT VARCHAR2(100) := 'created';
  c_awaiting_delivery_status CONSTANT VARCHAR2(100) := 'awaiting_delivery';
  c_not_exists_status CONSTANT VARCHAR2(100) := 'not_exists';
  c_registed_status CONSTANT VARCHAR2(100) := 'registed';
  c_results_list_name CONSTANT VARCHAR2(100) := 'results';
  c_days_waiting_delivery CONSTANT NUMBER := 2;
  c_consignee_warehouse warehouse.whcode%TYPE := '1-С';
  c_db_name VARCHAR2(30) := 'CSALES';
  
  f_price_form_code NUMBER := NULL;  
  f_error_msg hills_orders.error_msg%TYPE := NULL;
  f_tdoccode NUMBER := -1;

  /*
   * Курсор по новоприбывшим заказам
   */
  CURSOR cur_new_created_orders IS
    (SELECT order_number, shipping_date, user_id, customer_id, invoice
     FROM hills_orders ho 
     WHERE ho.status = c_created_status
       AND ho.is_activated_by_distributor = 1
       AND ho.TRADEDOC_CODE = -1)
  FOR UPDATE;

  /*
   * Курсор по фактуре заказа
   */
  CURSOR cur_lines(ord cur_new_created_orders%ROWTYPE) IS
    (SELECT sku, quantity, final_price FROM TABLE(ord.invoice));

FUNCTION get_last_order_date RETURN VARCHAR2
AS
  tmp DATE;
  retval VARCHAR2(11);
  c_hills_integration_date VARCHAR2(10) := '01.08.2018';
BEGIN
  SELECT MAX(ho.date_download)
  INTO tmp
  FROM hills_orders ho;
  tmp := NVL(tmp, TO_DATE(c_hills_integration_date, 'DD.MM.YYYY'));
  retval := TO_CHAR(tmp, 'YYYY-MM-DD');
  RETURN retval; 
END;

FUNCTION get_upload_request RETURN CLOB
AS
  stmt CLOB;
  lst json_list;
  retval CLOB;
  obj json;
BEGIN
  stmt := 'SELECT
                  TO_CHAR(gr.goodscode) "sku",
                  ROUND(p.pricer, 2) "price",
                  CASE WHEN gr.rem < 0 THEN 0 ELSE gr.rem END "quantity"
           FROM
                  price p,
                 (SELECT goodscode, SUM(remainder) rem
                  FROM goodsrem
                  WHERE goodscode IN (SELECT goodscode
                                      FROM goods
                                      WHERE prodcode = 88 AND hiddenflag = 0)
                  GROUP BY goodscode) gr
           WHERE
                  p.pservcode = ''*'' AND p.pformcode = ' || f_price_form_code || ' AND p.goodscode = gr.goodscode AND p.histno = 0';
  lst := json_dyn.executeList(stmt);
  obj := json();
  json_ext.put(obj, 'stock_records', lst);
  dbms_lob.createtemporary(retval, TRUE);
  obj.to_clob(retval);
  RETURN retval;  
END;

/* Определение кода агента по данным клиента предприятия */
FUNCTION get_agent(sale_point IN salepoint_t) RETURN NUMBER
AS
  ret_val NUMBER;
BEGIN
  SELECT a.agentcode
  INTO ret_val
  FROM cspagent a
  WHERE a.entcode = sale_point.ent_code
    AND a.branchcode = sale_point.branch_code
    AND a.servcode = sale_point.ent_serv_code
    AND a.ecustcode = sale_point.ent_cust_code
    AND a.spservcode = sale_point.sp_serv_code
    AND a.spcode = sale_point.sp_sode
    AND a.enddate IS NULL
    AND ROWNUM = 1;
  RETURN ret_val;
EXCEPTION
  WHEN OTHERS THEN
    f_error_msg := 'Не найден агент';
    RAISE;
END;

/*
     Заказ в Хиллз создается на клинику, но документ нужно выписать на врача.
     Поэтому врачи в СКАТ'е заведены как клиенты предприятия.
     Эта функция возвращает данные клиента предприятия по коду привязки врача в Хиллз.
     Код врача прописывается в поле ProvNo таблицы EntCustomer.
*/
FUNCTION get_doctor(user_id hills_orders.user_id%TYPE) RETURN salepoint_t
AS
  doctor salepoint_t;
  sale_point VARCHAR2(32767);
BEGIN
  SELECT ec.entcode || ec.branchcode || ec.servcode || ec.ecustcode || csp.spservcode || csp.spcode
  INTO sale_point
  FROM custspoint csp, entcustomer ec
  WHERE csp.entcode = ec.entcode
    AND csp.branchcode = ec.branchcode
    AND csp.servcode = ec.servcode
    AND csp.ecustcode = ec.ecustcode
    AND ec.provno = get_doctor.user_id
    AND csp.hiddenflag = 0
    AND csp.workflag = 1;
  doctor := salepoint_t(sale_point);
  RETURN doctor;
EXCEPTION
  WHEN OTHERS THEN
    f_error_msg := 'Не найдено место продажи (доктор) USER_ID = ' || get_doctor.user_id;
    RAISE;
END;

PROCEDURE create_trade_doc_cap(ord cur_new_created_orders%ROWTYPE)
AS
  doctor salepoint_t := get_doctor(ord.user_id);
  agent_code tradedoc.agent%TYPE := get_agent(ord.customer_id);  
BEGIN
  INSERT INTO tradedoc(
    tdoctype,
    tdocdate,
    entcode,
    branchcode,
    eservcode,
    ecustcode,
    spservcode,
    spcode,
    whcode,
    sprservcode,
    sellpform,
    AGENT,
    description,
    variant)
  VALUES(
    11,
    ord.shipping_date,
    doctor.ent_code,
    doctor.branch_code,
    doctor.ent_serv_code,
    doctor.ent_cust_code,
    doctor.sp_serv_code,
    doctor.sp_sode,
    c_consignee_warehouse,
    '*',
    f_price_form_code,
    agent_code,
    'АВТОЗАКАЗ',
    1)
  RETURNING tdoccode 
  INTO f_tdoccode;
EXCEPTION
  WHEN OTHERS THEN
    f_error_msg := 'Не удалось создать шапку документа';
    RAISE;
END;

PROCEDURE create_trade_doc_inv(line cur_lines%ROWTYPE)
AS
BEGIN
  INSERT INTO invoice(
    servcode,
    tdoccode,
    goodscode,
    moveunit,
    places,
    pricer,
    discprc,
    priced,
    sumr
  )
  VALUES(
    '*',
    f_tdoccode,
    line.sku,
    0,
    line.quantity,
    line.final_price,
    0,
    line.final_price,
    line.quantity * line.final_price
  );
EXCEPTION
  WHEN OTHERS THEN
    f_error_msg := 'Не удалось создать запись фактуры';
    RAISE;
END;

PROCEDURE create_trade_docs
AS

  PROCEDURE clear_error_msg
  AS
  BEGIN
    UPDATE hills_orders ho
    SET ho.error_msg = ''
    WHERE CURRENT OF cur_new_created_orders;
  END;

  PROCEDURE bind_trade_doc_to_order
  AS
  BEGIN
    UPDATE hills_orders ho
    SET ho.tradedoc_code = f_tdoccode
    WHERE CURRENT OF cur_new_created_orders;
  END;

BEGIN
  FOR ord IN cur_new_created_orders
  LOOP
    BEGIN
      clear_error_msg;

      create_trade_doc_cap(ord);

      FOR line IN cur_lines(ord)
      LOOP
        create_trade_doc_inv(line);
      END LOOP;

      bind_trade_doc_to_order;
    EXCEPTION
      /*
         В случае ошибки поле TRADEDOC_CODE таблицы hills_orders примет значение "-1".
         Такие заказы автоматически отменяются на сайте Хиллз.
      */
      WHEN OTHERS THEN
        log_pkg.save_line(p_scope => $$PLSQL_UNIT, p_text => f_error_msg, p_usercode => ord.order_number);
    END;
  END LOOP;
END;

/*
 * Определяем статус документа (для дальнейшего управления статусом заказа, ожидающего доставки)
 * НЕ СУЩЕСТВУЕТ
 *  1) документа, связанного с заявкой не существует, либо
 *  2) документу выставлен флаг удаления
 * ПРОВЕДЕН
 *  1) документ, связанный с заявкой, проведен, и
 *  2) с момента отправления заказа прошло более 2х дней
 */
PROCEDURE determine_doc_state
AS
BEGIN
  UPDATE hills_orders ho
  SET ho.tradedoc_status =
    CASE
        WHEN ho.status IN (c_created_status, c_awaiting_delivery_status)
             AND (NOT EXISTS (SELECT NULL FROM tradedoc td WHERE td.servcode = '*' AND td.tdoccode = ho.tradedoc_code)
                   OR EXISTS (SELECT NULL FROM tradedoc td WHERE td.servcode = '*' AND td.tdoccode = ho.tradedoc_code AND td.delflag = 1))
        THEN
          c_not_exists_status
        WHEN ho.status IN (c_awaiting_delivery_status)
             AND (EXISTS (SELECT NULL FROM tradedoc td
                          WHERE td.servcode = '*'
                            AND td.tdoccode = ho.tradedoc_code
                            AND td.regflag = 1
                            AND (SYSDATE - td.tdocdate) > c_days_waiting_delivery))
        THEN
          c_registed_status
        ELSE
          NULL
    END;
END;

-- Сброс даты доставки для удаленных документов
PROCEDURE reset_shipping_date
AS
BEGIN
  UPDATE hills_orders ho
  SET ho.shipping_date = NULL
  WHERE ho.tradedoc_status = c_not_exists_status;
END;

PROCEDURE update_or_insert(ord hills_order_t)
AS
BEGIN
  UPDATE hills_orders ho SET ho.status = ord.status WHERE ho.order_number = ord.order_number;
  IF SQL%ROWCOUNT = 0 THEN
    INSERT INTO hills_orders VALUES(ord);
  END IF;
  COMMIT;
END;

PROCEDURE process_orders(json_str IN CLOB)
AS
  orders json_list;
  ord hills_order_t;
BEGIN
  orders := json_list(json(json_str).get(c_results_list_name));

  FOR i IN 1..orders.COUNT
  LOOP
    ord := hills_order_t(json(orders.get(i)));
    update_or_insert(ord);
  END LOOP;

  create_trade_docs;

  determine_doc_state;

  reset_shipping_date;

  COMMIT;
END;

FUNCTION get_price_form_code RETURN NUMBER
AS
  ret_val NUMBER;
BEGIN
  ret_val := 
    CASE UPPER(c_db_name)
      WHEN UPPER('CSALES') THEN 108
      WHEN UPPER('EVSURG') THEN 215
    END;
  RETURN ret_val;  
EXCEPTION 
  WHEN OTHERS THEN
    f_error_msg := 'Задана неизвестная база данных';
    RAISE;  
END;

BEGIN
  f_price_form_code := get_price_form_code;  
END;
/
