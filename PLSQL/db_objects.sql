/* Объект представляет торговую точку клиента предприятия
   Конструктор создает объект из строки вида "1AB123C45678"
 */
CREATE OR REPLACE TYPE salepoint_t AS OBJECT
(
  ent_code INTEGER,
  branch_code CHAR,
  ent_serv_code CHAR,
  ent_cust_code INTEGER,
  sp_serv_code CHAR,
  sp_sode INTEGER,
  CONSTRUCTOR FUNCTION salepoint_t(str_salepoint IN VARCHAR2) RETURN SELF AS RESULT
)
/
CREATE OR REPLACE TYPE BODY salepoint_t
AS
  CONSTRUCTOR FUNCTION salepoint_t(str_salepoint IN VARCHAR2) RETURN SELF AS RESULT
  IS
    NO_MATCH EXCEPTION;
  BEGIN
    IF str_salepoint IS NULL OR NOT REGEXP_LIKE(str_salepoint, '^[[:digit:]]{1,2}[^[:digit:]]{1}[^[:digit:]]{1}[[:digit:]]{1,5}[^[:digit:]]{1}[[:digit:]]{1,5}$') THEN
      RAISE NO_MATCH;
    END IF;

    SELECT REGEXP_SUBSTR(str_salepoint, '^[[:digit:]]{1,2}') -- 1
    INTO SELF.ent_code
    FROM dual;

    SELECT REGEXP_SUBSTR(str_salepoint, '[^[:digit:]]{1}') -- A
    INTO SELF.branch_code
    FROM dual;

    SELECT REGEXP_SUBSTR(str_salepoint, '[^[:digit:]]{1}', 3) -- B
    INTO SELF.ent_serv_code
    FROM dual;

    SELECT REGEXP_SUBSTR(str_salepoint, '[[:digit:]]{1,5}', 4) -- 123
    INTO SELF.ent_cust_code
    FROM dual;

    SELECT REGEXP_SUBSTR(str_salepoint, '[^[:digit:]]{1}', 4) -- C
    INTO SELF.sp_serv_code
    FROM dual;

    SELECT REGEXP_SUBSTR(str_salepoint, '[[:digit:]]{1,5}$') --45678
    INTO SELF.sp_sode
    FROM dual;

    RETURN;

  EXCEPTION
    WHEN NO_MATCH THEN
      raise_application_error(-20006, 'Глобальный идентификатор клиента не соответствует шаблону');
  END;
END;

/

/*
  Тип строки фактуры заказа Hills
  Конструктор принимает строку в формате JSON, полученную от сервиса Hills
*/
CREATE OR REPLACE TYPE hills_line_t AS OBJECT
(
   sku VARCHAR2(50),
   final_price NUMBER,
   discount NUMBER,
   quantity NUMBER,
   CONSTRUCTOR FUNCTION hills_line_t(line IN json) RETURN SELF AS RESULT
)
/
CREATE OR REPLACE TYPE BODY hills_line_t
AS

CONSTRUCTOR FUNCTION hills_line_t(line IN json) RETURN SELF AS RESULT
IS
BEGIN
  SELF.sku := json_ext.get_string(line, 'sku');
  SELF.final_price := json_ext.get_number(line, 'final_price');
  SELF.discount := json_ext.get_number(line, 'discount');
  SELF.quantity := json_ext.get_number(line, 'quantity');
  RETURN;
END;

END;

/

CREATE OR REPLACE TYPE hills_invoice_tab AS TABLE OF hills_line_t

/

/*
  Объект представляет заказ Hills
  Конструктор принимает строку в формате JSON, полученную от сервиса Hills
*/
CREATE OR REPLACE TYPE hills_order_t AS OBJECT
(
       date_download TIMESTAMP,
       order_number NUMBER,
       status VARCHAR2(50),
       date_placed TIMESTAMP,
       shipping_date DATE,
       user_id NUMBER,
       is_activated_by_distributor NUMBER,
       client_id VARCHAR2(50),
       customer_id salepoint_t,
       delivery_id VARCHAR(50),
       bonus NUMBER,
       tradedoc_code NUMBER,
       tradedoc_status VARCHAR(50),
       json_content CLOB,
       invoice hills_invoice_tab,
       error_msg VARCHAR2(1000),
       CONSTRUCTOR FUNCTION hills_order_t(ord IN json) RETURN SELF AS RESULT
)
/
CREATE OR REPLACE TYPE BODY hills_order_t
AS

CONSTRUCTOR FUNCTION hills_order_t(ord IN json) RETURN SELF AS RESULT
IS
  l_lines json_list;
BEGIN
  dbms_lob.createtemporary(SELF.json_content, TRUE);
  SELF.date_download := CURRENT_TIMESTAMP;
  SELF.tradedoc_code := -1;
  SELF.tradedoc_status := '';
  SELF.order_number := json_ext.get_string(ord, 'number');
  SELF.status := json_ext.get_string(ord, 'status');
  SELF.date_placed := TO_TIMESTAMP(json_ext.get_string(Ord, 'date_placed'), 'YYYY-MM-DD"T"HH24:MI:SS.FF"Z"');
  SELF.shipping_date := NVL(json_ext.get_date(ord, 'shipping_date'), SYSDATE);
  SELF.user_id := json_ext.get_number(ord, 'user.id');
  SELF.is_activated_by_distributor := CASE WHEN json_ext.get_bool(ord, 'user.is_activated_by_distributor') THEN 1 ELSE 0 END;
  SELF.customer_id := salepoint_t(json_ext.get_string(ord, 'user.clinic.customer_id'));
  SELF.delivery_id := json_ext.get_string(ord, 'user.clinic.delivery_id');
  SELF.client_id := json_ext.get_string(ord, 'user.clinic.client_id');
  SELF.bonus := CASE WHEN json_ext.get_bool(ord, 'bonus') THEN 1 ELSE 0 END;
  ord.to_clob(SELF.json_content);
  SELF.invoice := hills_invoice_tab();
  l_lines := json_list(ord.get('lines'));
  FOR j IN 1..l_lines.COUNT
  LOOP
      SELF.invoice.extend;
      SELF.invoice(invoice.LAST) := hills_line_t(json(l_lines.get(j)));
  END LOOP;

  RETURN;
END;

END;

/

CREATE TABLE hills_orders OF hills_order_t
       NESTED TABLE invoice STORE AS hills_invoice_nt

/

