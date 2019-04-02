package ru.evenx.hills;

import java.util.List;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlValue;

/**
 * Классы для загрузки настроек из XML-файла
 *
 */
@XmlRootElement(name = "settings")
class Settings {
	@XmlElement(name = "db")
	public Db db;
	@XmlElement(name = "gateway")
	public GatewaySettings gateway;
	@XmlElement(name = "operation")
	public Operation operation;
	@XmlElement(name = "logmail")
	public MailSettings logmail;
}

class Db {
	@XmlAttribute
	public String name;
	@XmlAttribute
	public String login;
	@XmlAttribute
	public String password;
}

class Operation {
    @XmlValue
    public String value;
}

class MailSettings {
	@XmlAttribute
	public String auth;
	@XmlAttribute
	public String ssl;
	@XmlAttribute
	public String user;
	@XmlAttribute
	public String pass;
	@XmlAttribute
	public String host;
	@XmlAttribute
	public String port;
	@XmlAttribute
	public String from;
	@XmlAttribute
	public String subject;
	@XmlElement(name = "recipient")
	public List<Recipient> recipients;
}

class Recipient {
    @XmlValue
    public String value;
}

class GatewaySettings {
	@XmlAttribute
    public String url;
	@XmlAttribute
    public String token;
}