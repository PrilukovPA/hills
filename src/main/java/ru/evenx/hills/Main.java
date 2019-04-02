package ru.evenx.hills;

import java.io.File;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Интеграционная программа, объединяющая учетную систему СКАТ и сервис Hills Staff Feeding
 * Чтение параметров командной строки и запуск алгоритма
 * в соответствии с требованиями файла настроек.
 *
 */
@Command(name = "Hills Staff Feeding", mixinStandardHelpOptions = true, version = "1.0")
public class Main implements Runnable
{
	@Option(names = { "-S", "--settings" }, required = true, paramLabel = "<xml file>", description = "Load settings xml-file")
	private File settings;
	
    private static final Logger log = LoggerFactory.getLogger("ru.evenx.logback");
	
	public static void main( String[] args )
    {
		CommandLine.run(new Main(), args);
    }

	public void run() {

		if (!settings.exists()) {
			log.error("Settings file not found");
			return;
		}
		
		String subjSuffix = ": OK";
		
		Hills hills = new Hills();
		try {
			hills.readSettings(settings);
			hills.logOn();
			hills.doWork();
			hills.logOff();
		} catch (Throwable e) {
			log.error(e.getMessage(), e);
			subjSuffix = ": ERROR";
			
		} finally {
			hills.sendLog(subjSuffix);
		}
	}
}
