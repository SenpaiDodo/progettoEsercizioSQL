package it.betacom.progettoEsercizioSQL.connection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import it.betacom.progettoEsercizioSQL.App;

public class SQLConnection {
	private static SQLConnection instance = null;
	private Connection conn = null;
	
	private String db_url;
	private String user;
	private String password;
	
	private SQLConnection() {
		try {
			Class.forName("com.mysql.cj.jdbc.Driver");
			System.out.println("Driver caricato correttamente");
		} catch(ClassNotFoundException e) {
			System.out.println("Non è stato possibile caricare il Driver");
			e.printStackTrace();
		}
		
		Properties properties = new Properties();
		InputStream inputStream = App.class.getClassLoader().getResourceAsStream("config.properties");
		
		try {
			properties.load(inputStream);
			this.user = properties.getProperty("jdbs.user");
			this.password = properties.getProperty("jdbs.password");
			this.db_url = properties.getProperty("jdbs.db_url");
		} catch (IOException e) {
			System.out.println("Errore caricamento proprieta': " + e.getMessage());
		}
	}
	
	public static synchronized SQLConnection getInstance() {
		if(instance == null) {
			instance = new SQLConnection();
		}
		
		return instance;
	}
	
	public Connection getConnection() {
		try {
			this.conn = DriverManager.getConnection(db_url, user, password);
			System.out.println("Connessione avvenuta.");
		} catch (SQLException e) {
			System.out.println("Errore connessione al server: " + e.getMessage());
		}
		return this.conn;
	}
	
	public void closeConnection() {
		try {
			if (this.conn != null) {
				this.conn.close();
				System.out.println("Connessione chiusa.");
			}
		} catch (SQLException e) {
			System.out.println("Errore chiusura connessione al server: " + e.getMessage());
		}
	}
}
