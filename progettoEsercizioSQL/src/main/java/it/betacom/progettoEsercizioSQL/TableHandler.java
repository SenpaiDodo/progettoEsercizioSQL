package it.betacom.progettoEsercizioSQL;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Scanner;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.itextpdf.text.Document;
import com.itextpdf.text.DocumentException;
import com.itextpdf.text.Paragraph;
import com.itextpdf.text.pdf.PdfWriter;

import it.betacom.progettoEsercizioSQL.connection.SQLConnection;

public class TableHandler {
	private SQLConnection sqlconn;
	private Connection connection;
	
	private String csv_path = "././././././resources/esercizioPartecipanti.csv";
	private static String pdf_path = "././././././output/situazione_estrazioni.pdf";
	
	protected static final Logger logger = LogManager.getLogger("TableHandler");

	private Connection getTableHandlerConnection() {
		this.sqlconn = SQLConnection.getInstance();
		logger.info("Connesione creata con successo.");
		return this.sqlconn.getConnection();
	}

	private void closeTableHandlerConnection() {
		try {
			if (this.connection != null) {
				this.connection.close();
			}
		} catch (SQLException e) {
			System.out.println("Errore chiusura connessione: " + e.getMessage());
		}
		if (this.sqlconn != null) {
			this.sqlconn.closeConnection();
		}
		logger.info("Connessione chiusa con successo.");
	}

	public void createTables() {
		this.connection = getTableHandlerConnection();
		
		try {
			Statement stmt = this.connection.createStatement();
			String sql1 = "CREATE TABLE `eserciziosqljava`.`partecipante` (\r\n"
					+ "  `id_partecipante` INT UNSIGNED NOT NULL AUTO_INCREMENT,\r\n"
					+ "  `nome_partecipante` VARCHAR(45) NOT NULL,\r\n"
					+ "  `sede_partecipante` VARCHAR(45) NOT NULL,\r\n"
					+ "  PRIMARY KEY (`id_partecipante`),\r\n"
					+ "  UNIQUE INDEX `id_partecipante_UNIQUE` (`id_partecipante` ASC) VISIBLE);";
			stmt.executeUpdate(sql1);
			logger.info("Tabella 'partecipante' creata con successo.");
			
			String sql2 = "CREATE TABLE `eserciziosqljava`.`estrazione` (\r\n"
					+ "  `id_estrazione` INT UNSIGNED NOT NULL AUTO_INCREMENT,\r\n"
					+ "  `id_partecipante` INT UNSIGNED NOT NULL,\r\n"
					+ "  `timestamp` DATETIME NOT NULL,\r\n"
					+ "  PRIMARY KEY (`id_estrazione`),\r\n"
					+ "  UNIQUE INDEX `id_estrazione_UNIQUE` (`id_estrazione` ASC) VISIBLE,\r\n"
					+ "  INDEX `id_partecipante_idx` (`id_partecipante` ASC) VISIBLE,\r\n"
					+ "  CONSTRAINT `id_partecipante`\r\n"
					+ "    FOREIGN KEY (`id_partecipante`)\r\n"
					+ "    REFERENCES `eserciziosqljava`.`partecipante` (`id_partecipante`)\r\n"
					+ "    ON DELETE NO ACTION\r\n"
					+ "    ON UPDATE NO ACTION);";
			stmt.executeUpdate(sql2);
			logger.info("Tabella 'estrazione' creata con successo.");
			
			closeTableHandlerConnection();
		} catch (SQLException e) {
			logger.error("Errore creazione tabelle: " + e.getMessage());
		}
	}
	
	public void dropTables() {
		this.connection = getTableHandlerConnection();
		
		try {
			Statement stmt = this.connection.createStatement();
			
			String sqlDrop1 = "DROP TABLE `eserciziosqljava`.`estrazione`;";
			stmt.executeUpdate(sqlDrop1);
			logger.info("Cancellazione tabella 'estrazione' avvenuta con successo.");
			
			String sqlDrop2 = "DROP TABLE `eserciziosqljava`.`partecipante`;";
			stmt.executeUpdate(sqlDrop2);
			logger.info("Cancellazione tabella 'partecipante' avvenuta con successo.");
		} catch (SQLException e) {
			logger.error("Errore cancellazione tabelle: " + e.getMessage());
		}
		
		closeTableHandlerConnection();
	}
	
	public void readCSV() {
		this.connection = getTableHandlerConnection();
		
		try {
			Statement stmt = this.connection.createStatement();
			Scanner sc;
			String sql;
			try {
				sc = new Scanner(new File(this.csv_path));
				sc.useDelimiter(";");
				while (sc.hasNext()) {
					sql = "INSERT INTO `eserciziosqljava`.`partecipante` (`nome_partecipante`, `sede_partecipante`) VALUES ('"
							+ sc.nextLine().replace(";", "','") + "');";
					System.out.println(sql);
					stmt.executeUpdate(sql);
				}
				System.out.println("Popolazione tabella 'partecipante' avvenuta con successo");
				
				closeTableHandlerConnection();
			} catch (FileNotFoundException e) {
				System.out.println("Errore file non trovato: " + e.getMessage());
			}
		} catch (SQLException e1) {
			System.out.println("Errore popolazione tabella 'partecipante': " + e1.getMessage());
		}
	}
	
	public void extraction() {
		this.connection = getTableHandlerConnection();
		
        Partecipante partecipante = estractParticipant(this.connection);

        System.out.println("Partecipante estratto: ");
        System.out.println("Nome: " + partecipante.getNome());
        System.out.println("Sede: " + partecipante.getSede());

        insertEstraction(connection, partecipante.getId());
		
		closeTableHandlerConnection();
	}
	
	private static Partecipante estractParticipant(Connection connection) {
        String query = "SELECT * FROM partecipante ORDER BY RAND() LIMIT 1";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
             ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                int id = resultSet.getInt("id_partecipante");
                String nome = resultSet.getString("nome_partecipante");
                String sede = resultSet.getString("sede_partecipante");

                return new Partecipante(id, nome, sede);
            }
        } catch (SQLException e) {
        	System.out.println("Errore estrazione dalla tabella 'partecipante': " + e.getMessage());
		}

        return null;
    }

    private static void insertEstraction(Connection connection, int idPartecipante) {
        String query = "INSERT INTO estrazione (id_partecipante, timestamp) VALUES (?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(1, idPartecipante);
            preparedStatement.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
        	System.out.println("Errore inserimento estrazione nella tabella 'estrazione': " + e.getMessage());
		}
    }
    
	public void printExctractionsSituation() {
		this.connection = getTableHandlerConnection();

		printExctractionsSituationConsole(this.connection);
		printExctractionsSituationPDF(this.connection);

		closeTableHandlerConnection();
	}
    
    private static void printExctractionsSituationConsole(Connection connection) {
        String query = "SELECT p.nome_partecipante, p.sede_partecipante, COUNT(e.id_estrazione) AS num_estrazioni " +
                       "FROM partecipante p " +
                       "LEFT JOIN estrazione e ON p.id_partecipante = e.id_partecipante " +
                       "GROUP BY p.id_partecipante " +
                       "ORDER BY num_estrazioni DESC";

        try (PreparedStatement preparedStatement = connection.prepareStatement(query);
             ResultSet resultSet = preparedStatement.executeQuery()) {

            System.out.println("Situazione delle estrazioni:");

            while (resultSet.next()) {
                String nomePartecipante = resultSet.getString("nome_partecipante");
                String sedePartecipante = resultSet.getString("sede_partecipante");
                int numEstrazioni = resultSet.getInt("num_estrazioni");

                System.out.println(nomePartecipante + " - " + sedePartecipante + ": " + numEstrazioni + " estrazioni");
            }
        } catch (SQLException e) {
        	System.out.println("Errore stampa situazione estrazioni nella tabella 'estrazione': " + e.getMessage());
		}
    }
    
    private static void printExctractionsSituationPDF(Connection connection) {

        String query = "SELECT p.nome_partecipante, p.sede_partecipante, COUNT(e.id_estrazione) AS num_estrazioni " +
                "FROM partecipante p " +
                "LEFT JOIN estrazione e ON p.id_partecipante = e.id_partecipante " +
                "GROUP BY p.id_partecipante " +
                "ORDER BY num_estrazioni DESC";

		Document document = new Document();
		try {
			PdfWriter.getInstance(document, new FileOutputStream(pdf_path));
		} catch (FileNotFoundException e) {
			System.out.println("Errore file 'situazione_estrazioni.pdf' non trovato: " + e.getMessage());
		} catch (DocumentException e) {
			System.out.println("Errore caricamento file 'situazione_estrazioni.pdf': " + e.getMessage());
		}
		document.open();

		try (PreparedStatement preparedStatement = connection.prepareStatement(query);
				ResultSet resultSet = preparedStatement.executeQuery()) {

			document.add(new Paragraph("Situazione delle estrazioni:"));

			while (resultSet.next()) {
				String nomePartecipante = resultSet.getString("nome_partecipante");
				String sedePartecipante = resultSet.getString("sede_partecipante");
				int numEstrazioni = resultSet.getInt("num_estrazioni");

				String line = nomePartecipante + " - " + sedePartecipante + ": " + numEstrazioni + " estrazioni";
				document.add(new Paragraph(line));
			}
		} catch (SQLException e) {
			System.out.println("Errore stampa situazione estrazioni nella tabella 'estrazione' nel file .pdf: " + e.getMessage());
		} catch (DocumentException e) {
			System.out.println("Errore sull'uso file 'situazione_estrazioni.pdf': " + e.getMessage());
		}

		document.close();
		System.out.println("File PDF generato con successo: " + pdf_path);
	}
}
