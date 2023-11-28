package it.betacom.progettoEsercizioSQL;

public class App {
	public static void main(String[] args) {
		TableHandler th = new TableHandler();
		th.dropTables();
		th.createTables();

		th.readCSV();
		int exctractionLimit = 5;
		for(int i = 0; i < exctractionLimit; i++) {
			th.extraction();
		}
		
		th.printExctractionsSituation();
	}
}
