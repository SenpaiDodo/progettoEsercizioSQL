package it.betacom.progettoEsercizioSQL;

public class Partecipante {
	private int id;
    private String nome;
    private String sede;

    public Partecipante(int id, String nome, String sede) {
        this.id = id;
        this.nome = nome;
        this.sede = sede;
    }

    public int getId() {
        return id;
    }

    public String getNome() {
        return nome;
    }

    public String getSede() {
        return sede;
    }
}
