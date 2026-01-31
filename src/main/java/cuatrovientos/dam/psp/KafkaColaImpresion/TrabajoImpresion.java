package cuatrovientos.dam.psp.KafkaColaImpresion;

import com.google.gson.Gson;

public class TrabajoImpresion {
    private String titulo;
    private String documento;
    private String tipo; 
    private String sender; 

    public TrabajoImpresion(String titulo, String documento, String tipo, String sender) {
        this.titulo = titulo;
        this.documento = documento;
        this.tipo = tipo;
        this.sender = sender;
    }

    // Getters para acceder a los campos
    public String getTitulo() { return titulo; }
    public String getDocumento() { return documento; }
    public String getTipo() { return tipo; }
    public String getSender() { return sender; }
    
    /**
     * Convierte el objeto actual a formato JSON.
     */
    public String toJson() {
        return new Gson().toJson(this);
    }

    /**
     * Crea un objeto TrabajoImpresion a partir de un JSON.
     */
    public static TrabajoImpresion fromJson(String json) {
        return new Gson().fromJson(json, TrabajoImpresion.class);
    }
}
